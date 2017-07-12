#![allow(dead_code)]
//#![deny(warnings)]
#![feature(conservative_impl_trait, drop_types_in_const)]
#![cfg_attr(feature = "flame_it", feature(plugin, custom_attribute))]
#![cfg_attr(feature = "flame_it", plugin(flamer))]

#[cfg(feature = "flame_it")]
extern crate flame;

#[macro_use]
extern crate slog;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

#[macro_use]
extern crate serde_derive;

extern crate base64;

extern crate dogstatsd;
extern crate env_logger;
extern crate arrayvec;
extern crate arraydeque;
extern crate rusoto_sqs;
extern crate rusoto_sns;
extern crate rusoto_core;
extern crate hyper;
extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;
extern crate threadpool;
extern crate futures_cpupool;
extern crate rusoto_credential;
extern crate two_lock_queue;
extern crate itertools;
extern crate serde;
extern crate serde_json;
extern crate lru_time_cache;
extern crate stopwatch;
extern crate slog_json;
extern crate uuid;
extern crate slog_stream;
extern crate fibers;
extern crate chrono;

macro_rules! time {
    ($expression:expr) => (
        {
            let mut sw = $crate::stopwatch::Stopwatch::start_new();
            let exp = $expression;
            sw.stop();
            println!("{} took {}ms",stringify!($expression) , sw.elapsed_ms());
            exp
//              $expression
        }
    );
    ($expression:expr, $s:expr) => (
        {
            let mut sw = $crate::stopwatch::Stopwatch::start_new();
            let exp = $expression;
            sw.stop();
            println!("{} took {}ms", stringify!($s), sw.elapsed_ms());
            exp
//              $expression
        }
    );
}

macro_rules! timeout_ms {
    ($pool:expr, $closure:expr, $dur:expr) => {
        {
            let timeout = Timer::default().sleep(Duration::from_millis($dur))
                .then(|_| Err(()));
            let value = $pool.spawn_fn($closure);
            let value_or_timeout = timeout.select(value).map(|(win, _)| win);
            value_or_timeout.wait()
        }
    };
    ($pool:expr, $closure:expr, $dur:expr, $timer:expr) => {
        {
            let timeout = $timer.sleep(Duration::from_millis($dur))
                .then(|_| Err(()));
            let value = $pool.spawn_fn($closure);
            let value_or_timeout = timeout.select(value).map(|(win, _)| win);
            value_or_timeout.wait()
        }
    };
}

mod util;
mod delay;
mod visibility;
mod delete;
mod publish;
mod processor;
mod consumer;
mod autoscaling;

use processor::*;
use consumer::*;
use visibility::*;
use delete::*;
use publish::*;

use futures_cpupool::CpuPool;

use std::thread;
use std::sync::Mutex;
use std::fs::OpenOptions;
use slog::{Drain, FnValue};
use dogstatsd::{Client, Options};
use std::sync::Arc;
use util::*;
use std::time::Duration;

const PROCESSOR_COUNT: usize = 10;
const CONSUMER_COUNT: usize = 10;


fn main() {
    set_timer();
    let log_path = "everything.log";
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .expect(&format!("Failed to create log file {}", log_path));

    // create logger
    let logger = slog::Logger::root(
        Mutex::new(slog_json::Json::default(file)).map(slog::Fuse),
        o!("version" => env!("CARGO_PKG_VERSION"),
           "place" =>
              FnValue(move |info| {
                  format!("{}:{} {}",
                          info.file(),
                          info.line(),
                          info.module())
              }))
    );

    // slog_stdlog uses the logger from slog_scope, so set a logger there
    let _guard = slog_scope::set_global_logger(logger);

    // register slog_stdlog as the log handler with the log crate
    slog_stdlog::init().expect("Failed to initialize slog_stdlog");

    info!(slog_scope::logger(), "Service Started");

    let timer = get_timer();
    let provider = get_profile_provider();

    let queue_name = "local-dev-cobrien-TEST_QUEUE";
    let queue_url = create_queue(&CpuPool::new_num_cpus(), &provider, queue_name.to_owned(), timer.clone());

    let metrics = Arc::new(Client::new(Options::default()).expect("Failure to create metrics"));

    let sqs_client = Arc::new(new_sqs_client(&provider));

    let deleter = MessageDeleterBroker::new(
        |_| {
            MessageDeleter::new(sqs_client.clone(), queue_url.clone())
        },
        350,
        None
    );

    let consumer_throttler = ConsumerThrottler::new();
    let consumer_throttler = ConsumerThrottlerActor::new(consumer_throttler);

    let deleter = MessageDeleteBuffer::new(deleter, Duration::from_millis(1));
    let deleter = MessageDeleteBufferActor::new(deleter);

    let sqs_client = Arc::new(new_sqs_client(&provider));
    let sns_client = Arc::new(new_sns_client(&provider));

    let broker = VisibilityTimeoutExtenderBroker::new(
        |_| {
            VisibilityTimeoutExtender::new(sqs_client.clone(), queue_url.clone(), deleter.clone())
        },
        550,
        None
    );

    let buffer = VisibilityTimeoutExtenderBuffer::new(broker, 2);
    let buffer = VisibilityTimeoutExtenderBufferActor::new(buffer);

    let flusher = BufferFlushTimer::new(buffer.clone(), Duration::from_millis(200));
    let flusher = BufferFlushTimerActor::new(flusher);
    std::mem::forget(flusher);

    let state_manager = MessageStateManager::new(buffer, deleter.clone(), consumer_throttler.clone());
    let state_manager = MessageStateManagerActor::new(state_manager);

    let publisher = MessagePublisherBroker::new(
        |_| {
            MessagePublisher::new(sns_client.clone(), state_manager.clone())
        },
        500,
        None
    );

    let processor = DelayMessageProcessorBroker::new(
        |_| {
            DelayMessageProcessor::new(state_manager.clone(), publisher.clone(), TopicCreator::new(sns_client.clone()))
        },
        PROCESSOR_COUNT,
        None
    );

    let sqs_client = Arc::new(new_sqs_client(&provider));

    let sqs_broker = DelayMessageConsumerBroker::new(
            |actor| {
                DelayMessageConsumer::new(sqs_client.clone(), queue_url.clone(), metrics.clone(), actor, state_manager.clone(), processor.clone())
            },
        10,
        None
    );

    consumer_throttler.register_consumer(sqs_broker.clone());

    // Maximum number of in flight messages
    for worker in sqs_broker.workers {
        worker.consume()
    }
    println!("consumers started");

    loop {
        thread::park();
    }
}
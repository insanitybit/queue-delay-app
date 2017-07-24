#[macro_use]
extern crate slog;

#[macro_use]
extern crate serde_derive;

extern crate base64;
extern crate hyper;
extern crate lru_time_cache;
extern crate rusoto_credential;
extern crate rusoto_core;
extern crate rusoto_sns;
extern crate rusoto_sqs;
extern crate slog_json;
extern crate sqs_service_helper;
extern crate serde;
extern crate serde_json;
extern crate two_lock_queue;
extern crate uuid;

use rusoto_sqs::{Sqs, SqsClient, CreateQueueRequest};
use rusoto_credential::{ProvideAwsCredentials, ChainProvider, ProfileProvider};
use rusoto_core::{default_tls_client, Region};
use rusoto_sns::SnsClient;
use std::fs::OpenOptions;
use slog::{Drain, FnValue};
use slog::Logger;
use std::sync::{Arc, Mutex};
use std::thread;
use sqs_service_helper::service::{SqsServiceBuilder, Service};
use std::env;
use std::time::Duration;

mod publish;
mod message_handler;
mod delay;

use message_handler::*;
use publish::*;

fn main() {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("everything.log")
        .expect(&format!("Failed to create log file {}", "everything.log"));

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

    let provider = get_profile_provider();
    let sns_client = Arc::new(new_sns_client(&provider));

    let publisher = MessagePublisher::new(
        sns_client.clone(),
        logger.clone());

    let service = SqsServiceBuilder::default()
        .with_message_handler(move |_| {
            let topic_creator = TopicCreator::new(
                sns_client
            );
            DelayMessageProcessor::new(
                publisher,
                topic_creator,
                logger
            )
        })
        .with_msg_handler_count(4)
        .with_msg_handler_max_queue_depth(1000)
        .with_consumer_count(4)
        .with_visibility_extender_count(8)
        .with_visibility_buffer_flush_period(Duration::from_millis(500))
        .with_deleter_count(4)
        .with_deleter_buffer_flush_period(Duration::from_millis(500))
        .with_with_url("queue_url".to_owned())
        .with_short_circuit(true)
        .with_throttle_consumers(true)
        .with_logger(logger)
        .build();

    loop {
        thread::park();
    }
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn new_sns_client<P>(sns_provider: &P) -> SnsClient<P, hyper::Client>
    where P: ProvideAwsCredentials + Clone + Send + 'static
{
    SnsClient::new(
        default_tls_client().unwrap(),
        sns_provider.clone(),
        Region::UsEast1
    )
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_profile_provider() -> ChainProvider {
    let profile = match env::var("AWS_PROFILE") {
        Ok(val) => val.to_string(),
        Err(_) => "default".to_string(),
    };

    let mut profile_provider = ProfileProvider::new().unwrap();
    profile_provider.set_profile(profile);
    ChainProvider::with_profile_provider(profile_provider)
}
use std::time::{Instant, Duration};
use std::thread;

use rusoto_sqs::{Sqs, ReceiveMessageRequest};
use dogstatsd::{Client};
use std::sync::Arc;
use slog_scope;
use visibility::*;
use autoscaling::*;
use uuid;

use processor::*;

use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded, channel};

pub const MAX_INFLIGHT_MESSAGES: usize = 100;

pub struct DelayMessageConsumer<SQ>
    where SQ: Sqs + Send + Sync + 'static,
{
    sqs_client: Arc<SQ>,
    queue_url: String,
    metrics: Arc<Client>,
    actor: DelayMessageConsumerActor,
    vis_manager: MessageStateManagerActor,
    processor: DelayMessageProcessorBroker,
    throttler: ThrottlerActor
}

impl<SQ> DelayMessageConsumer<SQ>
    where SQ: Sqs + Send + Sync + 'static,
{
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new(sqs_client: Arc<SQ>,
               queue_url: String,
               metrics: Arc<Client>,
               actor: DelayMessageConsumerActor,
               vis_manager: MessageStateManagerActor,
               processor: DelayMessageProcessorBroker,
               throttler: ThrottlerActor)
               -> DelayMessageConsumer<SQ>
    {
        DelayMessageConsumer {
            sqs_client,
            queue_url,
            metrics,
            actor,
            vis_manager,
            processor,
            throttler
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn consume(&self) {
        let msg_request = ReceiveMessageRequest {
            max_number_of_messages: Some(10),
            queue_url: self.queue_url.to_owned(),
            wait_time_seconds: Some(20),
            ..Default::default()
        };

        // If we receive a network error we'll sleep for a few ms and retry
        let messages = match self.sqs_client.receive_message(&msg_request) {
            Ok(res) => {
                res.messages
            }
            Err(e) => {
                warn!(slog_scope::logger(), "Failed to receive sqs message. {}", e);
                return;
            }
        };

        if let Some(mut messages) = messages {
            let o_len = messages.len();
            messages.sort_by(|a, b| a.receipt_handle.cmp(&b.receipt_handle));
            messages.dedup_by(|a, b| a.receipt_handle == b.receipt_handle);

            if o_len != messages.len() {
                println!("Contained duplicate messages!");
            }

            let messages: Vec<_> = messages.iter().filter_map(|msg| {
                match msg.receipt_handle {
                    Some(ref receipt) if msg.body.is_some() => {
                        let now = Instant::now();
                        self.vis_manager.register(receipt.to_owned(), Duration::from_secs(30), now.clone());
                        self.throttler.message_start(receipt.to_owned(), now.clone());
                        Some(msg)
                    }
                    _   => None
                }
            }).collect();

            println!("Processing {} messages", messages.len());
            for message in messages {
                self.processor.process(message.clone());
            }

        }
    }

    pub fn throttle(&self, how_long: Duration)
    {
        thread::sleep(how_long);
    }

    fn route_msg(&mut self, msg: DelayMessageConsumerMessage) {
        match msg {
            DelayMessageConsumerMessage::Consume  => self.consume(),
            DelayMessageConsumerMessage::Throttle {how_long}    => self.throttle(how_long),
        };

        self.actor.consume();
    }

    fn wait(&self) {
        let init_backoff = 10;
        let mut backoff = init_backoff;
        let max_backoff = 100;
        while self.vis_manager.sender.len() > MAX_INFLIGHT_MESSAGES {
            thread::sleep(Duration::from_millis(backoff));
            if backoff * 2 <  max_backoff{
                backoff = backoff * 2;
            } else {
                backoff = init_backoff;
            }
        }
    }
}

pub enum DelayMessageConsumerMessage
{
    Consume,
    Throttle {how_long: Duration},
}

#[derive(Clone)]
pub struct DelayMessageConsumerActor
{
    sender: Sender<DelayMessageConsumerMessage>,
    receiver: Receiver<DelayMessageConsumerMessage>,
    p_sender: Sender<DelayMessageConsumerMessage>,
    p_receiver: Receiver<DelayMessageConsumerMessage>,
    id: String
}

impl DelayMessageConsumerActor
{
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new<SQ, F>(new: F)
                      -> DelayMessageConsumerActor
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(DelayMessageConsumerActor) -> DelayMessageConsumer<SQ>,
    {
        let (sender, receiver) = unbounded();
        let (p_sender, p_receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        let actor = DelayMessageConsumerActor {
            sender,
            receiver: receiver.clone(),
            p_sender,
            p_receiver: p_receiver.clone(),
            id,
        };

        let mut _actor = new(actor.clone());

        let recvr = receiver.clone();
        let p_recvr = p_receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if let Ok(msg) = p_recvr.try_recv() {
                        _actor.route_msg(msg);
                        continue
                    }

                    match recvr.recv_timeout(Duration::from_secs(30)) {
                        Ok(msg) => {
                            _actor.route_msg(msg);
                            continue
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            println!("Consumer disconnected");
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            continue
                        }
                    }
                }
            });

        actor
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn from_queue<SQ, F>(new: &F,
                             sender: Sender<DelayMessageConsumerMessage>,
                             receiver: Receiver<DelayMessageConsumerMessage>,
                             p_sender: Sender<DelayMessageConsumerMessage>,
                             p_receiver: Receiver<DelayMessageConsumerMessage>)
                             -> DelayMessageConsumerActor
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(DelayMessageConsumerActor) -> DelayMessageConsumer<SQ>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let actor = DelayMessageConsumerActor {
            sender,
            receiver: receiver.clone(),
            p_sender,
            p_receiver: p_receiver.clone(),
            id: id,
        };

        let mut _actor = new(actor.clone());

        let recvr = receiver.clone();
        let p_recvr = p_receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if let Ok(msg) = p_recvr.try_recv() {
                        _actor.route_msg(msg);
                        continue
                    }

                    match recvr.recv_timeout(Duration::from_secs(30)) {
                        Ok(msg) => {
                            _actor.route_msg(msg);
                            continue
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            println!("Consumer disconnected");
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            continue
                        }
                    }
                }
            });

        actor
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn consume(&self) {
        let _ = self.sender.send(DelayMessageConsumerMessage::Consume)
            .expect("Underlying consumer has died");
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn throttle(&self, how_long: Duration) {
        let _ = self.p_sender.send(DelayMessageConsumerMessage::Throttle {how_long})
            .expect("Underlying consumer has died");
    }
}



#[derive(Clone)]
pub struct DelayMessageConsumerBroker
{
    pub workers: Vec<DelayMessageConsumerActor>,
    pub worker_count: usize,
    sender: Sender<DelayMessageConsumerMessage>,
    p_sender: Sender<DelayMessageConsumerMessage>,
    id: String
}

impl DelayMessageConsumerBroker
{
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new<T, SQ, F>(new: F,
                         worker_count: usize,
                         max_queue_depth: T)
                         -> DelayMessageConsumerBroker
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(DelayMessageConsumerActor) -> DelayMessageConsumer<SQ>,
              T: Into<Option<usize>>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);
        let (p_sender, p_receiver) = unbounded();

        let workers: Vec<_> = (0..worker_count)
            .map(|_| DelayMessageConsumerActor::from_queue(&new, sender.clone(), receiver.clone(),
            p_sender.clone(), p_receiver.clone()))
            .collect();

        let worker_count = workers.len();

        DelayMessageConsumerBroker {
            workers,
            worker_count,
            sender,
            p_sender,
            id
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn consume(&self) {
        self.sender.send(
            DelayMessageConsumerMessage::Consume
        ).unwrap();
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn throttle(&self, how_long: Duration) {
        self.sender.send(
            DelayMessageConsumerMessage::Throttle {how_long}
        ).unwrap();
    }

}

#[derive(Clone)]
pub struct ConsumerThrottler {
    consumer_broker: Option<DelayMessageConsumerBroker>,
}

impl ConsumerThrottler{
    pub fn new()
    -> ConsumerThrottler {
        ConsumerThrottler {
            consumer_broker: None,
        }
    }

    pub fn throttle(&self, how_long: Duration) {
        match self.consumer_broker {
            Some(ref consumer_broker) => {
                for _ in 0..consumer_broker.worker_count {
                    consumer_broker.throttle(how_long)
                }
            },
            None    => println!("Error - no consumer registered with ConsumerThrottler")
        }
    }

    pub fn register_consumer(&mut self, consumer: DelayMessageConsumerBroker) {
        self.consumer_broker = Some(consumer);
    }

    fn route_msg(&mut self, msg: ConsumerThrottlerMessage) {
        match msg {
            ConsumerThrottlerMessage::Throttle {how_long} => self.throttle(how_long),
            ConsumerThrottlerMessage::RegisterconsumerBroker {consumer} => self.register_consumer(consumer),
        }
    }
}

pub enum ConsumerThrottlerMessage
{
    Throttle {how_long: Duration},
    RegisterconsumerBroker {consumer: DelayMessageConsumerBroker}
}

#[derive(Clone)]
pub struct ConsumerThrottlerActor
{
    sender: Sender<ConsumerThrottlerMessage>,
    receiver: Receiver<ConsumerThrottlerMessage>,
    p_sender: Sender<ConsumerThrottlerMessage>,
    p_receiver: Receiver<ConsumerThrottlerMessage>,
    id: String
}

impl ConsumerThrottlerActor
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: ConsumerThrottler)
                      -> ConsumerThrottlerActor
    {
        let (sender, receiver) = unbounded();
        let (p_sender, p_receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        let mut actor = actor;

        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if let Ok(msg) = recvr.try_recv() {
                        actor.route_msg(msg);
                    }

                    match recvr.recv_timeout(Duration::from_secs(30)) {
                        Ok(msg) => {
                            actor.route_msg(msg);
                            continue
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            continue
                        }
                    }
                }
            });

        ConsumerThrottlerActor {
            sender: sender.clone(),
            receiver: receiver.clone(),
            p_sender: p_sender.clone(),
            p_receiver: p_receiver.clone(),
            id: id,
        }
    }

    pub fn register_consumer(&self, consumer: DelayMessageConsumerBroker) {
        self.sender.send(ConsumerThrottlerMessage::RegisterconsumerBroker {consumer});
    }

    pub fn throttle(&self, how_long: Duration) {
        let _ = self.p_sender.send(ConsumerThrottlerMessage::Throttle {how_long})
            .expect("Underlying consumer has died");
    }
}
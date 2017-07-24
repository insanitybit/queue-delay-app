use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError, channel};
use uuid;

use std::time::{Duration};
use rusoto_sns::{Sns, PublishInput};
use delay::DelayMessage;

use slog::Logger;
use std::sync::Arc;
use std::iter::Iterator;
use std::thread;
use std::error::Error;

pub struct MessagePublisher<SN>
    where SN: Sns + Send + Sync + 'static,
{
    sns_client: Arc<SN>,
    logger: Logger
}

impl<SN> MessagePublisher<SN>
    where SN: Sns + Send + Sync + 'static,
{
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new(sns_client: Arc<SN>, logger: Logger)
               -> MessagePublisher<SN>
    {
        MessagePublisher {
            sns_client,
            logger
        }
    }

    pub fn publish(&self, message: String, topic_arn: String) -> Result<(), String> {
        let topic_arn = Some(topic_arn);
        let input = PublishInput {
            message,
            topic_arn,
            ..Default::default()
        };

        let mut backoff = 0;
        loop {
            let res = self.sns_client.publish(&input);
            match res {
                Ok(_)   => {
                    return Ok(())
                },
                Err(e)  => {
                    backoff += 1;
                    thread::sleep(Duration::from_secs(20 * backoff));
                    if backoff > 3 {
                        return Err(e.description().to_owned())
                    }
                }
            }
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn route_msg(&mut self, msg: MessagePublisherMessage) {
        match msg {
            MessagePublisherMessage::PublishAndDelete {message, topic_arn, receipt}  =>
                self.publish(message.message, topic_arn)
        };
    }
}

#[derive(Debug)]
pub enum MessagePublisherMessage {
    PublishAndDelete {
        message: DelayMessage,
        topic_arn: String,
        receipt: String,
    },
}

#[derive(Clone)]
pub struct MessagePublisherActor {
    sender: Sender<MessagePublisherMessage>,
    id: String,
}

impl MessagePublisherActor {
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new<SN>(actor: MessagePublisher<SN>) -> MessagePublisherActor
        where SN: Sns + Send + Sync + 'static,
    {
        let mut actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();

        thread::spawn(
            move || {
                loop {
                    match recvr.recv_timeout(Duration::from_secs(60)) {
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

        MessagePublisherActor {
            sender,
            id,
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn from_queue<SN, F>(
        new: &F,
        sender: Sender<MessagePublisherMessage>,
        receiver: Receiver<MessagePublisherMessage>)
        -> MessagePublisherActor
        where SN: Sns + Send + Sync + 'static,
              F: Fn(MessagePublisherActor) -> MessagePublisher<SN>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let actor = MessagePublisherActor {
            sender: sender.clone(),
            id,
        };

        let mut _actor = new(actor.clone());

        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    match recvr.recv_timeout(Duration::from_secs(60)) {
                        Ok(msg) => {
                            _actor.route_msg(msg);
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

        actor
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn publish_and_delete(&self, message: DelayMessage, topic_arn: String, receipt: String) {
        self.sender.send(MessagePublisherMessage::PublishAndDelete {
            message, topic_arn, receipt
        }).unwrap();
    }
}

#[derive(Clone)]
pub struct MessagePublisherBroker
{
    workers: Vec<MessagePublisherActor>,
    sender: Sender<MessagePublisherMessage>,
    id: String
}

impl MessagePublisherBroker
{
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new<T, F, SN>(new: F,
                         worker_count: usize,
                         max_queue_depth: T)
                         -> MessagePublisherBroker
        where F: Fn(MessagePublisherActor) -> MessagePublisher<SN>,
              T: Into<Option<usize>>,
              SN: Sns + Send + Sync + 'static,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);

        let workers = (0..worker_count)
            .map(|_| MessagePublisherActor::from_queue(&new, sender.clone(), receiver.clone()))
            .collect();

        MessagePublisherBroker {
            workers,
            sender,
            id
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn publish_and_delete(&self, message: DelayMessage, topic_arn: String, receipt: String) {
        self.sender.send(
            MessagePublisherMessage::PublishAndDelete { message, topic_arn, receipt}
        ).unwrap();
    }
}


use lru_time_cache::LruCache;

#[derive(Debug)]
pub enum Topic {
    Cached(String),
    Created(String)
}

impl Topic {
    pub fn get(self) -> String {
        match self {
            Topic::Cached(t) | Topic::Created(t) => t,
        }
    }
}


use lru_time_cache::Entry;
use rusoto_sns::CreateTopicInput;

pub struct TopicCreator<SN>
    where SN: Sns + Send + Sync + 'static,
{
    sns_client: Arc<SN>,
    topic_cache: LruCache<String, String>,
}

impl<SN> TopicCreator<SN>
    where SN: Sns + Send + Sync + 'static,
{
    pub fn new(sns_client: Arc<SN>) -> TopicCreator<SN> {
        TopicCreator {
            sns_client,
            topic_cache: LruCache::with_expiry_duration_and_capacity(Duration::from_secs(60 * 60), 500),
        }
    }

    pub fn get_or_create(&mut self, topic_name: &str) -> Result<Topic, String> {
        let entry = self.topic_cache.entry(topic_name.to_owned());

        match entry {
            Entry::Occupied(oc) => {
                Ok(Topic::Cached(oc.into_mut().to_owned()))
            }
            Entry::Vacant(vac) => {
                let create_topic_input = CreateTopicInput {
                    name: topic_name.to_owned()
                };
                let arn_res = self.sns_client.create_topic(&create_topic_input);

                match arn_res {
                    Ok(arn) => {
                        match arn.topic_arn {
                            Some(arn) => {
                                //                                info!("Created topic: {}", arn);
                                vac.insert(arn.clone());
                                Ok(Topic::Created(arn))
                            }
                            None => Err("returned arn was None".to_owned())
                        }
                    }
                    Err(e) => {
                        Err(format!("{}", e))
                    }
                }
            }
        }
    }
}

#![allow(dead_code)]
use two_lock_queue::{Sender, Receiver, RecvTimeoutError, channel};

use std::time::{Duration};
use rusoto_sns::{Sns, PublishInput};
use lru_time_cache::Entry;
use rusoto_sns::CreateTopicInput;

use slog::Logger;
use std::sync::Arc;
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

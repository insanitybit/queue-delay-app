use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError, channel};
use uuid;

use base64::decode;
use serde_json;
use std::time::{Duration};
use rusoto_sns::{Sns, PublishInput};
use rusoto_sqs::Message as SqsMessage;
use delay::DelayMessage;

use slog::Logger;
use std::sync::Arc;
use std::iter::Iterator;
use std::thread;
use std::error::Error;
use publish::*;
use sqs_service_helper::processor::MessageHandler;

pub struct DelayMessageProcessor<SN>
    where SN: Sns + Send + Sync + 'static,
{
    publisher: MessagePublisher<SN>,
    topic_creator: TopicCreator<SN>,
    logger: Logger
}

impl<SN> DelayMessageProcessor<SN>
    where SN: Sns + Send + Sync + 'static,
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(publisher: MessagePublisher<SN>,
               topic_creator: TopicCreator<SN>,
               logger: Logger)
               -> DelayMessageProcessor<SN>
    {
        DelayMessageProcessor {
            publisher,
            topic_creator,
            logger
        }
    }
}

impl<SN> MessageHandler for DelayMessageProcessor<SN>
    where SN: Sns + Send + Sync + 'static,
{
    #[cfg_attr(feature = "flame_it", flame)]
    fn process_message(&mut self, msg: SqsMessage) -> Result<(), String> {
        let raw_body = match msg.body {
            Some(ref body) => body.to_owned(),
            None => {
                warn!(self.logger, "Message has no body.");

                return Err("Message has no body.".to_owned());
            }
        };

        let body = decode(&raw_body);

        let body = match body {
            Ok(body) => body,
            Err(e) => {
                return Err(format!("Body was not base64 encoded: {}", e));
            }
        };

        let delay_message: Result<DelayMessage, _> = serde_json::from_slice(&body[..]);

        let delay_message = match delay_message {
            Ok(m) => m,
            Err(e) => {
                return Err(format!("Failed to deserialize delay message: {}", e));
            }
        };

        let arn = self.topic_creator.get_or_create(&delay_message.topic_name)?;

        self.publisher.publish(delay_message.message, arn.get())
    }
}
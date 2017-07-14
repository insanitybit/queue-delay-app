use visibility::*;
use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError, channel};
use uuid;

use std::time::{Duration};
use rusoto_sns::{Sns, PublishInput};
use delay::DelayMessage;

use std::sync::Arc;
use std::iter::Iterator;
use std::thread;
use std::error::Error;

pub struct MessagePublisher<SN>
    where SN: Sns + Send + Sync + 'static,
{
    sns_client: Arc<SN>,
    vis_manager: MessageStateManagerActor,
}

impl<SN> MessagePublisher<SN>
    where SN: Sns + Send + Sync + 'static,
{
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new(sns_client: Arc<SN>, vis_manager: MessageStateManagerActor) -> MessagePublisher<SN> {
        MessagePublisher {
            sns_client,
            vis_manager
        }
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn publish_and_delete(&self, msg: DelayMessage, topic_arn: String, receipt: String) {
        match self.publish(msg.message, topic_arn) {
            Ok(_)   => {
                self.vis_manager.deregister(receipt.clone(), true);
            },
            Err(e)  => {
                println!("Failed to publish message with: {}", e);
                self.vis_manager.deregister(receipt, false);
            }
        }
    }

    pub fn publish(&self, message: String, topic_arn: String) -> Result<(), String> {
        let topic_arn = Some(topic_arn);
        let input = PublishInput {
            message,
            topic_arn,
            ..Default::default()
        };

        let res = self.sns_client.publish(&input);
        match res {
            Ok(_)   => Ok(()),
            Err(e)  => Err(e.description().to_owned())
        }

    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn route_msg(&self, msg: MessagePublisherMessage) {
        match msg {
            MessagePublisherMessage::PublishAndDelete {message, topic_arn, receipt}  =>
                self.publish_and_delete(message, topic_arn, receipt)
        }
    }
}

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
    receiver: Receiver<MessagePublisherMessage>,
    id: String,
}

impl MessagePublisherActor {
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new<SN>(actor: MessagePublisher<SN>) -> MessagePublisherActor
        where SN: Sns + Send + Sync + 'static,
    {
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
            sender: sender,
            receiver: receiver,
            id: id,
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
            receiver: receiver.clone(),
            id: id,
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
                            println!("Haven't received a message in 10 seconds");
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


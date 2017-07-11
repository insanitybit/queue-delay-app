use visibility::*;
use publish::*;
use base64::decode;
use serde_json;

use rusoto_sqs::Message as SqsMessage;
use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded, channel};
use std::time::Duration;
use rusoto_sns::Sns;
use delay::DelayMessage;
use util::{TopicCreator};
use uuid::Uuid;
use std::thread;

pub struct DelayMessageProcessor<SN>
    where SN: Sns + Send + Sync + 'static,
{
    vis_manager: MessageStateManagerActor,
    publisher: MessagePublisherBroker,
    topic_creator: TopicCreator<SN>
}

impl<SN> DelayMessageProcessor<SN>
    where SN: Sns + Send + Sync + 'static,
{
    pub fn new(vis_manager: MessageStateManagerActor,
               publisher: MessagePublisherBroker,
               topic_creator: TopicCreator<SN>)
               -> DelayMessageProcessor<SN>
    {
        DelayMessageProcessor {
            vis_manager,
            publisher,
            topic_creator,
        }
    }

    pub fn process_message(&mut self, msg: SqsMessage) {
        time!({
            let receipt = match msg.receipt_handle.clone() {
                Some(receipt)   => receipt,
                None    => {
                    println!("No receipt found for message in process_message");
                    return
                }
            };

            let raw_body = match msg.body {
                Some(ref body) => body.to_owned(),
                None => {
                    println!("Message has no body.");
                    self.vis_manager.deregister(receipt, false);
                    return;
                }
            };

            let body = decode(&raw_body);

            let body = match body {
                Ok(body) => body,
                Err(e) => {
                println!("Body was not base64 encoded: {}", e);
                    self.vis_manager.deregister(receipt, false);
                    return;
                }
            };

            let delay_message: Result<DelayMessage, _> = serde_json::from_slice(&body[..]);

            let delay_message = match delay_message {
                Ok(m) => m,
                Err(e) => {
                println!("Failed to deserialize delay message: {}", e);
                    self.vis_manager.deregister(receipt, false);
                    return;
                }
            };

            let arn = self.topic_creator.get_or_create(delay_message.topic_name.clone());

            let arn = match arn {
                Ok(arn) => arn,
                Err(e) => {
                    println!("Failed to get arn with {}", e);
                    self.vis_manager.deregister(receipt, false);
                    return;
                }
            };

            time!(self.publisher.publish_and_delete(delay_message, arn.get(), receipt));
        }, "processor")
    }

    pub fn route_msg(&mut self, message: DelayMessageProcessorMessage) {
        match message {
            DelayMessageProcessorMessage::Process { message } => self.process_message(message)
        }
    }
}

pub enum DelayMessageProcessorMessage {
    Process { message: SqsMessage }
}

#[derive(Clone)]
pub struct DelayMessageProcessorActor {
    sender: Sender<DelayMessageProcessorMessage>,
    receiver: Receiver<DelayMessageProcessorMessage>,
    id: String
}

impl DelayMessageProcessorActor {
    pub fn from_queue<SN, F>(new: &F,
                             sender: Sender<DelayMessageProcessorMessage>,
                             receiver: Receiver<DelayMessageProcessorMessage>)
                             -> DelayMessageProcessorActor
        where SN: Sns + Send + Sync + 'static,
              F: Fn(DelayMessageProcessorActor) -> DelayMessageProcessor<SN>
    {
        let id = Uuid::new_v4().to_string();

        let actor = DelayMessageProcessorActor {
            sender: sender.clone(),
            receiver: receiver.clone(),
            id: id,
        };

        let mut _actor = new(actor.clone());

        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if recvr.len() > 10 {
                        println!("DelayMessageProcessorActorx queue len {}", recvr.len());
                    }

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

    pub fn new<SN, F>(new: F)
                      -> DelayMessageProcessorActor
        where SN: Sns + Send + Sync + 'static,
              F: FnOnce(DelayMessageProcessorActor) -> DelayMessageProcessor<SN>
    {
        let (sender, receiver) = channel(100);
        let id = Uuid::new_v4().to_string();

        let actor = DelayMessageProcessorActor {
            sender: sender.clone(),
            receiver: receiver.clone(),
            id: id,
        };

        let mut _actor = new(actor.clone());

        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {

                    if recvr.len() > 10 {
                        println!("DelayMessageProcessorActor queue len {}", recvr.len());
                    }

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
}

#[derive(Clone)]
pub struct DelayMessageProcessorBroker
{
    workers: Vec<DelayMessageProcessorActor>,
    sender: Sender<DelayMessageProcessorMessage>,
    id: String
}

impl DelayMessageProcessorBroker
{
    pub fn new<T, F, SN>(new: F,
                         worker_count: usize,
                         max_queue_depth: T)
                         -> DelayMessageProcessorBroker
        where F: Fn(DelayMessageProcessorActor) -> DelayMessageProcessor<SN>,
              T: Into<Option<usize>>,
              SN: Sns + Send + Sync + 'static,
    {
        let id = Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);

        let workers = (0..worker_count)
            .map(|_| DelayMessageProcessorActor::from_queue(&new, sender.clone(), receiver.clone()))
            .collect();

        DelayMessageProcessorBroker {
            workers,
            sender,
            id
        }
    }

    pub fn process(&self, message: SqsMessage) {
        self.sender.send(
            DelayMessageProcessorMessage::Process { message }
        ).unwrap();
    }
}

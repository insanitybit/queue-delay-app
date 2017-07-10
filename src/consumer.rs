use std::time::*;
use std::thread;

use rusoto_sqs::{Sqs, ReceiveMessageRequest};
use dogstatsd::{Client};
use std::sync::Arc;
use slog_scope;
use visibility::*;
use uuid;

use processor::*;

use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded};


pub struct DelayMessageConsumer<SQ>
    where SQ: Sqs + Send + Sync + 'static,
{
    sqs_client: Arc<SQ>,
    queue_url: String,
    metrics: Arc<Client>,
    actor: DelayMessageConsumerActor,
    vis_manager: VisibilityTimeoutManagerActor,
    processor: DelayMessageProcessorBroker
}

impl<SQ> DelayMessageConsumer<SQ>
    where SQ: Sqs + Send + Sync + 'static,
{
    pub fn new(sqs_client: Arc<SQ>,
               queue_url: String,
               metrics: Arc<Client>,
               actor: DelayMessageConsumerActor,
               vis_manager: VisibilityTimeoutManagerActor,
               processor: DelayMessageProcessorBroker)
               -> DelayMessageConsumer<SQ>
    {
        DelayMessageConsumer {
            sqs_client,
            queue_url,
            metrics,
            actor,
            vis_manager,
            processor
        }
    }

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

                self.actor.consume();
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
                        self.vis_manager.register(receipt.to_owned(), Duration::from_secs(30), Instant::now());
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

        self.actor.consume();
    }

    fn route_msg(&self, msg: DelayMessageConsumerMessage) {
        match msg {
            DelayMessageConsumerMessage::Consume  => self.consume()
        };
    }
}


enum DelayMessageConsumerMessage
{
    Consume
}

#[derive(Clone)]
pub struct DelayMessageConsumerActor
{
    sender: Sender<DelayMessageConsumerMessage>,
    receiver: Receiver<DelayMessageConsumerMessage>,
    id: String
}

impl DelayMessageConsumerActor
{
    pub fn new<SQ, F>(new: F)
                      -> DelayMessageConsumerActor
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(DelayMessageConsumerActor) -> DelayMessageConsumer<SQ>,
    {
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        let actor = DelayMessageConsumerActor {
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
                            continue
                        }
                    }
                }
            });

        actor
    }

    pub fn consume(&self) {
        let _ = self.sender.send(DelayMessageConsumerMessage::Consume)
            .expect("Underlying consumer has died");
    }
}
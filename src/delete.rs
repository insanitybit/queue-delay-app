use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError, channel};
use uuid;

use chrono;

use std::time::{Instant, Duration};
use std::collections::HashMap;
use rusoto_sqs::{Sqs, DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry};

use std::sync::Arc;
use arrayvec::ArrayVec;
use std::iter::Iterator;
use std::thread;

#[derive(Clone)]
pub struct MessageDeleter<SQ>
    where SQ: Sqs + Send + Sync + 'static,
{
    sqs_client: Arc<SQ>,
    queue_url: String,
}

impl<SQ> MessageDeleter<SQ>
    where SQ: Sqs + Send + Sync + 'static,
{
    pub fn new(sqs_client: Arc<SQ>, queue_url: String) -> MessageDeleter<SQ> {
        MessageDeleter {
            sqs_client,
            queue_url
        }
    }

    pub fn delete_messages(&self, receipts: Vec<(String, Instant)>) {
        let msg_count = receipts.len();
        println!("Deleting {} messages", msg_count);

        let mut receipt_init_map = HashMap::new();

        for (receipt, time) in receipts {
            receipt_init_map.insert(receipt, time);
        }

        let entries = receipt_init_map.keys().map(|r| {
            DeleteMessageBatchRequestEntry {
            id: format!("{}", uuid::Uuid::new_v4()),
            receipt_handle: r.to_owned()
        }
        }).collect();


        let req = DeleteMessageBatchRequest {
            entries,
            queue_url: self.queue_url.clone()
        };

        let mut backoff = 0;

        loop {
            match self.sqs_client.delete_message_batch(&req) {
                Ok(res)   => {
                    let now = Instant::now();
                    for init_time in receipt_init_map.values() {
                        let dur = now - *init_time;

                        match chrono::Duration::from_std(dur) {
                            Ok(dur) => println!("Took {}ms to process message to deletion",
                                dur.num_milliseconds()),
                            Err(e)  => println!("Failed to time {}", e)
                        }
                    }
                    break
                },
                Err(e)  => {
                    if backoff >= 5 {
                        println!("Failed to deleted {} messages {}", msg_count ,e);
                        break
                    }
                    backoff += 1;
                }
            }
        }

    }

    pub fn route_msg(&self, msg: MessageDeleterMessage) {
        match msg {
            MessageDeleterMessage::DeleteMessages { receipts } => self.delete_messages(receipts)
        }
    }
}

pub enum MessageDeleterMessage {
    DeleteMessages {
        receipts: Vec<(String, Instant)>,
    },
}

#[derive(Clone)]
pub struct MessageDeleterActor {
    sender: Sender<MessageDeleterMessage>,
    receiver: Receiver<MessageDeleterMessage>,
    id: String,
}

impl MessageDeleterActor {
    pub fn new<SQ>(actor: MessageDeleter<SQ>) -> MessageDeleterActor
        where SQ: Sqs + Send + Sync + 'static,
    {
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();

        thread::spawn(
            move || {
                loop {
                    if recvr.len() > 10 {
                        println!("MessageDeleterActor queue len {}", recvr.len());
                    }
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

        MessageDeleterActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    pub fn from_queue<SQ, F>(
        new: &F,
        sender: Sender<MessageDeleterMessage>,
        receiver: Receiver<MessageDeleterMessage>)
        -> MessageDeleterActor
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(MessageDeleterActor) -> MessageDeleter<SQ>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let actor = MessageDeleterActor {
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
                        println!("MessageDeleterActor queue len {}", recvr.len());
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
                            println!("MessageDeleteActor Haven't received a message in 10 seconds");
                            continue
                        }
                    }
                }
            });

        actor
    }
}

#[derive(Clone)]
pub struct MessageDeleterBroker
{
    workers: Vec<MessageDeleterActor>,
    sender: Sender<MessageDeleterMessage>,
    id: String
}

impl MessageDeleterBroker
{
    pub fn new<T, F, SQ>(new: F,
                         worker_count: usize,
                         max_queue_depth: T)
                         -> MessageDeleterBroker
        where F: Fn(MessageDeleterActor) -> MessageDeleter<SQ>,
              T: Into<Option<usize>>,
              SQ: Sqs + Send + Sync + 'static,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);

        let workers = (0..worker_count)
            .map(|_| MessageDeleterActor::from_queue(&new, sender.clone(), receiver.clone()))
            .collect();

        MessageDeleterBroker {
            workers,
            sender,
            id
        }
    }

    pub fn delete_messages(&self, receipts: Vec<(String, Instant)>) {
        self.sender.send(
            MessageDeleterMessage::DeleteMessages { receipts }
        ).unwrap();
    }
}

pub struct MessageDeleteBuffer {
    deleter_broker: MessageDeleterBroker,
    buffer: ArrayVec<[(String, Instant); 10]>,
    flush_period: Duration
}

impl MessageDeleteBuffer {
    pub fn new(deleter_broker: MessageDeleterBroker, flush_period: u8) -> MessageDeleteBuffer
    {
        MessageDeleteBuffer {
            deleter_broker: deleter_broker,
            buffer: ArrayVec::new(),
            flush_period: Duration::from_secs(flush_period as u64)
        }
    }

    pub fn delete_message(&mut self, receipt: String, init_time: Instant) {
        if self.buffer.is_full() {
            println!("MessageDeleteBuffer buffer full. Flushing.");
            self.flush();
        }

        self.buffer.push((receipt, init_time));
    }

    pub fn flush(&mut self) {
        self.deleter_broker.delete_messages(Vec::from(self.buffer.as_ref()));
        self.buffer.clear();
    }

    pub fn on_timeout(&mut self) {
        if self.buffer.len() != 0 {
            println!("MessageDeleteBuffer timeout. Flushing {} messages.", self.buffer.len());
            self.flush();
        }
    }
}

pub enum MessageDeleteBufferMessage {
    Delete {
        receipt: String,
        init_time: Instant
    },
    Flush {},
    OnTimeout {},
}

#[derive(Clone)]
pub struct MessageDeleteBufferActor {
    sender: Sender<MessageDeleteBufferMessage>,
    receiver: Receiver<MessageDeleteBufferMessage>,
    id: String,
}

impl MessageDeleteBufferActor {
    pub fn new
    (actor: MessageDeleteBuffer)
     -> MessageDeleteBufferActor
    {
        let mut actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if recvr.len() > 10 {
                        println!("MessageDeleteBufferActor queue len {}", recvr.len());
                    }
                    match recvr.recv_timeout(Duration::from_secs(60)) {
                        Ok(msg) => {
                            actor.route_msg(msg);
                            continue
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            println!("MessageDeleteBufferActor Haven't received a message in 10 seconds");
                            continue
                        }
                    }
                }
            });

        MessageDeleteBufferActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    pub fn delete_message(&self, receipt: String, init_time: Instant) {
        let msg = MessageDeleteBufferMessage::Delete {
            receipt,
            init_time
        };
        self.sender.send(msg).expect("All receivers have died.");
    }

    pub fn flush(&self) {
        let msg = MessageDeleteBufferMessage::Flush {};
        self.sender.send(msg).expect("All receivers have died.");
    }

    pub fn on_timeout(&self) {
        let msg = MessageDeleteBufferMessage::OnTimeout {};
        self.sender.send(msg).expect("All receivers have died.");
    }
}

impl MessageDeleteBuffer
{
    pub fn route_msg(&mut self, msg: MessageDeleteBufferMessage) {
        match msg {
            MessageDeleteBufferMessage::Delete {
                receipt,
                init_time
            } => {
                self.delete_message(receipt, init_time)
            }
            MessageDeleteBufferMessage::Flush {} => self.flush(),
            MessageDeleteBufferMessage::OnTimeout {} => self.on_timeout(),
        };
    }
}

// BufferFlushTimer
#[derive(Clone)]
pub struct DeleteBufferFlusher
{
    pub buffer: MessageDeleteBufferActor,
    pub period: Duration
}

impl DeleteBufferFlusher
{
    pub fn new(buffer: MessageDeleteBufferActor, period: Duration) -> DeleteBufferFlusher {
        DeleteBufferFlusher {
            buffer,
            period
        }
    }
}

#[derive(Debug)]
pub enum DeleteBufferFlushMessage {
    Start,
    End,
}

#[derive(Clone)]
pub struct DeleteBufferFlusherActor {
    sender: Sender<DeleteBufferFlushMessage>,
    receiver: Receiver<DeleteBufferFlushMessage>,
    id: String,
}

impl DeleteBufferFlusherActor {
    pub fn new(actor: DeleteBufferFlusher)
               -> DeleteBufferFlusherActor
    {
        let actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();
        thread::spawn(move || {
            loop {
                if recvr.len() > 10 {
                    println!("DeleteBufferFlusherActor queue len {}", recvr.len())
                }
                let recvr = recvr.clone();
                let actor = actor.clone();
                let dur = actor.period; // Default, minimal timeout

                let res = recvr.recv_timeout(dur);

                match res {
                    Ok(msg) => {
                        match msg {
                            DeleteBufferFlushMessage::Start => {},
                            DeleteBufferFlushMessage::End => break
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        actor.buffer.on_timeout();
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        println!("disconnected");
                        actor.buffer.on_timeout();
                        break
                    }
                }
            }
        });

        DeleteBufferFlusherActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    pub fn start(&self) {
        let msg = DeleteBufferFlushMessage::Start;
        self.sender.send(msg).expect("All receivers have died.");
    }

    pub fn end(&self) {
        let msg = DeleteBufferFlushMessage::End;
        self.sender.send(msg).expect("All receivers have died.");
    }
}
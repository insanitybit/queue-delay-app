#![allow(unreachable_code)]

use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError, channel};
use uuid;


use std::time::{Instant, Duration};
use std::collections::HashMap;
use rusoto_sqs::{Sqs, ChangeMessageVisibilityBatchRequestEntry, ChangeMessageVisibilityBatchRequest};

use std::sync::Arc;
use arrayvec::ArrayVec;
use std::iter::Iterator;
use std::thread;
use delete::*;

// The visibility timeout manager creates and registers VisibilityTimeouts
// It is also responsible for deregistering VisibilityTimeouts
#[derive(Clone)]
pub struct VisibilityTimeoutManager
{
    timers: HashMap<String, VisibilityTimeoutActor>,
    buffer: VisibilityTimeoutExtenderBufferActor,
    deleter: MessageDeleteBufferActor
}

impl VisibilityTimeoutManager
{
    pub fn new(buffer: VisibilityTimeoutExtenderBufferActor, deleter: MessageDeleteBufferActor) -> VisibilityTimeoutManager
    {
        VisibilityTimeoutManager {
            timers: HashMap::with_capacity(100),
            buffer,
            deleter
        }
    }

    pub fn register(&mut self, receipt: String, visibility_timeout: Duration, start_time: Instant) {
        let vis_timeout = VisibilityTimeout::new(self.buffer.clone(), self.deleter.clone(), receipt.clone());
        let vis_timeout = VisibilityTimeoutActor::new(vis_timeout);
        vis_timeout.start(visibility_timeout, start_time);

        self.timers.insert(receipt, vis_timeout);
    }

    pub fn deregister(&mut self, receipt: String, should_delete: bool) {
        let vis_timeout = self.timers.remove(&receipt);

        match vis_timeout {
            Some(vis_timeout) => vis_timeout.end(should_delete),
            None => {
                println!("Attempting to deregister timeout that does not exist:\
                receipt: {} should_delete: {}", receipt, should_delete);
                return;
            }
        };
    }
}

pub enum VisibilityTimeoutManagerMessage {
    RegisterVariant {
        receipt: String,
        visibility_timeout: Duration,
        start_time: Instant
    },
    DeregisterVariant { receipt: String, should_delete: bool },
}

#[derive(Clone)]
pub struct VisibilityTimeoutManagerActor {
    pub sender: Sender<VisibilityTimeoutManagerMessage>,
    receiver: Receiver<VisibilityTimeoutManagerMessage>,
    id: String,
}

impl VisibilityTimeoutManagerActor {
    pub fn new(actor: VisibilityTimeoutManager)
               -> VisibilityTimeoutManagerActor
    {
        let mut actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();

        thread::spawn(
            move || {
                loop {
                    if recvr.len() > 5 {
                        println!("VisibilityTimeoutManagerActor queue len {}", recvr.len());
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

        VisibilityTimeoutManagerActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    pub fn register(&self, receipt: String, visibility_timeout: Duration, start_time: Instant) {
        let msg = VisibilityTimeoutManagerMessage::RegisterVariant {
            receipt,
            visibility_timeout,
            start_time
        };
        self.sender.send(msg).expect("All receivers have died.");
    }

    pub fn deregister(&self, receipt: String, should_delete: bool) {
        if !should_delete {
            println!("Error when processing message, apparently!");
        }

        let msg = VisibilityTimeoutManagerMessage::DeregisterVariant { receipt, should_delete };
        self.sender.send(msg).expect("All receivers have died.");
    }
}

impl VisibilityTimeoutManager
{
    pub fn route_msg(&mut self, msg: VisibilityTimeoutManagerMessage) {
        match msg {
            VisibilityTimeoutManagerMessage::RegisterVariant {
                receipt, visibility_timeout, start_time
            } =>
                self.register(receipt, visibility_timeout, start_time),
            VisibilityTimeoutManagerMessage::DeregisterVariant { receipt, should_delete } => {
                self.deregister(receipt, should_delete)
            }
        };
    }
}

// VisibilityTimeout emits events to VisibilityTimeoutExtenders when a messagActore
// needs its visibility timeout extended. Upon receiving a kill message it will
// stop emitting these events.
#[derive(Clone)]
pub struct VisibilityTimeout
{
    pub buf: VisibilityTimeoutExtenderBufferActor,
    pub receipt: String,
    pub deleter: MessageDeleteBufferActor
}

impl VisibilityTimeout
{
    pub fn new(buf: VisibilityTimeoutExtenderBufferActor, deleter: MessageDeleteBufferActor, receipt: String) -> VisibilityTimeout {
        VisibilityTimeout {
            buf,
            receipt,
            deleter
        }
    }
}

pub enum VisibilityTimeoutMessage {
    StartVariant {
        init_timeout: Duration,
        start_time: Instant
    },
    EndVariant {
        should_delete: bool
    },
}

#[derive(Clone)]
pub struct VisibilityTimeoutActor {
    sender: Sender<VisibilityTimeoutMessage>,
    receiver: Receiver<VisibilityTimeoutMessage>,
    id: String,
}

impl VisibilityTimeoutActor {
    pub fn new(actor: VisibilityTimeout) -> VisibilityTimeoutActor
    {
        let (sender, receiver): (Sender<VisibilityTimeoutMessage>, Receiver<VisibilityTimeoutMessage>) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();

        thread::spawn(move || {
            let mut dur = Duration::from_secs(30); // Default, minimal timeout
            let mut _start_time = None;

            loop {
                if recvr.len() > 5 {
                    println!("VisibilityTimeoutActor queue len {}", recvr.len());
                }
                let recvr = recvr.clone();
                let actor = actor.clone();
                let receipt = actor.receipt.clone();
                let res = recvr.recv_timeout(dur / 2);


                match res {
                    Ok(msg) => {
                        match msg {
                            VisibilityTimeoutMessage::StartVariant { init_timeout, start_time } => {
                                actor.buf.extend(receipt.clone(), init_timeout, start_time.clone(), false, start_time.clone());

                                // we can't afford to not flush the initial timeout
                                actor.buf.flush();

                                dur = init_timeout;
                                _start_time = Some(start_time);
                            }
                            VisibilityTimeoutMessage::EndVariant { should_delete }
                            => {
                                match _start_time {
                                    Some(st) => {
                                        actor.buf.extend(receipt.clone(), dur, st.clone(), should_delete, st.clone());
                                    }
                                    None => {
                                        println!("Error, no start time provided")
                                    }
                                }
                                return;
                            }
                        }
                    }
                    Err(_) => {
                        dur = dur * 2;
                        match _start_time {
                            Some(st) => {
                                actor.buf.extend(receipt.clone(), dur, st.clone(), false, st.clone());
                            }
                            None => {
                                println!("Error, no start time provided")
                            }
                        }
                    }
                }
            }
        });

        VisibilityTimeoutActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    // 'start' sets up the VisibilityTimeout with its initial timeout
    pub fn start(&self, init_timeout: Duration, start_time: Instant) {
        self.sender.send(VisibilityTimeoutMessage::StartVariant {
            init_timeout,
            start_time
        }).expect("All receivers have died: VisibilityTimeoutMessage::StartVariant");
    }

    // 'end' stops the VisibilityTimeout from emitting any more events
    pub fn end(&self, should_delete: bool) {
        self.sender.send(VisibilityTimeoutMessage::EndVariant { should_delete })
            .expect("All receivers have died: VisibilityTimeoutMessage::EndVariant");
    }
}


#[derive(Clone)]
pub struct VisibilityTimeoutExtenderBroker
{
    workers: Vec<VisibilityTimeoutExtenderActor>,
    sender: Sender<VisibilityTimeoutExtenderMessage>,
    id: String
}

impl VisibilityTimeoutExtenderBroker
{
    pub fn new<T, SQ, F>(new: F,
                         worker_count: usize,
                         max_queue_depth: T)
                         -> VisibilityTimeoutExtenderBroker
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(VisibilityTimeoutExtenderActor) -> VisibilityTimeoutExtender<SQ>,
              T: Into<Option<usize>>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);

        let workers = (0..worker_count)
            .map(|_| VisibilityTimeoutExtenderActor::from_queue(&new, sender.clone(), receiver.clone()))
            .collect();

        VisibilityTimeoutExtenderBroker {
            workers,
            sender,
            id
        }
    }

    pub fn extend(&self, timeout_info: Vec<(String, Duration, Instant, bool, Instant)>) {
        self.sender.send(
            VisibilityTimeoutExtenderMessage::ExtendVariant {
                timeout_info
            }
        ).unwrap();
    }
}

// The VisibilityTimeoutExtenderBuffer is a 'broker' of VisibilityTimeoutExtenders. It will buffer
// messages into chunks, and send those chunks to the VisibilityTimeoutExtender
// It will buffer messages for some amount of time, or until it receives 10 messages
// in an effort to perform bulk API calls
pub struct VisibilityTimeoutExtenderBuffer
{
    extender_broker: VisibilityTimeoutExtenderBroker,
    // Replace this with a Broker to do proper work stealing
    buffer: ArrayVec<[(String, Duration, Instant, bool, Instant); 10]>,
    last_flush: Instant,
    flush_period: Duration
}

impl VisibilityTimeoutExtenderBuffer

{
    // u8::MAX is just over 4 minutes
    // Highly suggest keep the number closer to 10s of seconds at most.
    pub fn new(extender_broker: VisibilityTimeoutExtenderBroker, flush_period: u8) -> VisibilityTimeoutExtenderBuffer

    {
        VisibilityTimeoutExtenderBuffer {
            extender_broker,
            buffer: ArrayVec::new(),
            last_flush: Instant::now(),
            flush_period: Duration::from_secs(flush_period as u64)
        }
    }


    pub fn extend(&mut self, receipt: String, timeout: Duration, start_time: Instant, should_delete: bool, init_time: Instant) {
        if self.buffer.is_full() {
            self.flush();
        }
        self.buffer.push((receipt, timeout, start_time, should_delete, init_time));

        //        if should_delete {
        //            self.flush()
        //        }
    }

    fn dedup_cache(&mut self) {
        self.buffer.as_mut().sort();
        use std::iter::FromIterator;
        let mut temp_buf = self.buffer.as_ref().to_owned();
        temp_buf.dedup();
        if temp_buf.len() < self.buffer.len() {
            self.buffer = ArrayVec::from_iter(temp_buf.into_iter());
        }
    }

    pub fn flush(&mut self) {
        self.extender_broker.extend(Vec::from(self.buffer.as_ref()));
        self.buffer.clear();
    }

    pub fn drop_message(&mut self, receipt: String) {
        if self.buffer.len() == 0 {
            println!("Race to clear buffer failed");
            return;
        }
        let mut index = Some(0);

        for (i, &(ref r, _, _, _, _)) in self.buffer.iter().enumerate() {
            if *r == receipt {
                index = Some(i);
                break
            }
        }

        let index = match index {
            Some(index) => {
                println!("Successfully removed message before race");
                index
            }
            None => {
                println!("Race to clear buffer failed");
                return;
            }
        };

        self.buffer.swap_remove(index);
    }

    pub fn on_timeout(&mut self) {
        if self.buffer.len() != 0 {
            println!("VisibilityTimeoutExtenderBuffer timeout. Flushing {:#?} messages.", self.buffer.len());
            self.flush();
        }
    }
}

pub enum VisibilityTimeoutExtenderBufferMessage {
    Extend { receipt: String, timeout: Duration, start_time: Instant, should_delete: bool, init_time: Instant },
    Flush {},
    DropMessage { receipt: String },
    OnTimeout {},
}

#[derive(Clone)]
pub struct VisibilityTimeoutExtenderBufferActor {
    sender: Sender<VisibilityTimeoutExtenderBufferMessage>,
    receiver: Receiver<VisibilityTimeoutExtenderBufferMessage>,
    //    receiver: Receiver<VisibilityTimeoutExtenderBufferMessage>,
    id: String,
}

impl VisibilityTimeoutExtenderBufferActor {
    pub fn new(actor: VisibilityTimeoutExtenderBuffer)
               -> VisibilityTimeoutExtenderBufferActor
    {
        let mut actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if recvr.len() > 5 {
                        println!("VisibilityTimeoutExtenderBuffer queue len {}", recvr.len());
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
                            println!("Haven't received a message in 10 seconds");
                            continue
                        }
                    }
                }
            });

        VisibilityTimeoutExtenderBufferActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    pub fn extend(&self, receipt: String, timeout: Duration, start_time: Instant, should_delete: bool, init_time: Instant) {
        let msg = VisibilityTimeoutExtenderBufferMessage::Extend {
            receipt,
            timeout,
            should_delete,
            start_time,
            init_time
        };
        self.sender.send(msg).expect("All receivers have died.");
    }

    pub fn flush(&self) {
        let msg = VisibilityTimeoutExtenderBufferMessage::Flush {};
        self.sender.send(msg).expect("All receivers have died.");
    }

    pub fn drop_message(&self, receipt: String) {
        let msg = VisibilityTimeoutExtenderBufferMessage::DropMessage { receipt };
        self.sender.send(msg).expect("All receivers have died")
    }

    pub fn on_timeout(&self) {
        let msg = VisibilityTimeoutExtenderBufferMessage::OnTimeout {};
        self.sender.send(msg).expect("All receivers have died.");
    }
}

impl VisibilityTimeoutExtenderBuffer
{
    pub fn route_msg(&mut self, msg: VisibilityTimeoutExtenderBufferMessage) {
        match msg {
            VisibilityTimeoutExtenderBufferMessage::Extend {
                receipt,
                timeout,
                start_time,
                should_delete,
                init_time
            } => {
                self.extend(receipt, timeout, start_time, should_delete, init_time)
            }
            VisibilityTimeoutExtenderBufferMessage::Flush {} => self.flush(),
            VisibilityTimeoutExtenderBufferMessage::DropMessage { receipt } => self.drop_message(receipt),
            VisibilityTimeoutExtenderBufferMessage::OnTimeout {} => self.on_timeout(),
        };
    }
}

// BufferFlushTimer
#[derive(Clone)]
pub struct BufferFlushTimer
{
    pub buffer: VisibilityTimeoutExtenderBufferActor,
    pub period: Duration
}

impl BufferFlushTimer
{
    pub fn new(buffer: VisibilityTimeoutExtenderBufferActor, period: Duration) -> BufferFlushTimer {
        BufferFlushTimer {
            buffer,
            period
        }
    }
}

pub enum BufferFlushTimerMessage {
    StartVariant,
    End,
}

#[derive(Clone)]
pub struct BufferFlushTimerActor {
    sender: Sender<BufferFlushTimerMessage>,
    receiver: Receiver<BufferFlushTimerMessage>,
    id: String,
}

impl BufferFlushTimerActor {
    pub fn new(actor: BufferFlushTimer) -> BufferFlushTimerActor
    {
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();
        
        thread::spawn(move || {
            loop {
                if recvr.len() > 5 {
                    println!("BufferFlushTimerActor queue len {}", recvr.len());
                }
                let recvr = recvr.clone();
                let actor = actor.clone();
                let dur = actor.period; // Default, minimal timeout

                let res = recvr.recv_timeout(dur);

                match res {
                    Ok(msg) => {
                        match msg {
                            BufferFlushTimerMessage::StartVariant => {}
                            BufferFlushTimerMessage::End => {
                                actor.buffer.flush();
                                break
                            }
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        actor.buffer.on_timeout();
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        actor.buffer.flush();
                        break
                    }
                }
            }
        });

        BufferFlushTimerActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    pub fn start(&self) {
        let msg = BufferFlushTimerMessage::StartVariant;
        self.sender.send(msg).expect("All receivers have died.");
    }

    pub fn end(&self) {
        let msg = BufferFlushTimerMessage::End;
        self.sender.send(msg).expect("All receivers have died.");
    }
}

// VisibilityTimeoutExtender receives messages with receipts and timeout durations,
// and uses these to extend the timeout
// It will attempt to use bulk APIs where possible.
// It does not emit any events
#[derive(Clone)]
pub struct VisibilityTimeoutExtender<SQ>
    where SQ: Sqs + Send + Sync + 'static
{
    sqs_client: Arc<SQ>,
    queue_url: String,
    deleter: MessageDeleteBufferActor,
}

impl<SQ> VisibilityTimeoutExtender<SQ>
    where SQ: Sqs + Send + Sync + 'static
{
    pub fn new(sqs_client: Arc<SQ>, queue_url: String, deleter: MessageDeleteBufferActor) -> VisibilityTimeoutExtender<SQ> {
        VisibilityTimeoutExtender {
            sqs_client,
            queue_url,
            deleter,
        }
    }

    pub fn extend(&mut self, timeout_info: Vec<(String, Duration, Instant, bool, Instant)>) {
        let mut id_map = HashMap::with_capacity(10);

        let mut to_delete = vec![];
        let entries: Vec<_> = timeout_info.into_iter().filter_map(|(receipt, timeout, start_time, should_delete, _)| {
            let now = Instant::now();
            if start_time + timeout < now {
                println!("ERROR: MESSAGE TIMEOUT HAS EXPIRED ALREADY: {:#?} {:#?} {:#?}", start_time, timeout, now);
                return None;
            }

            if should_delete {
                to_delete.push((receipt.clone(), start_time));
                None
            } else {
                let id = format!("{}", uuid::Uuid::new_v4());

                id_map.insert(id.clone(), receipt.clone());

                Some(ChangeMessageVisibilityBatchRequestEntry {
                    id,
                    receipt_handle: receipt,
                    visibility_timeout: Some(39600 as i64)
                })
            }
        }).collect();

        if entries.is_empty() {
            for (receipt, init_time) in to_delete.into_iter() {
                self.deleter.delete_message(receipt, init_time);
            }
            return;
        }

        let req = ChangeMessageVisibilityBatchRequest {
            entries,
            queue_url: self.queue_url.clone()
        };

        let mut backoff = 5;
        loop {
            match self.sqs_client.change_message_visibility_batch(&req) {
                Ok(t) => {
                    if !t.failed.is_empty() {
                        for failed in t.failed.clone() {
                            //                            let _= id_map.get(&failed.id).unwrap();
                            //                            println!("Failed to change message timeout {:#?}", t.failed)
                        }
                    } else {
                        println!("Successfully changed message visibilities");
                    }

                    break
                }
                Err(e) => {
                    thread::sleep(Duration::from_secs(2));
                    backoff += 1;
                    if backoff > 3 {
                        println!("Failed to change message visibility {}", e);
                        break
                    } else {
                        continue
                    }
                }
            };
        }

        for (receipt, init_time) in to_delete.into_iter() {
            self.deleter.delete_message(receipt, init_time);
        }
    }
}

pub enum VisibilityTimeoutExtenderMessage {
    ExtendVariant {
        timeout_info: Vec<(String, Duration, Instant, bool, Instant)>,
    },
}

#[derive(Clone)]
pub struct VisibilityTimeoutExtenderActor {
    sender: Sender<VisibilityTimeoutExtenderMessage>,
    receiver: Receiver<VisibilityTimeoutExtenderMessage>,
    id: String,
}

impl VisibilityTimeoutExtenderActor {
    pub fn from_queue<SQ, F>(new: &F,
                             sender: Sender<VisibilityTimeoutExtenderMessage>,
                             receiver: Receiver<VisibilityTimeoutExtenderMessage>)
                             -> VisibilityTimeoutExtenderActor
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(VisibilityTimeoutExtenderActor) -> VisibilityTimeoutExtender<SQ>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let actor = VisibilityTimeoutExtenderActor {
            sender: sender.clone(),
            receiver: receiver.clone(),
            id: id,
        };

        let mut _actor = new(actor.clone());

        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if recvr.len() > 5 {
                        println!("VisibilityTimeoutExtenderActor queue len {}", recvr.len());
                    }
                    match recvr.recv_timeout(Duration::from_secs(60)) {
                        Ok(msg) => {
                            _actor.route_msg(msg);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            println!("Haven't received a message in 10 seconds");
                        }
                    }
                }
            });

        actor
    }


    pub fn new<SQ>(actor: VisibilityTimeoutExtender<SQ>) -> VisibilityTimeoutExtenderActor
        where SQ: Sqs + Send + Sync + 'static {
        let mut actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();

        thread::spawn(
            move || {
                loop {
                    if recvr.len() > 5 {
                        println!("VisibilityTimeoutExtenderActor queue len {}", recvr.len());
                    }
                    match recvr.recv_timeout(Duration::from_secs(60)) {
                        Ok(msg) => {
                            actor.route_msg(msg);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            println!("Haven't received a message in 10 seconds");
                        }
                    }
                }
            });

        VisibilityTimeoutExtenderActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    pub fn extend(&self, timeout_info: Vec<(String, Duration, Instant, bool, Instant)>) {
        let msg = VisibilityTimeoutExtenderMessage::ExtendVariant { timeout_info };
        self.sender.send(msg).expect("All receivers have died.");
    }
}

impl<SQ> VisibilityTimeoutExtender<SQ>
    where SQ: Sqs + Send + Sync + 'static
{
    pub fn route_msg(&mut self, msg: VisibilityTimeoutExtenderMessage) {
        match msg {
            VisibilityTimeoutExtenderMessage::ExtendVariant { timeout_info } => {
                self.extend(timeout_info)
            }
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rusoto_sqs;
    use rusoto_credential::*;
    use rusoto_sns::*;
    use two_lock_queue::channel;
    use std::sync::*;
    use std::collections::VecDeque;
    use std;
    use std::mem::forget;
    use std::fs::OpenOptions;
    use slog;
    use slog_json;
    use slog_scope;
    use slog_stdlog;
    use slog::{Drain, FnValue};
    use base64::encode;

    #[cfg(feature = "flame_it")]
    use flame;
    use std::fs::File;

    struct MockSqs {}

    impl Sqs for MockSqs {
        fn receive_message(&self, input: &rusoto_sqs::ReceiveMessageRequest) -> Result<rusoto_sqs::ReceiveMessageResult, rusoto_sqs::ReceiveMessageError> {
            unimplemented!()
        }

        fn add_permission(&self, input: &rusoto_sqs::AddPermissionRequest) -> Result<(), rusoto_sqs::AddPermissionError> {
            unimplemented!()
        }


        fn change_message_visibility(&self,
                                     input: &rusoto_sqs::ChangeMessageVisibilityRequest)
                                     -> Result<(), rusoto_sqs::ChangeMessageVisibilityError> {
            Ok(())
        }


        fn change_message_visibility_batch
        (&self,
         input: &rusoto_sqs::ChangeMessageVisibilityBatchRequest)
         -> Result<rusoto_sqs::ChangeMessageVisibilityBatchResult, rusoto_sqs::ChangeMessageVisibilityBatchError> {
            thread::sleep(Duration::from_millis(15)); // network latency
            Ok(
                rusoto_sqs::ChangeMessageVisibilityBatchResult {
                    failed: vec![],
                    successful: vec![]
                }
            )
        }


        fn create_queue(&self,
                        input: &rusoto_sqs::CreateQueueRequest)
                        -> Result<rusoto_sqs::CreateQueueResult, rusoto_sqs::CreateQueueError> {
            unimplemented!()
        }


        fn delete_message(&self, input: &rusoto_sqs::DeleteMessageRequest) -> Result<(), rusoto_sqs::DeleteMessageError> {
            unimplemented!()
        }


        fn delete_message_batch(&self,
                                input: &rusoto_sqs::DeleteMessageBatchRequest)
                                -> Result<rusoto_sqs::DeleteMessageBatchResult, rusoto_sqs::DeleteMessageBatchError> {
            thread::sleep(Duration::from_millis(10)); // network latency
            Ok(
                rusoto_sqs::DeleteMessageBatchResult {
                    failed: vec![],
                    successful: vec![]
                }
            )
        }


        fn delete_queue(&self, input: &rusoto_sqs::DeleteQueueRequest) -> Result<(), rusoto_sqs::DeleteQueueError> {
            unimplemented!()
        }


        fn get_queue_attributes(&self,
                                input: &rusoto_sqs::GetQueueAttributesRequest)
                                -> Result<rusoto_sqs::GetQueueAttributesResult, rusoto_sqs::GetQueueAttributesError> {
            unimplemented!()
        }


        fn get_queue_url(&self,
                         input: &rusoto_sqs::GetQueueUrlRequest)
                         -> Result<rusoto_sqs::GetQueueUrlResult, rusoto_sqs::GetQueueUrlError> {
            unimplemented!()
        }


        fn list_dead_letter_source_queues
        (&self,
         input: &rusoto_sqs::ListDeadLetterSourceQueuesRequest)
         -> Result<rusoto_sqs::ListDeadLetterSourceQueuesResult, rusoto_sqs::ListDeadLetterSourceQueuesError> {
            unimplemented!()
        }


        fn list_queues(&self, input: &rusoto_sqs::ListQueuesRequest) -> Result<rusoto_sqs::ListQueuesResult, rusoto_sqs::ListQueuesError> {
            unimplemented!()
        }


        fn purge_queue(&self, input: &rusoto_sqs::PurgeQueueRequest) -> Result<(), rusoto_sqs::PurgeQueueError> {
            unimplemented!()
        }


        fn remove_permission(&self,
                             input: &rusoto_sqs::RemovePermissionRequest)
                             -> Result<(), rusoto_sqs::RemovePermissionError> {
            unimplemented!()
        }


        fn send_message(&self,
                        input: &rusoto_sqs::SendMessageRequest)
                        -> Result<rusoto_sqs::SendMessageResult, rusoto_sqs::SendMessageError> {
            unimplemented!()
        }


        fn send_message_batch(&self,
                              input: &rusoto_sqs::SendMessageBatchRequest)
                              -> Result<rusoto_sqs::SendMessageBatchResult, rusoto_sqs::SendMessageBatchError> {
            unimplemented!()
        }


        fn set_queue_attributes(&self,
                                input: &rusoto_sqs::SetQueueAttributesRequest)
                                -> Result<(), rusoto_sqs::SetQueueAttributesError> {
            unimplemented!()
        }
    }


    use fibers;

    #[test]
    fn test_happy() {
        set_timer();

        let sqs_client = Arc::new(MockSqs {});
        let queue_url = "some url".to_owned();

        let extender = VisibilityTimeoutExtender::new(sqs_client.clone(), queue_url.clone());
        let extender = VisibilityTimeoutExtenderActor::new(extender);

        let broker = VisibilityTimeoutExtenderBroker::new(
            |actor| {
                VisibilityTimeoutExtender::new(sqs_client.clone(), queue_url.clone())
            },
            10,
            None);


        let buffer = VisibilityTimeoutExtenderBuffer::new(broker, 4);
        let buffer = VisibilityTimeoutExtenderBufferActor::new(buffer);

        let flusher = BufferFlushTimer::new(buffer.clone(), Duration::from_secs(1));
        BufferFlushTimerActor::new(flusher);

        let timeout_manager = VisibilityTimeoutManager::new(Timer::default(), buffer);
        let timeout_manager = VisibilityTimeoutManagerActor::new(timeout_manager);

        for i in 0..500 {
            timeout_manager.register(format!("receipt{}", i), Duration::from_secs(30));
        }

        pool.run();

        loop {
            thread::park()
        }
    }
}
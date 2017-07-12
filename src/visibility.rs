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
use consumer::{self, ConsumerThrottlerActor};
use util::millis;

use lru_time_cache::LruCache;
use arraydeque::ArrayDeque;
use std::cmp::{min, max};
use std::iter::{self, FromIterator};

/// The MessageStateManager manages the local message's state in the SQS service. That is, it will
/// handle maintaining the messages visibility, and it will handle deleting the message
/// Anything actors that may impact this state should likely be invoked or managed by this actor
pub struct MessageStateManager
{
    timers: LruCache<String, (VisibilityTimeoutActor, Instant)>,
    buffer: VisibilityTimeoutExtenderBufferActor,
    deleter: MessageDeleteBufferActor,
    throttler: ConsumerThrottlerActor,
    proc_times: ArrayDeque<[u32; 1024]>,
    backlog_limit: usize
}

impl MessageStateManager
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(buffer: VisibilityTimeoutExtenderBufferActor,
               deleter: MessageDeleteBufferActor,
               throttler: ConsumerThrottlerActor) -> MessageStateManager
    {
        let proc_times = ArrayDeque::from_iter(
            iter::repeat(120)
        );

        // Create the new MessageStateManager with a maximum cache  lifetime of 12 hours, which is
        // the maximum amount of time a message can be kept invisible
        MessageStateManager {
            timers: LruCache::with_expiry_duration(Duration::from_secs(12 * 60 * 60)),
            buffer,
            deleter,
            throttler,
            proc_times,
            backlog_limit: 500
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn register(&mut self, receipt: String, visibility_timeout: Duration, start_time: Instant) {
        let vis_timeout = VisibilityTimeout::new(self.buffer.clone(), self.deleter.clone(), receipt.clone());
        let vis_timeout = VisibilityTimeoutActor::new(vis_timeout);
        vis_timeout.start(visibility_timeout, start_time);

        self.timers.insert(receipt, (vis_timeout, start_time));
    }

    fn handle_backlog(&self) {
        if self.timers.len() > self.backlog_limit {
            let backlog = self.timers.len() - self.backlog_limit;
            let timeout = (millis(self.get_average_ms()) + 10) * backlog as u64;
            println!("In Flight message count larger than backlog_limit: {}.\
            Throttling: backlog {} for {}", self.backlog_limit, backlog, timeout);
            self.throttler.throttle(Duration::from_millis(timeout));
        } else {

            let avg = millis(self.get_average_ms());

            let additional = match self.backlog_limit - self.timers.len() {
                0 ... 10 => 500,
                10 ... 100 => 100,
                100 ... 1000 => 10,
                _ => 0
            };

            let timeout = avg + additional as u64;
            println!("In Flight message count larger than backlog_limit: {}.\
            Throttling: Preemptive throttle for {}", self.backlog_limit, timeout);
            self.throttler.throttle(Duration::from_millis(timeout));
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn deregister(&mut self, receipt: String, should_delete: bool) {
        let vis_timeout = self.timers.remove(&receipt);

        match vis_timeout {
            Some((vis_timeout, start_time)) => {
                let now = Instant::now();

                let proc_time = millis(now - start_time) as u32;

                self.proc_times.pop_front();

                self.proc_times.push_back(proc_time);

                self.backlog_limit = self.get_max_backlog(Duration::from_secs(28)) as usize;

                vis_timeout.end(should_delete);
            }
            None => {
                println!("Attempting to deregister timeout that does not exist:\
                receipt: {} should_delete: {}", receipt, should_delete);
            }
        };
    }

    // Given a timeout of n seconds, and our current processing times,
    // what is the number of messages we can process within that timeout
    fn get_max_backlog(&self, dur: Duration) -> u64 {
        let max_ms = millis(dur);
        let proc_time = max(1, millis(self.get_average_ms()));

        let max_msgs = max_ms / proc_time;

        min(max_msgs, 10) as u64
    }

    fn get_average_ms(&self) -> Duration {
        if self.proc_times.is_empty() {
            Duration::from_millis(80)
        } else {
            let mut avg = 0;

            for time in &self.proc_times {
                avg += *time;
                avg /= self.proc_times.len() as u32
            }
            Duration::from_millis(avg as u64)
        }
    }
}

pub enum MessageStateManagerMessage {
    RegisterVariant {
        receipt: String,
        visibility_timeout: Duration,
        start_time: Instant
    },
    DeregisterVariant { receipt: String, should_delete: bool },
}

#[derive(Clone)]
pub struct MessageStateManagerActor {
    pub sender: Sender<MessageStateManagerMessage>,
    receiver: Receiver<MessageStateManagerMessage>,
    id: String,
}

impl MessageStateManagerActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: MessageStateManager)
               -> MessageStateManagerActor
    {
        let mut actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let recvr = receiver.clone();

        thread::spawn(
            move || {
                loop {
                    //                    if recvr.len() > 5 {
                    ////                        println!("VisibilityTimeoutManagerActor queue len {}", recvr.len());
                    //                    }
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

        MessageStateManagerActor {
            sender: sender,
            receiver: receiver,
            id: id,
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn register(&self, receipt: String, visibility_timeout: Duration, start_time: Instant) {
        let msg = MessageStateManagerMessage::RegisterVariant {
            receipt,
            visibility_timeout,
            start_time
        };
        self.sender.send(msg).expect("All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn deregister(&self, receipt: String, should_delete: bool) {
        if !should_delete {
            println!("Error when processing message, apparently!");
        }

        let msg = MessageStateManagerMessage::DeregisterVariant { receipt, should_delete };
        self.sender.send(msg).expect("All receivers have died.");
    }
}

impl MessageStateManager
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn route_msg(&mut self, msg: MessageStateManagerMessage) {
        match msg {
            MessageStateManagerMessage::RegisterVariant {
                receipt, visibility_timeout, start_time
            } =>
                self.register(receipt, visibility_timeout, start_time),
            MessageStateManagerMessage::DeregisterVariant { receipt, should_delete } => {
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
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(buf: VisibilityTimeoutExtenderBufferActor,
               deleter: MessageDeleteBufferActor,
               receipt: String) -> VisibilityTimeout {
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
    #[cfg_attr(feature = "flame_it", flame)]
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
                                actor.buf.extend(receipt.clone(), init_timeout, start_time.clone(), false);

                                // we can't afford to not flush the initial timeout
                                actor.buf.flush();

                                dur = init_timeout;
                                _start_time = Some(start_time);
                            }
                            VisibilityTimeoutMessage::EndVariant { should_delete }
                            => {
                                match _start_time {
                                    Some(st) => {
                                        actor.buf.extend(receipt.clone(), dur, st.clone(), should_delete);
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
                        let dur = match dur.checked_mul(2) {
                            Some(d) => d,
                            None => dur
                        };

                        match _start_time {
                            Some(st) => {
                                actor.buf.extend(receipt.clone(), dur, st.clone(), false);
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
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn start(&self, init_timeout: Duration, start_time: Instant) {
        self.sender.send(VisibilityTimeoutMessage::StartVariant {
            init_timeout,
            start_time
        }).expect("All receivers have died: VisibilityTimeoutMessage::StartVariant");
    }

    // 'end' stops the VisibilityTimeout from emitting any more events
    #[cfg_attr(feature = "flame_it", flame)]
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
    #[cfg_attr(feature = "flame_it", flame)]
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

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&self, timeout_info: Vec<(String, Duration, Instant, bool)>) {
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
    buffer: ArrayVec<[(String, Duration, Instant, bool); 10]>,
    last_flush: Instant,
    flush_period: Duration
}

impl VisibilityTimeoutExtenderBuffer

{
    // u8::MAX is just over 4 minutes
    // Highly suggest keep the number closer to 10s of seconds at most.
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(extender_broker: VisibilityTimeoutExtenderBroker, flush_period: u8) -> VisibilityTimeoutExtenderBuffer

    {
        VisibilityTimeoutExtenderBuffer {
            extender_broker,
            buffer: ArrayVec::new(),
            last_flush: Instant::now(),
            flush_period: Duration::from_secs(flush_period as u64)
        }
    }


    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&mut self, receipt: String, timeout: Duration, start_time: Instant, should_delete: bool) {
        if self.buffer.is_full() {
            println!("MessageDeleteBuffer buffer full. Flushing.");
            self.flush();
        }

        self.buffer.push((receipt, timeout, start_time, should_delete));
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

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn flush(&mut self) {
        self.extender_broker.extend(Vec::from(self.buffer.as_ref()));
        self.buffer.clear();
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn drop_message(&mut self, receipt: String) {
        if self.buffer.len() == 0 {
            println!("Race to clear buffer failed");
            return;
        }
        let mut index = Some(0);

        for (i, &(ref r, _, _, _)) in self.buffer.iter().enumerate() {
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

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn on_timeout(&mut self) {
        if self.buffer.len() != 0 {
            println!("VisibilityTimeoutExtenderBuffer timeout. Flushing {:#?} messages.", self.buffer.len());
            self.flush();
        }
    }
}

pub enum VisibilityTimeoutExtenderBufferMessage {
    Extend { receipt: String, timeout: Duration, start_time: Instant, should_delete: bool },
    Flush {},
    DropMessage { receipt: String },
    OnTimeout {},
}

#[derive(Clone)]
pub struct VisibilityTimeoutExtenderBufferActor {
    sender: Sender<VisibilityTimeoutExtenderBufferMessage>,
    receiver: Receiver<VisibilityTimeoutExtenderBufferMessage>,
    id: String,
}

impl VisibilityTimeoutExtenderBufferActor {
    #[cfg_attr(feature = "flame_it", flame)]
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
                    match recvr.recv_timeout(actor.flush_period) {
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

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&self, receipt: String, timeout: Duration, start_time: Instant, should_delete: bool) {
        let msg = VisibilityTimeoutExtenderBufferMessage::Extend {
            receipt,
            timeout,
            should_delete,
            start_time,
        };
        self.sender.send(msg).expect("All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn flush(&self) {
        let msg = VisibilityTimeoutExtenderBufferMessage::Flush {};
        self.sender.send(msg).expect("All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn drop_message(&self, receipt: String) {
        let msg = VisibilityTimeoutExtenderBufferMessage::DropMessage { receipt };
        self.sender.send(msg).expect("All receivers have died")
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn on_timeout(&self) {
        let msg = VisibilityTimeoutExtenderBufferMessage::OnTimeout {};
        self.sender.send(msg).expect("All receivers have died.");
    }
}

impl VisibilityTimeoutExtenderBuffer
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn route_msg(&mut self, msg: VisibilityTimeoutExtenderBufferMessage) {
        match msg {
            VisibilityTimeoutExtenderBufferMessage::Extend {
                receipt,
                timeout,
                start_time,
                should_delete,
            } => {
                self.extend(receipt, timeout, start_time, should_delete)
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
    #[cfg_attr(feature = "flame_it", flame)]
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
    #[cfg_attr(feature = "flame_it", flame)]
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

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn start(&self) {
        let msg = BufferFlushTimerMessage::StartVariant;
        self.sender.send(msg).expect("All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
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
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(sqs_client: Arc<SQ>, queue_url: String, deleter: MessageDeleteBufferActor) -> VisibilityTimeoutExtender<SQ> {
        VisibilityTimeoutExtender {
            sqs_client,
            queue_url,
            deleter,
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&mut self, timeout_info: Vec<(String, Duration, Instant, bool)>) {
        let mut id_map = HashMap::with_capacity(10);

        let mut to_delete = vec![];
        let entries: Vec<_> = timeout_info.into_iter().filter_map(|(receipt, timeout, start_time, should_delete)| {
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
        timeout_info: Vec<(String, Duration, Instant, bool)>,
    },
}

#[derive(Clone)]
pub struct VisibilityTimeoutExtenderActor {
    sender: Sender<VisibilityTimeoutExtenderMessage>,
    receiver: Receiver<VisibilityTimeoutExtenderMessage>,
    id: String,
}

impl VisibilityTimeoutExtenderActor {
    #[cfg_attr(feature = "flame_it", flame)]
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


    #[cfg_attr(feature = "flame_it", flame)]
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

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&self, timeout_info: Vec<(String, Duration, Instant, bool)>) {
        let msg = VisibilityTimeoutExtenderMessage::ExtendVariant { timeout_info };
        self.sender.send(msg).expect("All receivers have died.");
    }
}

impl<SQ> VisibilityTimeoutExtender<SQ>
    where SQ: Sqs + Send + Sync + 'static
{
    #[cfg_attr(feature = "flame_it", flame)]
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
    use serde_json;
    use delay::DelayMessage;
    #[cfg(feature = "flame_it")]
    use flame;
    use std::fs::File;
    use uuid::Uuid;

    struct MockSqs {}

    struct MockSns {}

    impl Sqs for MockSqs {
        fn receive_message(&self, input: &rusoto_sqs::ReceiveMessageRequest) -> Result<rusoto_sqs::ReceiveMessageResult, rusoto_sqs::ReceiveMessageError> {
            thread::sleep(Duration::from_millis(1));

            let mut messages = vec![];
            for _ in 0..10 {
                let delay_message = serde_json::to_string(
                    &DelayMessage {
                        message: Uuid::new_v4().to_string(),
                        topic_name: "topic".to_owned(),
                        correlation_id: Some("foo".to_owned()),
                    }).unwrap();

                messages.push(
                    rusoto_sqs::Message {
                        attributes: None,
                        body: Some(encode(&delay_message)),
                        md5_of_body: Some("md5body".to_owned()),
                        md5_of_message_attributes: Some("md5attrs".to_owned()),
                        message_attributes: None,
                        message_id: Some(Uuid::new_v4().to_string()),
                        receipt_handle: Some(Uuid::new_v4().to_string()),
                    }
                )
            }

            Ok(rusoto_sqs::ReceiveMessageResult {
                messages: Some(messages)
            }
            )
        }

        fn add_permission(&self, input: &rusoto_sqs::AddPermissionRequest) -> Result<(), rusoto_sqs::AddPermissionError> {
            unimplemented!()
        }


        fn change_message_visibility(&self,
                                     input: &rusoto_sqs::ChangeMessageVisibilityRequest)
                                     -> Result<(), rusoto_sqs::ChangeMessageVisibilityError> {
            thread::sleep(Duration::from_millis(10));
            Ok(())
        }


        fn change_message_visibility_batch
        (&self,
         input: &rusoto_sqs::ChangeMessageVisibilityBatchRequest)
         -> Result<rusoto_sqs::ChangeMessageVisibilityBatchResult, rusoto_sqs::ChangeMessageVisibilityBatchError> {
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
            Ok(
                rusoto_sqs::CreateQueueResult {
                    queue_url: Some("queueurl".to_owned())
                }
            )
        }


        fn delete_message(&self, input: &rusoto_sqs::DeleteMessageRequest) -> Result<(), rusoto_sqs::DeleteMessageError> {
            unimplemented!()
        }


        fn delete_message_batch(&self,
                                input: &rusoto_sqs::DeleteMessageBatchRequest)
                                -> Result<rusoto_sqs::DeleteMessageBatchResult, rusoto_sqs::DeleteMessageBatchError> {
            thread::sleep(Duration::from_millis(10));
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

    impl Sns for MockSns {
        fn add_permission(&self, input: &AddPermissionInput) -> Result<(), AddPermissionError> {
            unimplemented!()
        }


        fn check_if_phone_number_is_opted_out
        (&self,
         input: &CheckIfPhoneNumberIsOptedOutInput)
         -> Result<CheckIfPhoneNumberIsOptedOutResponse, CheckIfPhoneNumberIsOptedOutError> {
            unimplemented!()
        }


        fn confirm_subscription(&self,
                                input: &ConfirmSubscriptionInput)
                                -> Result<ConfirmSubscriptionResponse, ConfirmSubscriptionError> {
            unimplemented!()
        }


        fn create_platform_application
        (&self,
         input: &CreatePlatformApplicationInput)
         -> Result<CreatePlatformApplicationResponse, CreatePlatformApplicationError> {
            unimplemented!()
        }


        fn create_platform_endpoint(&self,
                                    input: &CreatePlatformEndpointInput)
                                    -> Result<CreateEndpointResponse, CreatePlatformEndpointError> {
            unimplemented!()
        }


        fn create_topic(&self,
                        input: &CreateTopicInput)
                        -> Result<CreateTopicResponse, CreateTopicError> {
            Ok(
                CreateTopicResponse {
                    topic_arn: Some("Arn".to_owned())
                }
            )
        }


        fn delete_endpoint(&self, input: &DeleteEndpointInput) -> Result<(), DeleteEndpointError> {
            unimplemented!()
        }


        fn delete_platform_application(&self,
                                       input: &DeletePlatformApplicationInput)
                                       -> Result<(), DeletePlatformApplicationError> {
            unimplemented!()
        }


        fn delete_topic(&self, input: &DeleteTopicInput) -> Result<(), DeleteTopicError> {
            unimplemented!()
        }


        fn get_endpoint_attributes
        (&self,
         input: &GetEndpointAttributesInput)
         -> Result<GetEndpointAttributesResponse, GetEndpointAttributesError> {
            unimplemented!()
        }


        fn get_platform_application_attributes
        (&self,
         input: &GetPlatformApplicationAttributesInput)
         -> Result<GetPlatformApplicationAttributesResponse, GetPlatformApplicationAttributesError> {
            unimplemented!()
        }


        fn get_sms_attributes(&self,
                              input: &GetSMSAttributesInput)
                              -> Result<GetSMSAttributesResponse, GetSMSAttributesError> {
            unimplemented!()
        }


        fn get_subscription_attributes
        (&self,
         input: &GetSubscriptionAttributesInput)
         -> Result<GetSubscriptionAttributesResponse, GetSubscriptionAttributesError> {
            unimplemented!()
        }


        fn get_topic_attributes(&self,
                                input: &GetTopicAttributesInput)
                                -> Result<GetTopicAttributesResponse, GetTopicAttributesError> {
            unimplemented!()
        }


        fn list_endpoints_by_platform_application
        (&self,
         input: &ListEndpointsByPlatformApplicationInput)
         -> Result<ListEndpointsByPlatformApplicationResponse,
             ListEndpointsByPlatformApplicationError> {
            unimplemented!()
        }


        fn list_phone_numbers_opted_out
        (&self,
         input: &ListPhoneNumbersOptedOutInput)
         -> Result<ListPhoneNumbersOptedOutResponse, ListPhoneNumbersOptedOutError> {
            unimplemented!()
        }


        fn list_platform_applications
        (&self,
         input: &ListPlatformApplicationsInput)
         -> Result<ListPlatformApplicationsResponse, ListPlatformApplicationsError> {
            unimplemented!()
        }


        fn list_subscriptions(&self,
                              input: &ListSubscriptionsInput)
                              -> Result<ListSubscriptionsResponse, ListSubscriptionsError> {
            unimplemented!()
        }


        fn list_subscriptions_by_topic
        (&self,
         input: &ListSubscriptionsByTopicInput)
         -> Result<ListSubscriptionsByTopicResponse, ListSubscriptionsByTopicError> {
            unimplemented!()
        }


        fn list_topics(&self, input: &ListTopicsInput) -> Result<ListTopicsResponse, ListTopicsError> {
            unimplemented!()
        }


        fn opt_in_phone_number(&self,
                               input: &OptInPhoneNumberInput)
                               -> Result<OptInPhoneNumberResponse, OptInPhoneNumberError> {
            unimplemented!()
        }


        fn publish(&self, input: &PublishInput) -> Result<PublishResponse, PublishError> {
            thread::sleep(Duration::from_millis(15));
            Ok(PublishResponse {
                message_id: Some("id".to_owned())
            })
        }


        fn remove_permission(&self,
                             input: &RemovePermissionInput)
                             -> Result<(), RemovePermissionError> {
            unimplemented!()
        }


        fn set_endpoint_attributes(&self,
                                   input: &SetEndpointAttributesInput)
                                   -> Result<(), SetEndpointAttributesError> {
            unimplemented!()
        }


        fn set_platform_application_attributes(&self,
                                               input: &SetPlatformApplicationAttributesInput)
                                               -> Result<(), SetPlatformApplicationAttributesError> {
            unimplemented!()
        }


        fn set_sms_attributes(&self,
                              input: &SetSMSAttributesInput)
                              -> Result<SetSMSAttributesResponse, SetSMSAttributesError> {
            unimplemented!()
        }


        fn set_subscription_attributes(&self,
                                       input: &SetSubscriptionAttributesInput)
                                       -> Result<(), SetSubscriptionAttributesError> {
            unimplemented!()
        }


        fn set_topic_attributes(&self,
                                input: &SetTopicAttributesInput)
                                -> Result<(), SetTopicAttributesError> {
            unimplemented!()
        }


        fn subscribe(&self, input: &SubscribeInput) -> Result<SubscribeResponse, SubscribeError> {
            unimplemented!()
        }


        fn unsubscribe(&self, input: &UnsubscribeInput) -> Result<(), UnsubscribeError> {
            unimplemented!()
        }
    }


    use fibers;
    use processor::*;
    use consumer::*;
    use visibility::*;
    use delete::*;
    use publish::*;
    use itertools::Itertools;
    use futures_cpupool::CpuPool;
    use dogstatsd::{Client, Options};
    use hyper;
    use util;
    use util::TopicCreator;

    #[test]
    fn test_happy() {
        util::set_timer();
        let timer = util::get_timer();
        let provider = util::get_profile_provider();
        let queue_name = "local-dev-cobrien-TEST_QUEUE";
        let queue_url = "some queue url".to_owned();

        let metrics = Arc::new(Client::new(Options::default()).expect("Failure to create metrics"));

        let sqs_client = Arc::new(new_sqs_client(&provider));

        let deleter = MessageDeleterBroker::new(
            |_| {
                MessageDeleter::new(sqs_client.clone(), queue_url.clone())
            },
            350,
            None
        );

        let deleter = MessageDeleteBuffer::new(deleter, Duration::from_millis(50));
        let deleter = MessageDeleteBufferActor::new(deleter);

        let delete_flusher = DeleteBufferFlusher::new(deleter.clone(), Duration::from_secs(1));
        DeleteBufferFlusherActor::new(delete_flusher.clone());

        let sqs_client = Arc::new(new_sqs_client(&provider));
        let sns_client = Arc::new(new_sns_client(&provider));

        let broker = VisibilityTimeoutExtenderBroker::new(
            |actor| {
                VisibilityTimeoutExtender::new(sqs_client.clone(), queue_url.clone(), deleter.clone())
            },
            550,
            None
        );

        let buffer = VisibilityTimeoutExtenderBuffer::new(broker, 2);
        let buffer = VisibilityTimeoutExtenderBufferActor::new(buffer);

        let flusher = BufferFlushTimer::new(buffer.clone(), Duration::from_millis(200));
        let flusher = BufferFlushTimerActor::new(flusher);

        let consumer_throttler = ConsumerThrottler::new();
        let consumer_throttler = ConsumerThrottlerActor::new(consumer_throttler);

        let state_manager = MessageStateManager::new(buffer, deleter.clone(), consumer_throttler.clone());
        let state_manager = MessageStateManagerActor::new(state_manager);

        let publisher = MessagePublisherBroker::new(
            |actor| {
                MessagePublisher::new(sns_client.clone(), state_manager.clone())
            },
            500,
            None
        );

        let processor = DelayMessageProcessorBroker::new(
            |actor| {
                DelayMessageProcessor::new(state_manager.clone(), publisher.clone(), TopicCreator::new(sns_client.clone()))
            },
            100,
            100_000
        );

        let sqs_client = Arc::new(new_sqs_client(&provider));
        let sqs_broker = DelayMessageConsumerBroker::new(
            |actor| {
                DelayMessageConsumer::new(sqs_client.clone(), queue_url.clone(), metrics.clone(), actor, state_manager.clone(), processor.clone())
            },
            10,
            None
        );

        consumer_throttler.register_consumer(sqs_broker.clone());

        // Maximum number of in flight messages
        for worker in sqs_broker.workers {
            worker.consume()
        }

        println!("consumers started");

        //        thread::sleep(Duration::from_secs(10));
        //        println!("Writing stats to disk");
        ////        flame::dump_html(&mut File::create("flame-graph.html").unwrap()).unwrap();
        //        thread::sleep(Duration::from_secs(120));
        loop {
            thread::park()
        }
    }

    fn new_sqs_client<P>(sqs_provider: &P) -> MockSqs
        where P: ProvideAwsCredentials + Clone + Send + 'static
    {
        MockSqs {}
    }

    fn new_sns_client<P>(sns_provider: &P) -> MockSns
        where P: ProvideAwsCredentials + Clone + Send + 'static
    {
        MockSns {}
    }
}
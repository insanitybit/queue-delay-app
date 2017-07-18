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
use slog_scope;
use consumer::ConsumerThrottlerActor;

use lru_time_cache::LruCache;
use std::cmp::{min, max};

/// The MessageStateManager manages the local message's state in the SQS service. That is, it will
/// handle maintaining the messages visibility, and it will handle deleting the message
/// Anything actors that may impact this state should likely be invoked or managed by this actor
pub struct MessageStateManager
{
    timers: LruCache<String, (VisibilityTimeoutActor, Instant)>,
    buffer: VisibilityTimeoutExtenderBufferActor,
    deleter: MessageDeleteBufferActor,
}

impl MessageStateManager
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(buffer: VisibilityTimeoutExtenderBufferActor,
               deleter: MessageDeleteBufferActor) -> MessageStateManager
    {
        // Create the new MessageStateManager with a maximum cache  lifetime of 12 hours, which is
        // the maximum amount of time a message can be kept invisible
        MessageStateManager {
            timers: LruCache::with_expiry_duration_and_capacity(Duration::from_secs(12 * 60 * 60), 120_000),
            buffer,
            deleter,
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn register(&mut self, receipt: String, visibility_timeout: Duration, start_time: Instant) {
        let vis_timeout = VisibilityTimeout::new(self.buffer.clone(), self.deleter.clone(), receipt.clone());
        let vis_timeout = VisibilityTimeoutActor::new(vis_timeout);
        vis_timeout.start(visibility_timeout, start_time);

        self.timers.insert(receipt, (vis_timeout, start_time));
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn deregister(&mut self, receipt: String, should_delete: bool) {
        let vis_timeout = self.timers.remove(&receipt);

        match vis_timeout {
            Some((vis_timeout, _)) => {
                vis_timeout.end(should_delete);
            }
            None => {
                warn!(slog_scope::logger(), "Attempting to deregister timeout that does not exist:\
                receipt: {} should_delete: {}", receipt, should_delete);
            }
        };
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
        self.sender.send(msg).expect("MessageStateManagerActor.register : All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn deregister(&self, receipt: String, should_delete: bool) {
        let msg = MessageStateManagerMessage::DeregisterVariant { receipt, should_delete };
        self.sender.send(msg).expect("MessageStateManagerActor.deregister : All receivers have died.");
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
                                        error!(slog_scope::logger(), "Error, no start time provided")
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
                                error!(slog_scope::logger(), "No start time provided")
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
#[derive(Clone)]
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
            self.flush();
        }

        self.buffer.push((receipt, timeout, start_time, should_delete));
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn flush(&mut self) {
        self.extender_broker.extend(Vec::from(self.buffer.as_ref()));
        self.buffer.clear();
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn on_timeout(&mut self) {
        if self.buffer.len() != 0 {
            self.flush();
        }
    }
}

pub enum VisibilityTimeoutExtenderBufferMessage {
    Extend { receipt: String, timeout: Duration, start_time: Instant, should_delete: bool },
    Flush {},
    OnTimeout {},
}

#[derive(Clone)]
pub struct VisibilityTimeoutExtenderBufferActor {
    sender: Sender<VisibilityTimeoutExtenderBufferMessage>,
    id: String,
}

impl VisibilityTimeoutExtenderBufferActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: VisibilityTimeoutExtenderBuffer)
               -> VisibilityTimeoutExtenderBufferActor
    {
        let mut actor = actor;
        let (vis_sender, vis_receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let vis_recvr = vis_receiver;

        for _ in 0..1 {
            let recvr = vis_recvr.clone();
            let mut actor = actor.clone();
            thread::spawn(
                move || {
                    loop {
                        match recvr.recv_timeout(actor.flush_period) {
                            Ok(msg) => {
                                actor.route_msg(msg);
                                continue
                            }
                            Err(RecvTimeoutError::Disconnected) => {
                                break
                            }
                            Err(RecvTimeoutError::Timeout) => {
                                actor.route_msg(VisibilityTimeoutExtenderBufferMessage::Flush {});
                                continue
                            }
                        }
                    }
                });
        }

        let mut actor = actor;
        let (del_sender, del_receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();
        let del_recvr = del_receiver;

        for _ in 0..1 {
            let recvr = del_recvr.clone();
            let mut actor = actor.clone();
            thread::spawn(
                move || {
                    loop {
                        match recvr.recv_timeout(actor.flush_period) {
                            Ok(msg) => {
                                actor.route_msg(msg);
                                continue
                            }
                            Err(RecvTimeoutError::Disconnected) => {
                                break
                            }
                            Err(RecvTimeoutError::Timeout) => {
                                actor.route_msg(VisibilityTimeoutExtenderBufferMessage::Flush {});
                                continue
                            }
                        }
                    }
                });
        }


        let (sender2, receiver2): (Sender<VisibilityTimeoutExtenderBufferMessage>, _) = unbounded();
        let recvr2 = receiver2.clone();
        let mut actor = actor.clone();
        thread::spawn(
            move || {
                loop {
                    match recvr2.recv_timeout(actor.flush_period) {
                        Ok(msg) => {
                            match msg {
                                VisibilityTimeoutExtenderBufferMessage::Extend {
                                    should_delete, ..
                                } => {
                                    if should_delete {
                                        del_sender.send(msg);
                                    } else {
                                        vis_sender.send(msg);
                                    }
                                }
                                msg => actor.route_msg(msg)
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            actor.route_msg(VisibilityTimeoutExtenderBufferMessage::Flush {});
                            continue
                        }
                    }
                }
            });


        VisibilityTimeoutExtenderBufferActor {
            sender: sender2,
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
        self.sender.send(msg).expect("VisibilityTimeoutExtenderBufferActor.send: All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn flush(&self) {
        let msg = VisibilityTimeoutExtenderBufferMessage::Flush {};
        self.sender.send(msg).expect("VisibilityTimeoutExtenderBufferActor.flush All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn on_timeout(&self) {
        let msg = VisibilityTimeoutExtenderBufferMessage::OnTimeout {};
        self.sender.send(msg).expect("VisibilityTimeoutExtenderBufferActor.on_timeout All receivers have died.");
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
        self.sender.send(msg).expect("BufferFlushTimerActor.start: All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn end(&self) {
        let msg = BufferFlushTimerMessage::End;
        self.sender.send(msg).expect("BufferFlushTimerActor.end: All receivers have died.");
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
        let mut id_map = HashMap::with_capacity(timeout_info.len());

        let mut to_delete = vec![];
        let entries: Vec<_> = timeout_info.into_iter().filter_map(|(receipt, timeout, start_time, should_delete)| {
            let now = Instant::now();
            if start_time + timeout < now + Duration::from_millis(10) {
                error!(slog_scope::logger(), "Message timeout expired before extend: start_time {:#?} timeout {:#?} now {:#?}", start_time, timeout, now);
                if should_delete {
                    to_delete.push((receipt.clone(), start_time));
                }
                return None;
            }

            if should_delete {
                to_delete.push((receipt.clone(), start_time));
                None
            } else {
                let id = format!("{}", uuid::Uuid::new_v4());

                id_map.insert(id.clone(), (receipt.clone(), timeout, start_time));

                Some(ChangeMessageVisibilityBatchRequestEntry {
                    id,
                    receipt_handle: receipt,
                    visibility_timeout: Some(timeout.as_secs() as i64)
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

        let mut backoff = 0;
        loop {
            match self.sqs_client.change_message_visibility_batch(&req) {
                Ok(t) => {
                    if !t.failed.is_empty() {
                        let mut to_retry = Vec::with_capacity(t.failed.len());
                        for failed in t.failed.clone() {
                            to_retry.push(id_map.get(&failed.id).unwrap().clone());
                        }
                        self.retry_extend(to_retry, 0);
                    }
                    trace!(slog_scope::logger(), "Successfully updated visibilities for {} messages", t.successful.len());
                    break
                }
                Err(e) => {
                    backoff += 1;
                    thread::sleep(Duration::from_secs(20 * backoff));
                    if backoff > 5 {
                        warn!(slog_scope::logger(), "Failed to change message visibility {}", e);
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

    fn retry_extend(&mut self, timeout_info: Vec<(String, Duration, Instant)>, attempts: usize) {
        if attempts > 10 {
            warn!(slog_scope::logger(), "Failed to retry_extend {} messages", timeout_info.len());
            return;
        }

        let mut id_map = HashMap::with_capacity(timeout_info.len());

        let entries: Vec<_> = timeout_info.into_iter().flat_map(|(receipt, timeout, start_time)| {
            let now = Instant::now();
            if start_time + timeout < now + Duration::from_millis(10) {
                error!(slog_scope::logger(), "Message timeout expired before extend: start_time {:#?} timeout {:#?} now {:#?}", start_time, timeout, now);
                None
            } else {
                let id = format!("{}", uuid::Uuid::new_v4());
                id_map.insert(id.clone(), (receipt.clone(), timeout, start_time));

                Some(ChangeMessageVisibilityBatchRequestEntry {
                    id,
                    receipt_handle: receipt,
                    visibility_timeout: Some(timeout.as_secs() as i64)
                })
            }
        }).collect();

        if entries.is_empty() {
            return;
        }

        let req = ChangeMessageVisibilityBatchRequest {
            entries,
            queue_url: self.queue_url.clone()
        };

        let mut backoff = 0;
        loop {
            match self.sqs_client.change_message_visibility_batch(&req) {
                Ok(t) => {
                    if !t.failed.is_empty() {
                        let mut to_retry = Vec::with_capacity(t.failed.len());
                        for failed in t.failed.clone() {
                            to_retry.push(id_map.get(&failed.id).unwrap().clone());
                        }
                        thread::sleep(Duration::from_millis(5 * attempts as u64 + 1));
                        self.retry_extend(to_retry, attempts + 1);
                    }
                    break
                }
                Err(e) => {
                    backoff += 1;
                    thread::sleep(Duration::from_millis(5 * backoff));
                    if backoff > 5 {
                        warn!(slog_scope::logger(), "Failed to change message visibility {}", e);
                        break
                    } else {
                        continue
                    }
                }
            };
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
                    match recvr.recv_timeout(Duration::from_secs(60)) {
                        Ok(msg) => {
                            _actor.route_msg(msg);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {}
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
                    match recvr.recv_timeout(Duration::from_secs(60)) {
                        Ok(msg) => {
                            actor.route_msg(msg);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {}
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
        self.sender.send(msg).expect("VisibilityTimeoutExtenderActor.extend: All receivers have died.");
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
    use std::sync::{Arc, Mutex};

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockSqs {
        pub receives: Arc<AtomicUsize>,
        pub deletes: Arc<AtomicUsize>
    }

    struct MockSns {
        pub publishes: Arc<AtomicUsize>
    }

    impl Sqs for MockSqs {
        fn receive_message(&self, input: &rusoto_sqs::ReceiveMessageRequest) -> Result<rusoto_sqs::ReceiveMessageResult, rusoto_sqs::ReceiveMessageError> {
            //            thread::sleep(Duration::from_millis(25));

            let mut rc = self.receives.clone();

            if rc.load(Ordering::Relaxed) >= 1_000 {
                thread::sleep(Duration::from_secs(20));
                return Ok(rusoto_sqs::ReceiveMessageResult {
                    messages: None
                });
            }

            rc.fetch_add(10, Ordering::Relaxed);

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
            //            thread::sleep(Duration::from_millis(10));
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
            let msg_count = input.entries.len();
            self.deletes.fetch_add(msg_count, Ordering::Relaxed);
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
            let mut rc = self.publishes.clone();

            rc.fetch_add(1, Ordering::Relaxed);

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
    use autoscaling::*;
    use delete::*;
    use publish::*;
    use itertools::Itertools;
    use futures_cpupool::CpuPool;
    use dogstatsd::{Client, Options};
    use hyper;
    use util;
    use util::TopicCreator;
    use xorshift::{self, Rng};

    #[test]
    fn test_thousand() {
        util::set_timer();
        let timer = util::get_timer();

        let log_path = "test_everything.log";
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(log_path)
            .expect(&format!("Failed to create log file {}", log_path));

        // create logger
        let logger = slog::Logger::root(
            Mutex::new(slog_json::Json::default(file)).map(slog::Fuse),
            o!("version" => env!("CARGO_PKG_VERSION"),
           "place" =>
              FnValue(move |info| {
                  format!("{}:{} {}",
                          info.file(),
                          info.line(),
                          info.module())
              }))
        );

        // slog_stdlog uses the logger from slog_scope, so set a logger there
        let _guard = slog_scope::set_global_logger(logger);


        let provider = util::get_profile_provider();
        let queue_name = "local-dev-cobrien-TEST_QUEUE";
        let queue_url = "some queue url".to_owned();

        let metrics = Arc::new(Client::new(Options::default()).expect("Failure to create metrics"));

        let sqs_client = Arc::new(new_sqs_client(&provider));


        let throttler = Throttler::new();
        let throttler = ThrottlerActor::new(throttler);

        let deleter = MessageDeleterBroker::new(
            |_| {
                MessageDeleter::new(sqs_client.clone(), queue_url.clone(), throttler.clone())
            },
            550,
            None
        );

        let deleter = MessageDeleteBuffer::new(deleter, Duration::from_millis(50));
        let deleter = MessageDeleteBufferActor::new(deleter);

        let delete_flusher = DeleteBufferFlusher::new(deleter.clone(), Duration::from_secs(1));
        DeleteBufferFlusherActor::new(delete_flusher.clone());

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

        let state_manager = MessageStateManager::new(buffer, deleter.clone());
        let state_manager = MessageStateManagerActor::new(state_manager);

        let processor = MessageHandlerBroker::new(
            |_| {
                let publisher = MessagePublisher::new(sns_client.clone(), state_manager.clone());
                DelayMessageProcessor::new(publisher, TopicCreator::new(sns_client.clone()))
            },
            100,
            1000,
            state_manager.clone()
        );

        let mut sqs_broker = DelayMessageConsumerBroker::new(
            |actor| {
                DelayMessageConsumer::new(sqs_client.clone(), queue_url.clone(), metrics.clone(), actor, state_manager.clone(), processor.clone(), throttler.clone())
            },
            100,
            None
        );

        consumer_throttler.register_consumer(sqs_broker.clone());

        throttler.register_consumer_throttler(consumer_throttler);

        {
            let mut workers = sqs_broker.workers.iter();
            let first = workers.next().unwrap();
            first.consume();

            time!({
                for worker in workers {
                    worker.consume();
                }
                loop {
                    let count = sqs_client.deletes.load(Ordering::Relaxed);

                    if count < 1_000 {

                    } else {
                        let count = sqs_client.deletes.store(0, Ordering::Relaxed);
                        break
                    }
                }
            }, "completed");
        }
        // Remove all consumer threads
        sqs_broker.shut_down();

        thread::sleep(Duration::from_secs(5));

        assert!(sns_client.publishes.load(Ordering::Relaxed) == 1000);
    }

    #[test]
    fn test_throttler() {
        util::set_timer();
        let timer = util::get_timer();

        let log_path = "test_everything.log";
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(log_path)
            .expect(&format!("Failed to create log file {}", log_path));

        // create logger
        let logger = slog::Logger::root(
            Mutex::new(slog_json::Json::default(file)).map(slog::Fuse),
            o!("version" => env!("CARGO_PKG_VERSION"),
           "place" =>
              FnValue(move |info| {
                  format!("{}:{} {}",
                          info.file(),
                          info.line(),
                          info.module())
              }))
        );

        // slog_stdlog uses the logger from slog_scope, so set a logger there
        let _guard = slog_scope::set_global_logger(logger);

        let mut throttler = Throttler::new();

        let old_limit = throttler.get_inflight_limit();

        for _ in 0..64 {
            throttler.message_start("receipt".to_owned(), Instant::now());
            throttler.message_stop("receipt".to_owned(), Instant::now(), false);
        }

        assert_eq!(old_limit, throttler.get_inflight_limit());

        // Given an increase in processing times we expect our inflight message
        // limit to decrease
        for _ in 0..64 {
            throttler.message_start("receipt".to_owned(), Instant::now());
            thread::sleep(Duration::from_millis(10));
            throttler.message_stop("receipt".to_owned(), Instant::now(), true);
        }

        let old_limit = throttler.get_inflight_limit();

        for _ in 0..64 {
            throttler.message_start("receipt".to_owned(), Instant::now());
            thread::sleep(Duration::from_millis(50));
            throttler.message_stop("receipt".to_owned(), Instant::now(), true);
        }

        let new_limit = throttler.get_inflight_limit();

        assert!(old_limit > new_limit);

        thread::sleep(Duration::from_millis(150));
    }

    #[test]
    fn test_deleter() {
        util::set_timer();
        let timer = util::get_timer();

        let log_path = "test_everything.log";
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(log_path)
            .expect(&format!("Failed to create log file {}", log_path));

        // create logger
        let logger = slog::Logger::root(
            Mutex::new(slog_json::Json::default(file)).map(slog::Fuse),
            o!("version" => env!("CARGO_PKG_VERSION"),
           "place" =>
              FnValue(move |info| {
                  format!("{}:{} {}",
                          info.file(),
                          info.line(),
                          info.module())
              }))
        );

        let provider = util::get_profile_provider();
        let queue_name = "local-dev-cobrien-TEST_QUEUE";
        let queue_url = "some queue url".to_owned();

        let metrics = Arc::new(Client::new(Options::default()).expect("Failure to create metrics"));

        let sqs_client = Arc::new(new_sqs_client(&provider));


        let throttler = Throttler::new();
        let throttler = ThrottlerActor::new(throttler);

        let mut deleter = MessageDeleter::new(sqs_client.clone(), queue_url.clone(), throttler.clone());

        deleter.delete_messages(vec![("receipt1".to_owned(), Instant::now())]);

        thread::sleep(Duration::from_millis(5));

        assert_eq!(sqs_client.deletes.load(Ordering::Relaxed), 1);
    }

    fn new_sqs_client<P>(sqs_provider: &P) -> MockSqs
        where P: ProvideAwsCredentials + Clone + Send + 'static
    {
        MockSqs {
            receives: Arc::new(AtomicUsize::new(0)),
            deletes: Arc::new(AtomicUsize::new(0))
        }
    }

    fn new_sns_client<P>(sns_provider: &P) -> MockSns
        where P: ProvideAwsCredentials + Clone + Send + 'static
    {
        MockSns {
            publishes: Arc::new(AtomicUsize::new(0))
        }
    }
}
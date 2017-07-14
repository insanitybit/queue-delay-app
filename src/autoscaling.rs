use arraydeque::ArrayDeque;
use lru_time_cache::LruCache;
use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded, channel};
use uuid;

use std::cmp::{min, max};
use std::iter::{self, FromIterator};
use std::time::{Instant, Duration};
use std::thread;

use consumer::*;
use util::*;

type CurrentActors = usize;

pub enum ScaleMetric {
    QueueDepth {
        depth: usize,
        current_actor_count: usize
    },
    ProcessingTime {
        process_time: Duration,
        current_actor_count: usize
    },
    EmptyReceives {
        current_actor_count: usize
    }
}

pub enum ScaleMessage {
    Up(usize),
    Down(usize)
}

pub trait Scalable {
    fn scale(&mut self, scale: ScaleMessage);
}

pub struct ScalingPolicy
{
    /// Minimum number of actors to scale down to
    min_actors: usize,
    /// Maximum number of actors to scale up to
    max_actors: usize,
    /// The minimum duration between emitting scaling commands
    wait_time: Duration,
    /// Function to determine how to scale
    should_scale: Box<Fn(ScaleMetric) -> Option<ScaleMessage>>
}

impl ScalingPolicy
{
    pub fn new(min_actors: usize, max_actors: usize, wait_time: Duration, should_scale: Box<Fn(ScaleMetric) -> Option<ScaleMessage>>)
               -> ScalingPolicy {
        ScalingPolicy {
            min_actors,
            max_actors,
            wait_time,
            should_scale
        }
    }

    pub fn default_algorithm(min_actors: usize, max_actors: usize, wait_time: Duration)
                             -> ScalingPolicy {
        ScalingPolicy {
            min_actors,
            max_actors,
            wait_time,
            should_scale: Box::new(move |metric| {
                match metric {
                    ScaleMetric::EmptyReceives { current_actor_count } => {
                        if current_actor_count > min_actors {
                            Some(ScaleMessage::Down(current_actor_count - 1))
                        } else {
                            None
                        }
                    }
                    ScaleMetric::ProcessingTime { process_time, current_actor_count } => {
                        if current_actor_count < max_actors && process_time > Duration::from_secs(2) {
                            Some(ScaleMessage::Up(current_actor_count + 1))
                        } else {
                            None
                        }
                    }
                    ScaleMetric::QueueDepth { depth, current_actor_count } => {
                        if current_actor_count < max_actors {
                            if depth > 1000 {
                                Some(ScaleMessage::Up(max_actors))
                            } else if depth > 100 {
                                let new_count = min(current_actor_count + 2, max_actors);
                                Some(ScaleMessage::Up(new_count))
                            } else if depth > 10 {
                                Some(ScaleMessage::Up(current_actor_count + 1))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                }
            })
        }
    }
}

pub struct AutoScaler<A>
    where A: Scalable,
{
    /// The actor to communicate scaling commands to
    scalable: A,
    /// The policy to determine how to scale
    policy: ScalingPolicy
}

impl<A> AutoScaler<A>
    where A: Scalable,
{
    pub fn new(scalable: A, policy: ScalingPolicy) -> AutoScaler<A> {
        AutoScaler {
            scalable,
            policy
        }
    }
}

enum ThrottlerMessage {
    MessageStart {
        receipt: String,
        time_started: Instant
    },
    MessageStop {
        receipt: String,
        time_stopped: Instant,
        success: bool
    },
    RegisterConsumerThrottler {
        throttler: ConsumerThrottlerActor
    }
}

#[derive(Clone)]
pub struct ThrottlerActor
{
    sender: Sender<ThrottlerMessage>,
    receiver: Receiver<ThrottlerMessage>,
    id: String
}


pub struct Throttler {
    throttler: Option<ConsumerThrottlerActor>,
    inflight_timings: LruCache<String, Instant>,
    proc_times: ArrayDeque<[u32; 1024]>,
    inflight_limit: usize,
}

impl Throttler {
    pub fn new() -> Throttler {
        let inflight_timings = LruCache::with_expiry_duration(Duration::from_secs(12 * 60 * 60));
        let proc_times = ArrayDeque::from_iter(iter::repeat(50));
        Throttler {
            throttler: None,
            inflight_timings,
            proc_times,
            inflight_limit: 0,
        }
    }

    pub fn message_start(&mut self, receipt: String, time_started: Instant) {
        println!("Registering: {}", receipt.clone());
        if let Some(_) = self.inflight_timings.insert(receipt, time_started.clone()) {
            println!("Error, message starting twice");
        }

        // If we have more inflight messages than our limit...
            // Get the number of messages over the limit
            let backlog = self.inflight_timings.len() - self.inflight_limit;
            // Calculate the average time it takes to process and delete messages
            let processing_time = millis(self.get_average_ms());
            // Throttle for the average processing time for each message
            let timeout = processing_time + 1 * backlog as u64;
        println!("{} messages above threshold of {}. Throttling consumers for {}ms. Proctime: {}ms", backlog, self.inflight_limit, timeout, processing_time);
        if self.inflight_timings.len() > self.inflight_limit * 10 {
            self.throttler.as_ref().unwrap().throttle(Duration::from_millis(timeout * 20));
            self.throttler.as_ref().unwrap().throttle(Duration::from_millis(timeout * 2));
            self.throttler.as_ref().unwrap().throttle(Duration::from_millis(timeout));
        } else if self.inflight_timings.len() > self.inflight_limit * 2 {
            self.throttler.as_ref().unwrap().throttle(Duration::from_millis(timeout * 2));
            self.throttler.as_ref().unwrap().throttle(Duration::from_millis(timeout));
        } else if self.inflight_timings.len() > self.inflight_limit {
            self.throttler.as_ref().unwrap().throttle(Duration::from_millis(timeout));
        }


//            else if self.inflight_timings.len() > self.inflight_limit * 80 / 100 {
//
//        }
    }

    pub fn message_stop(&mut self, receipt: String, time_stopped: Instant, success: bool) {
        // Get the time that the messagae was started
        let start_time = self.inflight_timings.remove(&receipt);

        match start_time {
            // We are only interested in timings for successfully processed messages, at least
            // in part because this is likely the slowest path and we want to throttle accordingly
            Some(start_time) => {
                // Calculate the time it took from message consumption to delete
                let proc_time = millis(time_stopped - start_time) as u32;

                self.proc_times.pop_front();
                self.proc_times.push_back(proc_time);
                println!("proc_time {}", proc_time);
                // Recalculate the maximum backlog based on our most recent processing times
                let new_max = self.get_max_backlog(Duration::from_secs(28));
                self.inflight_limit = if new_max == 0 {
                    println!("inflight limit {}", self.inflight_limit);
                    self.inflight_limit + 10
                } else {
                    println!("inflight limit {}", new_max);
                    new_max as usize
                };
            }
            _ => {
                println!("Attempting to deregister timeout that does not exist:\
                receipt: {} success: {}", receipt, success);
            }
        };
    }

    pub fn register_consumer_throttler(&mut self, consumer_throttler: ConsumerThrottlerActor) {
        self.throttler = Some(consumer_throttler);
    }

    // Given a timeout of n seconds, and our current processing times,
    // what is the number of messages we can process within that timeout
    fn get_max_backlog(&self, dur: Duration) -> u64 {
        let max_ms = millis(dur);
        let proc_time = millis(self.get_average_ms());

        if proc_time == 0 {
            return 0;
        }

        let max_msgs = max_ms / proc_time;

        max(max_msgs, 10) as u64
    }

    fn get_average_ms(&self) -> Duration {
        if self.proc_times.is_empty() {
            Duration::from_millis(80)
        } else {
            let mut avg = 0;

            for time in &self.proc_times {
                avg += *time;
            }
            avg /= self.proc_times.len() as u32;

            Duration::from_millis(avg as u64)
        }
    }

    fn route_msg(&mut self, msg: ThrottlerMessage) {
        match msg {
            ThrottlerMessage::MessageStart { receipt, time_started } => self.message_start(receipt, time_started),
            ThrottlerMessage::MessageStop { receipt, time_stopped, success } =>
                self.message_stop(receipt, time_stopped, success),
            ThrottlerMessage::RegisterConsumerThrottler { throttler } => self.register_consumer_throttler(throttler)
        }
    }
}


impl ThrottlerActor
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: Throttler)
               -> ThrottlerActor
    {
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        let mut actor = actor;

        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    println!("backlog: {}", recvr.len());
                    match recvr.recv_timeout(Duration::from_secs(30)) {
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

        ThrottlerActor {
            sender: sender.clone(),
            receiver: receiver.clone(),
            id: id,
        }
    }

    pub fn message_start(&self, receipt: String, time_started: Instant) {
        self.sender.send(ThrottlerMessage::MessageStart {
            receipt,
            time_started
        });
    }

    pub fn message_stop(&self, receipt: String, time_stopped: Instant, success: bool) {
        self.sender.send(ThrottlerMessage::MessageStop {
            receipt,
            time_stopped,
            success
        });
    }

    pub fn register_consumer_throttler(&self, consumer_throttler: ConsumerThrottlerActor) {
        self.sender.send(ThrottlerMessage::RegisterConsumerThrottler { throttler: consumer_throttler });
    }
}
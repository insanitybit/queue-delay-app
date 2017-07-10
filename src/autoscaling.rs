use std::time::Duration;
use std::cmp::min;
type CurrentActors = usize;

pub enum ScaleMetric {
    QueueDepth{
        depth: usize,
        current_actor_count: usize},
    ProcessingTime{
        process_time: Duration,
        current_actor_count: usize
    },
    EmptyReceives{
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
                        if current_actor_count < max_actors && process_time > Duration::from_secs(2){
                            Some(ScaleMessage::Up(current_actor_count + 1))
                        } else {
                            None
                        }
                    }
                    ScaleMetric::QueueDepth { depth, current_actor_count }  => {
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
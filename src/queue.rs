use coco::deque::{self, Worker, Stealer};
use parking_lot::{Mutex, Condvar, WaitTimeoutResult};

use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (w, s) = deque::new();

    let cond = Arc::new((Mutex::new(false), Condvar::new()));

    (
        Sender {
            worker: w,
            cond: cond.clone()
        },
        Receiver {
            stealer: s,
            cond: cond.clone()
        }
    )
}

pub struct Sender<T> {
    worker: Worker<T>,
    cond: Arc<(Mutex<bool>, Condvar)>
}

impl<T> Sender<T> {
    pub fn send(&self, msg: T) {
        self.worker.push(msg);
        let &(ref lock, ref cvar) = &*self.cond;
        let mut msgs_available = lock.lock();
        *msgs_available = true;
        cvar.notify_one();
    }
}

pub struct Receiver<T> {
    stealer: Stealer<T>,
    cond: Arc<(Mutex<bool>, Condvar)>
}

impl<T> Receiver<T> {
    pub fn receive(&self) -> Option<T> {
        let &(ref lock, ref cvar) = &*self.cond;
        let mut msgs_available = lock.lock();
        while !*msgs_available {
            cvar.wait(&mut msgs_available);
        }
        self.stealer.steal()
    }

    pub fn receive_timeout(&self, dur: Duration) -> Result<T, &'static str> {
        let &(ref lock, ref cvar) = &*self.cond;
        let mut msgs_available = lock.lock();
        while !*msgs_available {
            if cvar.wait_for(&mut msgs_available, dur).timed_out() {
                return Err("Timed out");
            };
        }
        if let Some(msg) = self.stealer.steal() {
            Ok(msg)
        } else {
            Err("No messages available")
        }
    }
}

mod bench {
    use super::*;
    use test::Bencher;
    use std::thread;

    #[bench]
    fn bench_send(b: &mut Bencher) {
        let (s, r) = channel();

        let t = thread::spawn(move || {
            while let Some( _) = r.receive() {
                print!("")
            }
        });

        b.iter(|| {
            s.send("Foo");
        });

        t.join();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_q() {
        let (s, r) = channel();
        s.send("");
        r.receive().unwrap();
    }
}
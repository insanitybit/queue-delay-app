use coco::deque::{self, Worker, Stealer};
use parking_lot::{Mutex, Condvar, WaitTimeoutResult};

use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Sender<T>
    where T: Send
{
    worker: Arc<Mutex<Worker<T>>>,
    cond: Arc<(Mutex<bool>, Condvar)>
}

pub struct Receiver<T>
    where T: Send
{
    stealer: Arc<Stealer<T>>,
    cond: Arc<(Mutex<bool>, Condvar)>
}

impl<T> Clone for Sender<T>
    where T: Send
{
    fn clone(&self) -> Sender<T> {
        Sender {
            worker: self.worker.clone(),
            cond: self.cond.clone()
        }
    }
}

impl<T> Clone for Receiver<T>
    where T: Send
{
    fn clone(&self) -> Receiver<T> {
        Receiver {
            stealer: self.stealer.clone(),
            cond: self.cond.clone()
        }
    }
}

pub fn channel<T>(_: usize) -> (Sender<T>, Receiver<T>)
    where T: Send
{
    let (w, s) = deque::new();

    let cond = Arc::new((Mutex::new(false), Condvar::new()));

    (
        Sender {
            worker: Arc::new(Mutex::new(w)),
            cond: cond.clone()
        },
        Receiver {
            stealer: Arc::new(s),
            cond: cond.clone()
        }
    )
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>)
    where T: Send
{
    let (w, s) = deque::new();

    let cond = Arc::new((Mutex::new(false), Condvar::new()));

    (
        Sender {
            worker: Arc::new(Mutex::new(w)),
            cond: cond.clone()
        },
        Receiver {
            stealer: Arc::new(s),
            cond: cond.clone()
        }
    )
}

impl<T> Sender<T>
    where T: Send
{
    pub fn send(&self, msg: T) -> Result<(), &'static str> {
        self.worker.lock().push(msg);
        let &(ref lock, ref cvar) = &*self.cond;
        let mut msgs_available = lock.lock();
        *msgs_available = true;
        cvar.notify_one();
        Ok(())
    }


    pub fn len(&self) -> usize {
        self.worker.lock().len()
    }
}

impl<T> Receiver<T>
    where T: Send
{
    pub fn recv(&self) -> Option<T> {
        let &(ref lock, ref cvar) = &*self.cond;
        let mut msgs_available = lock.lock();
        while !*msgs_available {
            cvar.wait(&mut msgs_available);
            *msgs_available = false;

        }
        let msg = self.stealer.steal();

        if self.stealer.len() > 0 {
            cvar.notify_one();
        }
        msg
    }

    pub fn recv_timeout(&self, dur: Duration) -> Result<T, &'static str> {
        let &(ref lock, ref cvar) = &*self.cond;
        let mut msgs_available = lock.lock();
        while !*msgs_available {
            let w = cvar.wait_for(&mut msgs_available, dur);
            *msgs_available = false;
            if w.timed_out() {
                return Err("Timed out");
            };
        }
        if let Some(msg) = self.stealer.steal() {
            if self.stealer.len() > 0 {
            cvar.notify_one();
            }
            Ok(msg)
        } else {
            Err("No messages available")
        }
    }

    pub fn len(&self) -> usize {
        self.stealer.len()
    }
}

mod bench {
    use super::*;
    use test::Bencher;
    use std::thread;
    use two_lock_queue::unbounded;

    #[bench]
    fn bench_send(b: &mut Bencher) {
        let (s, r) = channel(0);

        let t = thread::spawn(move || {
            while let Some(_) = r.recv() {

            }
        });

        b.iter(|| {
            s.send(1u8);
        });

        t.join();
    }

    #[bench]
    fn bench_recv(b: &mut Bencher) {
        let (s, r) = channel(0);

        let t = thread::spawn(move || {
            for _ in 0..1_000_000 {
                s.send(1u8);
            }
        });

        t.join();

        b.iter(|| {
            while let Some(_) = r.recv() {

            }
        });

    }


    #[bench]
    fn bench_old_send(b: &mut Bencher) {
        let (s, r) = unbounded();

        let t = thread::spawn(move || {
            while let Ok(_) = r.recv_timeout(Duration::from_millis(1)) {

            }
        });

        b.iter(|| {
            s.send(1u8);
        });

        t.join();
    }

    #[bench]
    fn bench_old_recv(b: &mut Bencher) {
        let (s, r) = unbounded();

        let t = thread::spawn(move || {
            for _ in 0..1_000_000 {
                s.send(1u8);
            }
        });

        t.join();

        b.iter(|| {
            while let Ok(_) = r.recv_timeout(Duration::from_millis(1)) {

            }
        });

    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_q() {
        let (s, r) = channel(0);
        s.send("");
        r.recv().unwrap();
    }
}
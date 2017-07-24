#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate queue_delay_app;

//use queue_delay_app::autoscaling::StreamingMedian;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
});

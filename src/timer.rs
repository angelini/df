use time;

use std::collections::HashMap;
use std::sync::Mutex;

lazy_static! {
    static ref TIMERS: Mutex<HashMap<usize, u64>> = Mutex::new(HashMap::new());
}

pub fn start(id: usize, description: &str) {
    TIMERS.lock().unwrap().insert(id, time::precise_time_ns());
    println!("  > {}: {}", id, description);
}

pub fn stop(id: usize) {
    match TIMERS.lock().unwrap().remove(&id) {
        Some(start_time) => {
            println!("  > {}: finished in {:.5}s", id, (time::precise_time_ns() - start_time) as f64 / 1_000_000_000.0);
        }
        None => {
            println!("  > {}: error cannot find start_time", id);
        }
    }
}

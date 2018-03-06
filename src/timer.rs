use std::collections::HashMap;
use std::sync::Mutex;

lazy_static! {
    pub static ref TIMERS: Mutex<HashMap<String, u64>> = Mutex::new(HashMap::new());
}

#[macro_export]
macro_rules! timer_start {
    ($description:expr) => {{
        extern crate time;
        let id = format!("{: >16}:{: >4}", file!(), line!());
        println!(" > {} {}", id, $description);
        $crate::timer::TIMERS.lock().unwrap().insert(id.clone(), time::precise_time_ns());
        id
    }};
}

#[macro_export]
macro_rules! timer_stop {
    ($id:expr) => {{
        extern crate time;
        match $crate::timer::TIMERS.lock().unwrap().remove(&$id) {
            Some(start_time) => {
                println!(" > {} finished in {:.5}s", $id, (time::precise_time_ns() - start_time) as f64 / 1_000_000_000.0);
            }
            None => {
                println!(" > {} error cannot find start_time", $id);
            }
        }
    }};
}


#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_imports)]

use std::thread;
use std::time::Duration;

fn main() {
    let v = vec![1, 4, 67];

    let child_thread = std::thread::spawn(|| {
        let bv = v;

        for i in 0..100 {
            println!("child thread {} {:?}", i, bv);
            std::thread::sleep( Duration::new(0, 100) );
        }
    });
    
    for i in 0..50 {
        println!("parent thread {}", i);
        std::thread::sleep( Duration::new(0, 100) );
    }
    child_thread.join().unwrap();

}

use std::sync::mpsc;
use std::sync::Mutex;

#[test]
fn test_channels() {
    let (tx, rx) = mpsc::channel();
    let nthreads = 5;

    for i in 0..nthreads+1 {
        let txmine = mpsc::Sender::clone(&tx);

        thread::spawn(move || {
            let msg = format!("msg from child: {}", i);
            txmine.send(msg).unwrap();
            thread::sleep(Duration::from_secs(1));
        });
    }

    std::mem::drop(tx);

    for received in rx {
        println!("Got: {}", received);
    }
    println!("Done");
}


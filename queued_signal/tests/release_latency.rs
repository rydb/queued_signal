//! How many replaced generations stay retained while publishing at a steady rate.
//!
//! Publishes with and without a reader thread and prints the retained count, so the reclamation
//! pipeline is visible as a backlog in publishes rather than a wall clock figure.

use kovan::{flush, pin};
use queued_signal::atom_coerce::flush_reclamation;
use queued_signal::state::WriterDriver;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Copy type carried by the driver.
#[derive(Clone, Copy, Debug)]
struct Counter(u64);

fn retained(driver: &WriterDriver<Counter>) -> u64 {
    driver.queued_state.cell.reclaim_health().retained()
}

#[test]
fn release_latency_in_publishes() {
    for (label, gap) in [
        ("1 ms gap", Duration::from_millis(1)),
        ("16 ms gap", Duration::from_millis(16)),
    ] {
        let mut driver = WriterDriver::<Counter>::new(Counter(0));
        let mut peak = 0;
        for i in 1..=120u64 {
            driver.publish_value(Counter(i));
            peak = peak.max(retained(&driver));
            if i % 20 == 0 {
                println!("{label}: publish {i}, retained {}", retained(&driver));
            }
            std::thread::sleep(gap);
        }
        println!("{label}: peak retained {peak}");

        for _ in 0..4 {
            flush_reclamation();
            std::thread::sleep(Duration::from_millis(2));
        }
        println!("{label}: after settle, retained {}", retained(&driver));
    }
}

/// Publishes on this thread while another thread pins and reads, which is the app shape.
///
/// The bevy thread publishes and the render thread reads, so the reclaimer has to advance the era
/// past a pin held on a thread it does not control. The second case keeps the thread but drops the
/// read, which separates the cost of a second thread from the cost of a pin.
#[test]
fn release_latency_with_a_reader_thread() {
    run_second_thread_case("reader thread", true, 0);
    run_second_thread_case("reader thread, flush every 100 reads", true, 100);
    run_second_thread_case("second thread, no reads", false, 0);
}

fn run_second_thread_case(label: &str, reads: bool, flush_every: u32) {
    let mut driver = WriterDriver::<Counter>::new(Counter(0));
    let state = driver.queued_state.clone();
    let stop = Arc::new(AtomicBool::new(false));

    let reader = {
        let state = state.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut rounds = 0u32;
            while !stop.load(Ordering::Acquire) {
                if reads {
                    let guard = state.read();
                    black_box(guard.0);
                    drop(guard);
                    rounds += 1;
                    if flush_every > 0 && rounds % flush_every == 0 {
                        flush_reclamation();
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };

    let mut peak = 0;
    for i in 1..=200u64 {
        driver.publish_value(Counter(i));
        peak = peak.max(retained(&driver));
        if i % 100 == 0 {
            println!("{label}: publish {i}, retained {}", retained(&driver));
        }
        std::thread::sleep(Duration::from_millis(16));
    }

    stop.store(true, Ordering::Release);
    reader.join().unwrap();
    println!("{label}: peak retained {peak}, final {}", retained(&driver));

    for _ in 0..4 {
        flush_reclamation();
        std::thread::sleep(Duration::from_millis(2));
    }
    println!("{label}: after settle, retained {}", retained(&driver));
}

/// Cost of one flush per frame while many signals publish once per frame.
///
/// The drain is per thread, so this measures one call for the whole set rather than one per signal.
#[test]
fn frame_drain_cost_with_many_signals() {
    for signals in [1usize, 10, 100, 500] {
        let mut drivers: Vec<WriterDriver<Counter>> = (0..signals)
            .map(|_| WriterDriver::new(Counter(0)))
            .collect();
        let rounds = 100u64;
        let mut spent = 0.0;
        for round in 1..=rounds {
            for (index, driver) in drivers.iter_mut().enumerate() {
                driver.publish_value(Counter(round * 10_000 + index as u64));
            }
            let start = Instant::now();
            let guard = pin();
            drop(guard);
            flush();
            spent += start.elapsed().as_nanos() as f64;
        }
        println!(
            "{signals} signals, one flush per frame: {:.1} ns per frame",
            spent / rounds as f64
        );
        drop(drivers);
    }
}

/// Cost of one flush per D O M when several D O Ms share a thread.
///
/// The extra flushes find an empty slot, so this measures what the redundant calls cost.
#[test]
fn per_dom_flush_cost() {
    for flushes in [1u32, 4, 16] {
        let signals = 100usize;
        let mut drivers: Vec<WriterDriver<Counter>> = (0..signals)
            .map(|_| WriterDriver::new(Counter(0)))
            .collect();
        let rounds = 100u64;
        let mut spent = 0.0;
        for round in 1..=rounds {
            for (index, driver) in drivers.iter_mut().enumerate() {
                driver.publish_value(Counter(round * 10_000 + index as u64));
            }
            let start = Instant::now();
            for _ in 0..flushes {
                let guard = pin();
                drop(guard);
                flush();
            }
            spent += start.elapsed().as_nanos() as f64;
        }
        println!(
            "{signals} signals, {flushes} flushes per frame: {:.1} ns per frame",
            spent / rounds as f64
        );
        drop(drivers);
    }
}

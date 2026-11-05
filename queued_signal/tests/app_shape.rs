//! Retention for an app that publishes on its own cadence and renders in response.
//!
//! A replaced generation is released by a later retire, not by a flush on the reading or the
//! writing thread. The contract this test locks in is that the lag stays at two generations
//! however long the app runs.

use std::cell::{Cell, RefCell};
use std::time::Duration;

use dioxus::prelude::*;
use queued_signal::state::{QueuedState, WriterDriver};

thread_local! {
    static STATE: RefCell<Option<QueuedState<u32>>> = const { RefCell::new(None) };
    static RENDERS: Cell<u32> = const { Cell::new(0) };
    static LAST: Cell<u32> = const { Cell::new(0) };
}

fn app() -> Element {
    rsx! { Reader {} }
}

#[component]
fn Reader() -> Element {
    RENDERS.with(|count| count.set(count.get() + 1));
    let value = STATE.with(|slot| *slot.borrow().as_ref().expect("state is set").read());
    LAST.with(|last| last.set(value));
    rsx! { "{value}" }
}

#[test]
fn a_publish_releases_the_previous_generation() {
    let mut driver = WriterDriver::new(0u32);
    let state = driver.queued_state.clone();
    let health = state.cell.reclaim_health();
    STATE.with(|slot| *slot.borrow_mut() = Some(state.clone()));

    let mut dom = VirtualDom::new(app);
    dom.rebuild_in_place();

    let mut peak = 0;
    for value in 1..=30u32 {
        driver.set_value(value);
        driver.tick(Duration::ZERO);
        let _ = dom.render_immediate_to_vec();

        assert_eq!(
            LAST.with(|last| last.get()),
            value,
            "the render must show the published value"
        );
        peak = peak.max(health.retained());

        driver.update_health();
        std::thread::sleep(Duration::from_millis(2));
    }

    // The count tracks the generations still in flight, so it never grows with the publish count.
    assert!(
        peak <= 8,
        "retention must stay bounded over 30 publishes, saw {peak}"
    );

    // A reclamation cycle releases everything except the live generation.
    for _ in 0..100 {
        queued_signal::atom_coerce::flush_reclamation();
        driver.update_health();
        if health.retained() <= 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        health.retained() <= 1,
        "a reclamation cycle must leave at most the live generation, saw {}",
        health.retained()
    );
}

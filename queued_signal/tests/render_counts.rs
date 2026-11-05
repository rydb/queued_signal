//! Render counts for readers that are active and for readers that stay mounted while skipping
//! the read.
//!
//! The harness drives one pass at a time. A pass begins, applies a value, publishes, then renders
//! whatever the driver marked.

use std::cell::{Cell, RefCell};
use std::time::Duration;

use dioxus::prelude::*;
use queued_signal::state::{QueuedState, WriterDriver};

thread_local! {
    static STATE: RefCell<Option<QueuedState<u32>>> = const { RefCell::new(None) };
    static SHOW: Cell<Option<Signal<bool>>> = const { Cell::new(None) };
    static RENDERS: Cell<u32> = const { Cell::new(0) };
    static LAST: Cell<u32> = const { Cell::new(0) };
}

fn app() -> Element {
    let show = use_signal(|| true);
    SHOW.with(|slot| slot.set(Some(show)));
    rsx! { Reader {} }
}

#[component]
fn Reader() -> Element {
    RENDERS.with(|count| count.set(count.get() + 1));
    let show = SHOW.with(|slot| slot.get().expect("show signal is set"));
    if show() {
        let value = STATE.with(|slot| *slot.borrow().as_ref().expect("state is set").read());
        LAST.with(|last| last.set(value));
        rsx! { "{value}" }
    } else {
        rsx! { "off" }
    }
}

fn renders() -> u32 {
    RENDERS.with(|count| count.get())
}

fn reset_renders() {
    RENDERS.with(|count| count.set(0));
}

fn last_value() -> u32 {
    LAST.with(|last| last.get())
}

fn set_show(value: bool) {
    let mut show = SHOW.with(|slot| slot.get().expect("show signal is set"));
    show.set(value);
}

fn publish(driver: &mut WriterDriver<u32>, value: u32) {
    driver.set_value(value);
    driver.tick(Duration::ZERO);
}

#[test]
fn readers_render_once_per_publish_and_prune_a_skipped_branch() {
    let mut driver = WriterDriver::new(0u32);
    let state = driver.queued_state.clone();
    STATE.with(|slot| *slot.borrow_mut() = Some(state));
    reset_renders();
    LAST.with(|last| last.set(0));

    let mut dom = VirtualDom::new(app);
    dom.rebuild_in_place();

    // The first render reads the queued signal and claims a slot.
    assert_eq!(renders(), 1);
    assert_eq!(last_value(), 0);

    // An active reader renders exactly once per pass, with the published value.
    for value in 1..=8u32 {
        publish(&mut driver, value);
        reset_renders();
        let _ = dom.render_immediate_to_vec();
        assert_eq!(renders(), 1, "active reader must render once per publish");
        assert_eq!(last_value(), value);
    }

    // The branch turns off while the component stays mounted.
    set_show(false);
    let _ = dom.render_immediate_to_vec();
    reset_renders();

    // The slot survives a bounded number of passes, then stops causing renders.
    let mut extra = 0;
    let mut version = 8u32;
    let mut tail = Vec::new();
    for _ in 0..8 {
        version += 1;
        publish(&mut driver, version);
        let _ = dom.render_immediate_to_vec();
        extra += renders();
        tail.push(renders());
        reset_renders();
    }
    assert!(
        extra <= 3,
        "a mounted reader that skips the read must stop rendering, saw {tail:?}"
    );
    assert_eq!(
        tail.last().copied(),
        Some(0),
        "the pruned slot must stop marking, saw {tail:?}"
    );

    // Turning the branch back on renders once and shows the newest value.
    set_show(true);
    let _ = dom.render_immediate_to_vec();
    assert_eq!(renders(), 1);
    assert_eq!(last_value(), version, "the reader must not miss an update");

    // Further publishes reach the reactivated slot again.
    publish(&mut driver, version + 1);
    reset_renders();
    let _ = dom.render_immediate_to_vec();
    assert_eq!(renders(), 1);
    assert_eq!(last_value(), version + 1);
}

#[test]
fn a_publish_renders_the_arrival_order_composition() {
    let mut driver = WriterDriver::new(1u32);
    let state = driver.queued_state.clone();
    STATE.with(|slot| *slot.borrow_mut() = Some(state));
    reset_renders();
    LAST.with(|last| last.set(0));

    let mut dom = VirtualDom::new(app);
    dom.rebuild_in_place();

    driver.set_value(10);
    driver.mutate(|value: &mut u32| *value += 5);
    driver.tick(Duration::ZERO);
    reset_renders();
    let _ = dom.render_immediate_to_vec();

    assert_eq!(renders(), 1);
    assert_eq!(
        last_value(),
        15,
        "an add queued after a set composes onto that set"
    );
}

use dioxus::prelude::*;
use queued_signal::queued_signal::QueuedSignal;
use std::{time::Duration};

#[derive(Clone)]
pub struct Counter {
    signal: QueuedSignal<i32>,
}


pub fn main() {
    dioxus::LaunchBuilder::new()
        .with_context(Counter {
            signal: QueuedSignal::new(0, Duration::from_millis(100)),
        })
        .launch(app);
}

fn app() -> Element {
    let counter = use_context::<Counter>();

    // Attaches the queued signal to this component – returns a Dioxus signal
    let count_signal = counter.signal.use_attached();

    use_future(move || {
        let value = counter.signal.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                value.mutate(|x| *x += 1);
            }
        }
    });

    rsx! {
        div {
            h1 { "Signal: {count_signal}" }
        }
    }
}
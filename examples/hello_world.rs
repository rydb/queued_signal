use dioxus::prelude::*;
use queued_signal::signal::{QueuedSignalRegistry, use_queued_signal};
use std::time::Duration;

#[derive(Default, Clone)]
pub struct AppNumbers {
    pub numbers: Vec<i32>,
}

fn main() {
    let registry = QueuedSignalRegistry::new();
    dioxus::LaunchBuilder::new()
        .with_context(registry)
        .launch(app);
}

fn app() -> Element {
    let numbers = use_queued_signal::<AppNumbers>();

    let numbers_clone = numbers.clone();
    use_future(move || {
    let value = numbers_clone.clone();
    async move {
        let mut counter = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            value.mutate(move |data| {
                data.numbers.push(counter);
                let _ = counter += 1;
            }).ok();
        }
    }
    });

    let display = numbers.read_signal()()
        .as_ref()
        .map(|data| format!("len = {}, data = {:?}", data.numbers.len(), data.numbers))
        .unwrap_or_else(|| "Initializing...".to_string());

    rsx! {
        div {
            h1 { "Queued Signal Demo (Lock‑Free Reads)" }
            p { "{display}" }
        }
    }
}
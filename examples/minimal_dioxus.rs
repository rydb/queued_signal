use std::time::Duration;

use dioxus::prelude::*;
use queued_signal::signal::{QueuedSignalHub, use_queued_signal};

#[derive(Clone, Debug)]
pub struct HelloWorld(pub String);

#[derive(Clone, Debug)]
pub struct Counter(pub i32);

pub fn main() {
    let hub = QueuedSignalHub::default();
    hub.register(HelloWorld("Hello World".to_string()));
    hub.register(Counter(0));

    dioxus::LaunchBuilder::new().with_context(hub).launch(app);
}

#[component]
pub fn app() -> Element {
    let hello = use_queued_signal::<HelloWorld>();
    let counter = use_queued_signal::<Counter>();

    let hello_text = use_memo(move || {
        hello
            .read_ok(|h| h.0.clone())
            .unwrap_or_else(|err| err.into())
    });

    let counter_text = use_memo(move || {
        counter
            .read_ok(|c| c.0.to_string())
            .unwrap_or_else(|err| err.into())
    });

    let health_text = use_memo(move || format!("{:?}", counter.health()));

    use_future(move || async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            counter.mutate(|n| n.0 += 1);
        }
    });

    rsx! {
        document::Stylesheet { href: asset!("assets/ui.css") }
        div {
            h1 { "{hello_text}" }
            h2 { "Counter: {counter_text}" }
            p { "Health: {health_text}" }
            button {
                onclick: move |_| counter.mutate(|n| n.0 += 10),
                "Bump +10"
            }
        }
    }
}

use std::{fmt::Display, time::Duration};

use dioxus::prelude::*;
use queued_signal::signal::{create_queued_signal_hub, use_queued_signal};

#[derive(Clone, Debug)]
pub struct HelloWorld(pub String);

impl Display for HelloWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct Counter(pub i32);

impl Display for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub fn main() {
    let sender = create_queued_signal_hub();
    sender.register(HelloWorld("Hello World".to_string()));
    sender.register(Counter(0));

    dioxus::LaunchBuilder::new()
        .with_context(sender)
        .launch(app);
}

#[component]
pub fn app() -> Element {
    let hello = use_queued_signal::<HelloWorld>();
    let counter = use_queued_signal::<Counter>();

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
            h1 { "{hello}" }
            h2 { "Counter: {counter}" }
            p { "Health: {health_text}" }
            button {
                onclick: move |_| counter.mutate(|n| n.0 += 10),
                "Bump +10"
            }
        }
    }
}

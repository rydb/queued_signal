//! Example of a multi-dom scenario where queued_signal shares state across DOMs.
//!
use blitz_shell::{BlitzApplication, BlitzShellEvent, WindowConfig, create_default_event_loop};
use dioxus::prelude::*;
use dioxus_core::VirtualDom;
use dioxus_native::{DioxusDocument, DioxusNativeWindowRenderer, DocumentConfig, WindowAttributes};
use parley::FontContext;
use parley::fontique::Blob;
use queued_signal::signal::{QueuedSignalHub, use_queued_signal};
use std::time::Duration;
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::prelude::*;
use winit::application::ApplicationHandler;
use winit::event::WindowEvent;
use winit::event_loop::ActiveEventLoop;
use winit::window::WindowId;

#[derive(Clone, Debug)]
pub struct HelloWorld(pub String);

#[derive(Clone, Debug)]
pub struct Counter(pub i32);

pub fn main() {
    setup();
}

#[component]
fn app_window_1() -> Element {
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

#[component]
fn app_window_2() -> Element {
    let hello = use_queued_signal::<HelloWorld>();
    let counter = use_queued_signal::<Counter>();

    use_future(move || async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            counter.mutate(|n| n.0 += 1);
        }
    });

    let hello_text = use_memo(move || {
        hello
            .read_ok(|h| h.0.clone())
            .unwrap_or_else(|_| "Fetching...".into())
    });

    let counter_text = use_memo(move || {
        counter
            .read_ok(|c| c.0.to_string())
            .unwrap_or_else(|_| "Fetching...".into())
    });

    let health_text = use_memo(move || format!("{:?}", counter.health()));

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

/// Wrapper around BlitzApplication that exits when *any* window is closed.
struct App {
    inner: BlitzApplication<DioxusNativeWindowRenderer>,
}

impl App {
    fn new(proxy: winit::event_loop::EventLoopProxy<BlitzShellEvent>) -> Self {
        Self {
            inner: BlitzApplication::new(proxy),
        }
    }

    fn add_window(&mut self, config: WindowConfig<DioxusNativeWindowRenderer>) {
        self.inner.add_window(config);
    }
}

impl ApplicationHandler<BlitzShellEvent> for App {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        self.inner.resumed(event_loop);
    }

    fn suspended(&mut self, event_loop: &ActiveEventLoop) {
        self.inner.suspended(event_loop);
    }

    fn new_events(&mut self, event_loop: &ActiveEventLoop, cause: winit::event::StartCause) {
        self.inner.new_events(event_loop, cause);
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        window_id: WindowId,
        event: WindowEvent,
    ) {
        if matches!(event, WindowEvent::CloseRequested) {
            self.inner.window_event(event_loop, window_id, event);
            event_loop.exit();
        } else {
            self.inner.window_event(event_loop, window_id, event);
        }
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, event: BlitzShellEvent) {
        self.inner.user_event(event_loop, event);
    }
}

pub fn setup() {
    // Chrome trace output for diagnosing reactivity.
    let (chrome_layer, _guard) = ChromeLayerBuilder::new()
        .file("./target/minimal_dioxus_trace.json")
        .include_args(true)
        .build();
    tracing_subscriber::registry().with(chrome_layer).init();

    let hub = QueuedSignalHub::new();
    hub.register(HelloWorld("Hello World".to_string()));
    hub.register(Counter(0));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

    let font_data: &'static [u8] = include_bytes!("assets/DejaVuSans.ttf");
    let mut font_ctx = FontContext::default();
    let families = font_ctx
        .collection
        .register_fonts(Blob::from(font_data.to_vec()), None);
    if let Some((family_id, _)) = families.first() {
        use parley::fontique::GenericFamily::*;
        for generic in [Serif, SansSerif, Monospace, Cursive, Fantasy, SystemUi] {
            font_ctx
                .collection
                .set_generic_families(generic, std::iter::once(*family_id));
        }
    }

    let event_loop = create_default_event_loop::<BlitzShellEvent>();
    // Each window must have its own renderer.
    // The renderer binds to a specific window surface,
    // and sharing a single renderer causes all renders
    // to target the most recently bound surface.
    let renderer1 = DioxusNativeWindowRenderer::new();
    let renderer2 = DioxusNativeWindowRenderer::new();

    let proxy = event_loop.create_proxy();
    let mut app = App::new(proxy);

    let mut vdom1 = VirtualDom::new_with_props(app_window_1, ());
    vdom1.insert_any_root_context(Box::new(hub.clone()));
    let mut doc1 = DioxusDocument::new(
        vdom1,
        DocumentConfig {
            font_ctx: Some(font_ctx.clone()),
            ..Default::default()
        },
    );
    doc1.initial_build();
    app.add_window(WindowConfig::with_attributes(
        Box::new(doc1),
        renderer1,
        WindowAttributes::default().with_title("Window 1"),
    ));

    let mut vdom2 = VirtualDom::new_with_props(app_window_2, ());
    vdom2.insert_any_root_context(Box::new(hub.clone()));
    let mut doc2 = DioxusDocument::new(
        vdom2,
        DocumentConfig {
            font_ctx: Some(font_ctx),
            ..Default::default()
        },
    );
    doc2.initial_build();
    app.add_window(WindowConfig::with_attributes(
        Box::new(doc2),
        renderer2,
        WindowAttributes::default().with_title("Window 2"),
    ));

    event_loop.run_app(&mut app).unwrap();
}

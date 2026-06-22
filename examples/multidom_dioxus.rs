//! Example of a multi-dom scenario where queued_signal shares state across DOMs.
//!
use blitz_shell::{
    BlitzApplication, BlitzShellEvent, BlitzShellProxy, WindowConfig, create_default_event_loop,
};
use blitz_traits::net::{Bytes, NetHandler, NetProvider, Request};
use dioxus::prelude::*;
use dioxus_core::{ScopeId, VirtualDom, provide_context};
use dioxus_native::{
    DioxusDocument, DioxusNativeEvent, DioxusNativeWindowRenderer, DocumentConfig, WindowAttributes,
};
use parley::FontContext;
use parley::fontique::Blob;
use queued_signal::signal::{create_queued_signal_hub, use_queued_signal};
use std::fmt::Display;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::prelude::*;
use winit::application::ApplicationHandler;
use winit::event::WindowEvent;
use winit::event_loop::ActiveEventLoop;
use winit::window::WindowId;

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
    setup();
}

#[component]
fn app_window_1() -> Element {
    let hello = use_queued_signal::<HelloWorld>();
    let counter = use_queued_signal::<Counter>();

    let health_text = use_memo(move || format!("{:?}", counter.health()));

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

    let health_text = use_memo(move || format!("{:?}", counter.health()));

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

/// Net provider to load assets
struct AssetNetProvider;

impl NetProvider for AssetNetProvider {
    fn fetch(&self, _doc_id: usize, request: Request, handler: Box<dyn NetHandler>) {
        let asset_path = request.url.path();
        if request.url.scheme() == "dioxus" {
            match std::fs::read(asset_path) {
                Ok(bytes) => {
                    handler.bytes(request.url.to_string(), Bytes::from(bytes));
                }
                Err(_) => {
                    panic!("asset not found at: {}", asset_path);
                }
            }
        }
    }
}

/// Wrapper around BlitzApplication that exits when any window is closed.
struct App {
    inner: BlitzApplication<DioxusNativeWindowRenderer>,
    proxy: BlitzShellProxy,
}

impl App {
    fn new(
        proxy: BlitzShellProxy,
        event_queue: std::sync::mpsc::Receiver<BlitzShellEvent>,
    ) -> Self {
        Self {
            inner: BlitzApplication::new(proxy.clone(), event_queue),
            proxy,
        }
    }

    fn add_window(&mut self, config: WindowConfig<DioxusNativeWindowRenderer>) {
        self.inner.add_window(config);
    }

    fn handle_native_event(
        &mut self,
        _event_loop: &dyn ActiveEventLoop,
        event: &DioxusNativeEvent,
    ) {
        if let DioxusNativeEvent::CreateHeadElement {
            window,
            name,
            attributes,
            contents,
        } = event
            && let Some(window) = self.inner.windows.get_mut(window)
        {
            let doc = window.downcast_doc_mut::<DioxusDocument>();
            doc.create_head_element(name, attributes, contents);
            window.poll();
        }
    }
}

impl ApplicationHandler for App {
    fn can_create_surfaces(&mut self, event_loop: &dyn ActiveEventLoop) {
        self.inner.can_create_surfaces(event_loop);

        // After BlitzApplication creates windows, run initial_build so
        // the vdom renders into each document. We also provide a minimal
        // document bridge so that document::Stylesheet can add <link>
        // elements to the <head> via CreateHeadElement events.
        let proxy = self.proxy.clone();
        for window in self.inner.windows.values_mut() {
            let window_id = window.window_id();
            let doc = window.downcast_doc_mut::<DioxusDocument>();
            doc.vdom.in_scope(ScopeId::ROOT, || {
                provide_context(Rc::new(HeadBridge::new(proxy.clone(), window_id))
                    as Rc<dyn dioxus_document::Document>);
            });
            doc.initial_build();
            window.request_redraw();
        }
    }

    fn window_event(
        &mut self,
        event_loop: &dyn ActiveEventLoop,
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

    fn proxy_wake_up(&mut self, event_loop: &dyn ActiveEventLoop) {
        while let Ok(event) = self.inner.event_queue.try_recv() {
            // Intercept Embedder events before BlitzApplication drops them.
            // Dioxus head elements (Stylesheet, Meta, Script, etc.) send
            // CreateHeadElement as embedder events via the proxy.

            // This is required in order to load Document::Stylesheet
            if let BlitzShellEvent::Embedder(embedder) = &event
                && let Some(native_event) = embedder.downcast_ref::<DioxusNativeEvent>()
            {
                self.handle_native_event(event_loop, native_event);
                continue;
            }
            self.inner.handle_blitz_shell_event(event_loop, event);
        }
    }
}

// Thin bridge that implements dioxus_document::Document by sending
// CreateHeadElement events through the BlitzShellProxy.
struct HeadBridge {
    proxy: BlitzShellProxy,
    window: WindowId,
}

impl HeadBridge {
    fn new(proxy: BlitzShellProxy, window: WindowId) -> Self {
        Self { proxy, window }
    }
}

impl dioxus_document::Document for HeadBridge {
    fn eval(&self, js: String) -> dioxus_document::Eval {
        dioxus_document::NoOpDocument.eval(js)
    }

    fn create_head_element(
        &self,
        name: &str,
        attributes: &[(&str, String)],
        contents: Option<String>,
    ) {
        let event = DioxusNativeEvent::CreateHeadElement {
            window: self.window,
            name: name.to_string(),
            attributes: attributes
                .iter()
                .map(|(n, v)| (n.to_string(), v.clone()))
                .collect(),
            contents,
        };
        self.proxy
            .send_event(BlitzShellEvent::embedder_event(event));
    }
}

pub fn setup() {
    let (chrome_layer, _guard) = ChromeLayerBuilder::new()
        .file("./target/minimal_dioxus_trace.json")
        .include_args(true)
        .build();
    tracing_subscriber::registry().with(chrome_layer).init();

    let sender = create_queued_signal_hub();
    sender.register(HelloWorld("Hello World".to_string()));
    sender.register(Counter(0));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

    // Due to font loading issues on linux, the demo overrides system fonts with a pre-loaded one from the /assets folder.
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

    let event_loop = create_default_event_loop();
    let renderer1 = DioxusNativeWindowRenderer::new();
    let renderer2 = DioxusNativeWindowRenderer::new();

    let (proxy, event_queue) = BlitzShellProxy::new(event_loop.create_proxy());
    let mut app = App::new(proxy, event_queue);

    let net_provider = Arc::new(AssetNetProvider);

    let mut vdom1 = VirtualDom::new_with_props(app_window_1, ());
    vdom1.insert_any_root_context(Box::new(sender.clone()));
    let doc1 = DioxusDocument::new(
        vdom1,
        DocumentConfig {
            font_ctx: Some(font_ctx.clone()),
            net_provider: Some(net_provider.clone()),
            ..Default::default()
        },
    );
    app.add_window(WindowConfig::with_attributes(
        Box::new(doc1),
        renderer1,
        WindowAttributes::default().with_title("Window 1"),
    ));

    let mut vdom2 = VirtualDom::new_with_props(app_window_2, ());
    vdom2.insert_any_root_context(Box::new(sender.clone()));
    let doc2 = DioxusDocument::new(
        vdom2,
        DocumentConfig {
            font_ctx: Some(font_ctx),
            net_provider: Some(net_provider),
            ..Default::default()
        },
    );
    app.add_window(WindowConfig::with_attributes(
        Box::new(doc2),
        renderer2,
        WindowAttributes::default().with_title("Window 2"),
    ));

    event_loop.run_app(app).unwrap();
}

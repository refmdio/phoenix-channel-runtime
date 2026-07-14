//! Browser WebSocket transport for `wasm32-unknown-unknown`.

#![forbid(unsafe_code)]

#[cfg(target_arch = "wasm32")]
mod web {
    use std::{cell::Cell, pin::Pin, rc::Rc, time::Duration};

    use futures::{Sink, SinkExt, StreamExt, future::poll_fn};
    use gloo_net::websocket::{Message, State, WebSocketError, futures::WebSocket};
    use gloo_timers::future::TimeoutFuture;
    use phoenix_channel_client::{
        ConnectContext, Connector, Endpoint, ResolvedEndpoint, Socket, SocketStatus, Timer,
    };
    use phoenix_channel_runtime::{
        Transport, TransportClose, TransportError, TransportErrorKind, TransportEvent, WireMessage,
    };
    use wasm_bindgen::{JsCast, JsValue, closure::Closure};
    use web_sys::{Event, VisibilityState, Window};

    pub struct WebTransport {
        inner: WebSocket,
    }

    pub struct WebLifecycle {
        window: Window,
        pagehide: Closure<dyn FnMut(Event)>,
        pageshow: Closure<dyn FnMut(Event)>,
        visibilitychange: Closure<dyn FnMut(Event)>,
        offline: Closure<dyn FnMut(Event)>,
        online: Closure<dyn FnMut(Event)>,
    }

    impl WebLifecycle {
        pub fn attach(socket: Socket) -> Result<Self, JsValue> {
            let window =
                web_sys::window().ok_or_else(|| JsValue::from_str("window unavailable"))?;
            let resume_after_page_show = Rc::new(Cell::new(false));
            let resume_after_online = Rc::new(Cell::new(false));

            let pagehide = {
                let socket = socket.clone();
                let resume = resume_after_page_show.clone();
                Closure::wrap(Box::new(move |_event: Event| {
                    if is_active(socket.status()) {
                        resume.set(true);
                        let socket = socket.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let _ = socket.disconnect().await;
                        });
                    }
                }) as Box<dyn FnMut(Event)>)
            };
            let pageshow = {
                let socket = socket.clone();
                let resume = resume_after_page_show;
                Closure::wrap(Box::new(move |_event: Event| {
                    if resume.replace(false) {
                        let socket = socket.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let _ = socket.connect().await;
                        });
                    }
                }) as Box<dyn FnMut(Event)>)
            };
            let visibilitychange = {
                let socket = socket.clone();
                let window = window.clone();
                Closure::wrap(Box::new(move |_event: Event| {
                    let visible = window.document().is_some_and(|document| {
                        document.visibility_state() == VisibilityState::Visible
                    });
                    if visible && socket.status() == SocketStatus::WaitingToReconnect {
                        let socket = socket.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let _ = socket.reconnect().await;
                        });
                    }
                }) as Box<dyn FnMut(Event)>)
            };
            let offline = {
                let socket = socket.clone();
                let resume = resume_after_online.clone();
                Closure::wrap(Box::new(move |_event: Event| {
                    if is_active(socket.status()) {
                        resume.set(true);
                        let socket = socket.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let _ = socket.disconnect().await;
                        });
                    }
                }) as Box<dyn FnMut(Event)>)
            };
            let online = {
                let socket = socket.clone();
                let resume = resume_after_online;
                Closure::wrap(Box::new(move |_event: Event| {
                    if resume.replace(false) {
                        let socket = socket.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let _ = socket.connect().await;
                        });
                    }
                }) as Box<dyn FnMut(Event)>)
            };

            add_listener(&window, "pagehide", &pagehide)?;
            add_listener(&window, "pageshow", &pageshow)?;
            add_listener(&window, "visibilitychange", &visibilitychange)?;
            add_listener(&window, "offline", &offline)?;
            add_listener(&window, "online", &online)?;
            Ok(Self {
                window,
                pagehide,
                pageshow,
                visibilitychange,
                offline,
                online,
            })
        }
    }

    impl Drop for WebLifecycle {
        fn drop(&mut self) {
            remove_listener(&self.window, "pagehide", &self.pagehide);
            remove_listener(&self.window, "pageshow", &self.pageshow);
            remove_listener(&self.window, "visibilitychange", &self.visibilitychange);
            remove_listener(&self.window, "offline", &self.offline);
            remove_listener(&self.window, "online", &self.online);
        }
    }

    fn is_active(status: SocketStatus) -> bool {
        matches!(
            status,
            SocketStatus::Connecting | SocketStatus::Connected | SocketStatus::WaitingToReconnect
        )
    }

    fn add_listener(
        window: &Window,
        name: &str,
        callback: &Closure<dyn FnMut(Event)>,
    ) -> Result<(), JsValue> {
        window.add_event_listener_with_callback(name, callback.as_ref().unchecked_ref())
    }

    fn remove_listener(window: &Window, name: &str, callback: &Closure<dyn FnMut(Event)>) {
        let _ = window.remove_event_listener_with_callback(name, callback.as_ref().unchecked_ref());
    }

    #[derive(Clone)]
    pub struct WebConnector {
        endpoint: WebEndpoint,
    }

    #[derive(Clone)]
    enum WebEndpoint {
        Url(String),
        Phoenix(Endpoint),
    }

    impl WebConnector {
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                endpoint: WebEndpoint::Url(url.into()),
            }
        }

        pub fn from_endpoint(endpoint: Endpoint) -> Self {
            Self {
                endpoint: WebEndpoint::Phoenix(endpoint),
            }
        }
    }

    impl Connector for WebConnector {
        fn connect(
            &self,
            context: ConnectContext,
        ) -> futures::future::LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>
        {
            let endpoint = self.endpoint.clone();
            Box::pin(async move {
                let endpoint = match endpoint {
                    WebEndpoint::Url(url) => ResolvedEndpoint {
                        url,
                        protocols: Vec::new(),
                    },
                    WebEndpoint::Phoenix(endpoint) => {
                        endpoint.resolve(context).await.map_err(|error| {
                            TransportError::with_kind(
                                TransportErrorKind::Connect,
                                error.to_string(),
                            )
                        })?
                    }
                };
                let transport = WebTransport::connect_resolved(endpoint).await?;
                Ok(Box::new(transport) as Box<dyn Transport>)
            })
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    pub struct WebTimer;

    impl Timer for WebTimer {
        fn sleep(&self, duration: Duration) -> futures::future::LocalBoxFuture<'static, ()> {
            let milliseconds = duration.as_millis().min(u128::from(u32::MAX)) as u32;
            Box::pin(TimeoutFuture::new(milliseconds))
        }
    }

    impl WebTransport {
        pub fn connect(url: &str) -> Result<Self, TransportError> {
            let inner = WebSocket::open(url).map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, format!("{error:?}"))
            })?;
            Ok(Self { inner })
        }

        async fn connect_resolved(endpoint: ResolvedEndpoint) -> Result<Self, TransportError> {
            let mut inner = if endpoint.protocols.is_empty() {
                WebSocket::open(&endpoint.url)
            } else {
                WebSocket::open_with_protocols(&endpoint.url, &endpoint.protocols)
            }
            .map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, format!("{error:?}"))
            })?;
            poll_fn(|context| Pin::new(&mut inner).poll_ready(context))
                .await
                .map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
                })?;
            if !matches!(inner.state(), State::Open) {
                return Err(TransportError::with_kind(
                    TransportErrorKind::Connect,
                    "WebSocket closed before the open event",
                ));
            }
            Ok(Self { inner })
        }
    }

    impl Transport for WebTransport {
        fn send<'a>(
            &'a mut self,
            message: WireMessage,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                let message = match message {
                    WireMessage::Text(text) => Message::Text(text),
                    WireMessage::Binary(bytes) => Message::Bytes(bytes),
                };
                self.inner.send(message).await.map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Send, format!("{error:?}"))
                })
            })
        }

        fn receive<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
            Box::pin(async move {
                let Some(message) = self.inner.next().await else {
                    return Ok(TransportEvent::Closed(TransportClose::connection_ended()));
                };
                match message {
                    Ok(Message::Text(text)) => Ok(TransportEvent::Message(WireMessage::Text(text))),
                    Ok(Message::Bytes(bytes)) => {
                        Ok(TransportEvent::Message(WireMessage::Binary(bytes)))
                    }
                    Err(WebSocketError::ConnectionClose(close)) => Ok(TransportEvent::Closed(
                        TransportClose::new(Some(close.code), close.reason, close.was_clean),
                    )),
                    Err(error) => Err(TransportError::with_kind(
                        TransportErrorKind::Receive,
                        error.to_string(),
                    )),
                }
            })
        }

        fn close<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                SinkExt::close(&mut self.inner).await.map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Close, format!("{error:?}"))
                })
            })
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use web::{WebConnector, WebLifecycle, WebTimer, WebTransport};

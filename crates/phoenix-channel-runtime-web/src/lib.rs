#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

#[cfg(target_arch = "wasm32")]
mod web {
    use std::{
        cell::{Cell, RefCell},
        collections::{HashMap, VecDeque},
        pin::Pin,
        rc::Rc,
        time::Duration,
    };

    use base64::{
        Engine as _,
        engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
    };
    use futures::{FutureExt, Sink, SinkExt, StreamExt, channel::mpsc, future::poll_fn};
    use gloo_net::http::Request;
    use gloo_net::websocket::{Message, State, WebSocketError, futures::WebSocket};
    use gloo_timers::future::TimeoutFuture;
    use phoenix_channel_client::{
        ConnectContext, Connector, Endpoint, ResolvedEndpoint, Socket, SocketStatus, Timer,
    };
    use phoenix_channel_runtime::{
        Transport, TransportClose, TransportCloseRequest, TransportError, TransportErrorKind,
        TransportEvent, WireMessage,
    };
    use serde::Deserialize;
    use wasm_bindgen::{JsCast, JsValue, closure::Closure};
    use web_sys::{AbortController, Event, VisibilityState, Window};

    const AUTH_TOKEN_PREFIX: &str = "base64url.bearer.phx.";
    const DEFAULT_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(20);
    const HEALTH_CHECK_REFERENCE: &str = "phoenix-channel-runtime-health";

    /// Browser WebSocket transport implementing the runtime-neutral transport API.
    pub struct WebTransport {
        inner: Option<WebSocket>,
        queued: VecDeque<TransportEvent>,
    }

    /// Phoenix LongPoll transport for browsers without a usable WebSocket path.
    pub struct LongPollTransport {
        endpoint: url::Url,
        token: Rc<RefCell<Option<String>>>,
        auth_token: Option<String>,
        events: mpsc::UnboundedReceiver<Result<TransportEvent, TransportError>>,
        closed: Rc<Cell<bool>>,
        request_timeout: Duration,
        requests: Rc<RefCell<HashMap<u64, AbortController>>>,
        next_request_id: Rc<Cell<u64>>,
    }

    /// Browser page and network lifecycle listeners for a managed socket.
    ///
    /// Keep this value alive while the socket is active. Dropping it removes
    /// all installed event listeners.
    pub struct WebLifecycle {
        window: Window,
        pagehide: Closure<dyn FnMut(Event)>,
        pageshow: Closure<dyn FnMut(Event)>,
        visibilitychange: Closure<dyn FnMut(Event)>,
        offline: Closure<dyn FnMut(Event)>,
        online: Closure<dyn FnMut(Event)>,
    }

    impl WebLifecycle {
        /// Attaches page hide/show, visibility, offline, and online listeners.
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

    /// Browser connector with optional Phoenix LongPoll fallback.
    #[derive(Clone)]
    pub struct WebConnector {
        endpoint: WebEndpoint,
        long_poll_fallback: Option<Duration>,
        long_poll_timeout: Duration,
    }

    /// Browser connector that always uses Phoenix LongPoll.
    #[derive(Clone)]
    pub struct LongPollConnector {
        endpoint: WebEndpoint,
        request_timeout: Duration,
    }

    #[derive(Clone)]
    enum WebEndpoint {
        Url(String),
        Phoenix(Endpoint),
    }

    impl WebConnector {
        /// Creates a connector for an already-resolved WebSocket URL.
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                endpoint: WebEndpoint::Url(url.into()),
                long_poll_fallback: None,
                long_poll_timeout: DEFAULT_LONG_POLL_TIMEOUT,
            }
        }

        /// Creates a connector that resolves a Phoenix endpoint each attempt.
        pub fn from_endpoint(endpoint: Endpoint) -> Self {
            Self {
                endpoint: WebEndpoint::Phoenix(endpoint),
                long_poll_fallback: None,
                long_poll_timeout: DEFAULT_LONG_POLL_TIMEOUT,
            }
        }

        /// Enables LongPoll fallback after this WebSocket open or health deadline.
        pub fn long_poll_fallback(mut self, after: Duration) -> Self {
            self.long_poll_fallback = Some(after);
            self
        }

        /// Sets the timeout applied to each fallback LongPoll request.
        pub fn long_poll_timeout(mut self, timeout: Duration) -> Self {
            self.long_poll_timeout = timeout;
            self
        }
    }

    impl LongPollConnector {
        /// Creates a LongPoll connector from an already-resolved endpoint URL.
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                endpoint: WebEndpoint::Url(url.into()),
                request_timeout: DEFAULT_LONG_POLL_TIMEOUT,
            }
        }

        /// Creates a LongPoll connector that resolves a Phoenix endpoint each attempt.
        pub fn from_endpoint(endpoint: Endpoint) -> Self {
            Self {
                endpoint: WebEndpoint::Phoenix(endpoint),
                request_timeout: DEFAULT_LONG_POLL_TIMEOUT,
            }
        }

        /// Sets the timeout applied to every LongPoll request.
        pub fn request_timeout(mut self, timeout: Duration) -> Self {
            self.request_timeout = timeout;
            self
        }
    }

    async fn resolve_endpoint(
        endpoint: WebEndpoint,
        context: ConnectContext,
    ) -> Result<ResolvedEndpoint, TransportError> {
        match endpoint {
            WebEndpoint::Url(url) => Ok(ResolvedEndpoint {
                url,
                protocols: Vec::new(),
            }),
            WebEndpoint::Phoenix(endpoint) => endpoint.resolve(context).await.map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
            }),
        }
    }

    impl Connector for WebConnector {
        fn connect(
            &self,
            context: ConnectContext,
        ) -> futures::future::LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>
        {
            let endpoint = self.endpoint.clone();
            let fallback = self.long_poll_fallback;
            let long_poll_timeout = self.long_poll_timeout;
            Box::pin(async move {
                let endpoint = resolve_endpoint(endpoint, context).await?;
                if let Some(after) = fallback {
                    if let Ok(transport) =
                        WebTransport::connect_with_health_check(endpoint.clone(), after).await
                    {
                        return Ok(Box::new(transport) as Box<dyn Transport>);
                    }
                    let transport =
                        LongPollTransport::connect_resolved(endpoint, long_poll_timeout).await?;
                    Ok(Box::new(transport) as Box<dyn Transport>)
                } else {
                    let transport = WebTransport::connect_resolved(endpoint).await?;
                    Ok(Box::new(transport) as Box<dyn Transport>)
                }
            })
        }
    }

    impl Connector for LongPollConnector {
        fn connect(
            &self,
            context: ConnectContext,
        ) -> futures::future::LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>
        {
            let endpoint = self.endpoint.clone();
            let request_timeout = self.request_timeout;
            Box::pin(async move {
                let endpoint = resolve_endpoint(endpoint, context).await?;
                let transport =
                    LongPollTransport::connect_resolved(endpoint, request_timeout).await?;
                Ok(Box::new(transport) as Box<dyn Transport>)
            })
        }
    }

    /// Browser timer backed by `setTimeout` and the Performance API.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct WebTimer;

    impl Timer for WebTimer {
        fn sleep(&self, duration: Duration) -> futures::future::LocalBoxFuture<'static, ()> {
            let milliseconds = duration.as_millis().min(u128::from(u32::MAX)) as u32;
            Box::pin(TimeoutFuture::new(milliseconds))
        }

        fn now(&self) -> Duration {
            web_sys::window()
                .and_then(|window| window.performance())
                .map(|performance| Duration::from_secs_f64(performance.now() / 1000.0))
                .unwrap_or_default()
        }
    }

    impl WebTransport {
        /// Opens an already-resolved browser WebSocket URL.
        pub fn connect(url: &str) -> Result<Self, TransportError> {
            let inner = WebSocket::open(url).map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, format!("{error:?}"))
            })?;
            Ok(Self {
                inner: Some(inner),
                queued: VecDeque::new(),
            })
        }

        async fn connect_with_health_check(
            endpoint: ResolvedEndpoint,
            threshold: Duration,
        ) -> Result<Self, TransportError> {
            let connect = Self::connect_resolved(endpoint).fuse();
            let open_timeout = timeout(threshold).fuse();
            futures::pin_mut!(connect, open_timeout);
            let mut transport = futures::select! {
                result = connect => result?,
                () = open_timeout => return Err(connect_error("WebSocket open timed out")),
            };

            {
                let health_check = transport.health_check().fuse();
                let health_timeout = timeout(threshold).fuse();
                futures::pin_mut!(health_check, health_timeout);
                futures::select! {
                    result = health_check => result?,
                    () = health_timeout => return Err(connect_error("WebSocket health check timed out")),
                }
            }
            Ok(transport)
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
            Ok(Self {
                inner: Some(inner),
                queued: VecDeque::new(),
            })
        }

        async fn health_check(&mut self) -> Result<(), TransportError> {
            let message =
                serde_json::json!([null, HEALTH_CHECK_REFERENCE, "phoenix", "heartbeat", {}])
                    .to_string();
            let inner = self.inner.as_mut().ok_or_else(closed_error)?;
            inner.send(Message::Text(message)).await.map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Send, format!("{error:?}"))
            })?;

            loop {
                let Some(message) = inner.next().await else {
                    return Err(connect_error("WebSocket ended during health check"));
                };
                let event = websocket_event(message)?;
                if let TransportEvent::Message(WireMessage::Text(text)) = &event {
                    if is_health_check_ack(text) {
                        return Ok(());
                    }
                }
                if matches!(event, TransportEvent::Closed(_)) {
                    return Err(connect_error("WebSocket closed during health check"));
                }
                self.queued.push_back(event);
            }
        }
    }

    fn is_health_check_ack(text: &str) -> bool {
        let Ok(frame) = serde_json::from_str::<serde_json::Value>(text) else {
            return false;
        };
        let Some(frame) = frame.as_array() else {
            return false;
        };
        frame.get(1).and_then(serde_json::Value::as_str) == Some(HEALTH_CHECK_REFERENCE)
            && frame.get(2).and_then(serde_json::Value::as_str) == Some("phoenix")
            && frame.get(3).and_then(serde_json::Value::as_str) == Some("phx_reply")
            && frame
                .get(4)
                .and_then(|payload| payload.get("status"))
                .and_then(serde_json::Value::as_str)
                == Some("ok")
    }

    #[derive(Deserialize)]
    struct PollResponse {
        status: u16,
        #[serde(default)]
        token: Option<String>,
        #[serde(default)]
        messages: Vec<String>,
    }

    impl LongPollTransport {
        async fn connect_resolved(
            endpoint: ResolvedEndpoint,
            request_timeout: Duration,
        ) -> Result<Self, TransportError> {
            let mut url = url::Url::parse(&endpoint.url).map_err(connect_error)?;
            let scheme = match url.scheme() {
                "ws" => "http".to_owned(),
                "wss" => "https".to_owned(),
                "http" | "https" => url.scheme().to_owned(),
                scheme => {
                    return Err(connect_error(format!(
                        "unsupported LongPoll URL scheme {scheme}"
                    )));
                }
            };
            url.set_scheme(&scheme)
                .map_err(|_| connect_error("failed to set LongPoll URL scheme"))?;
            let path = url.path().trim_end_matches('/');
            let path = path.strip_suffix("/websocket").map_or_else(
                || format!("{path}/longpoll"),
                |base| format!("{base}/longpoll"),
            );
            url.set_path(&path);

            let auth_token = endpoint
                .protocols
                .get(1)
                .and_then(|protocol| protocol.strip_prefix(AUTH_TOKEN_PREFIX))
                .and_then(|encoded| STANDARD_NO_PAD.decode(encoded).ok())
                .and_then(|bytes| String::from_utf8(bytes).ok());
            let requests = Rc::new(RefCell::new(HashMap::new()));
            let next_request_id = Rc::new(Cell::new(0));
            let response = long_poll_request(
                &url,
                None,
                auth_token.as_deref(),
                None,
                request_timeout,
                &requests,
                &next_request_id,
                TransportErrorKind::Connect,
            )
            .await?;
            match response.status {
                200 | 204 | 410 => {
                    let Some(initial_token) = response.token else {
                        return Err(connect_error("LongPoll handshake did not return a token"));
                    };
                    let token = Rc::new(RefCell::new(Some(initial_token)));
                    let closed = Rc::new(Cell::new(false));
                    let (events_tx, events) = mpsc::unbounded();
                    for message in response.messages {
                        let _ = events_tx.unbounded_send(Ok(TransportEvent::Message(
                            WireMessage::Text(message),
                        )));
                    }
                    let poll_endpoint = url.clone();
                    let poll_token = token.clone();
                    let poll_auth_token = auth_token.clone();
                    let poll_closed = closed.clone();
                    let poll_requests = requests.clone();
                    let poll_next_request_id = next_request_id.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        while !poll_closed.get() {
                            let current_token = poll_token.borrow().clone();
                            let response = long_poll_request(
                                &poll_endpoint,
                                current_token.as_deref(),
                                poll_auth_token.as_deref(),
                                None,
                                request_timeout,
                                &poll_requests,
                                &poll_next_request_id,
                                TransportErrorKind::Receive,
                            )
                            .await;
                            let response = match response {
                                Ok(response) => response,
                                Err(error) => {
                                    let _ = events_tx.unbounded_send(Err(error));
                                    break;
                                }
                            };
                            match response.status {
                                200 => {
                                    if let Some(next_token) = response.token {
                                        poll_token.replace(Some(next_token));
                                    }
                                    for message in response.messages {
                                        if events_tx
                                            .unbounded_send(Ok(TransportEvent::Message(
                                                WireMessage::Text(message),
                                            )))
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                }
                                204 => {}
                                410 => {
                                    poll_closed.set(true);
                                    let _ = events_tx.unbounded_send(Ok(TransportEvent::Closed(
                                        TransportClose::new(
                                            Some(3410),
                                            "LongPoll session is gone",
                                            false,
                                        ),
                                    )));
                                }
                                403 => {
                                    poll_closed.set(true);
                                    let _ = events_tx.unbounded_send(Ok(TransportEvent::Closed(
                                        TransportClose::new(
                                            Some(1008),
                                            "LongPoll request was forbidden",
                                            false,
                                        ),
                                    )));
                                }
                                status => {
                                    let _ =
                                        events_tx.unbounded_send(Err(TransportError::with_kind(
                                            TransportErrorKind::Receive,
                                            format!("LongPoll GET returned status {status}"),
                                        )));
                                    break;
                                }
                            }
                        }
                    });
                    Ok(Self {
                        endpoint: url,
                        token,
                        auth_token,
                        events,
                        closed,
                        request_timeout,
                        requests,
                        next_request_id,
                    })
                }
                403 => Err(connect_error("LongPoll handshake was forbidden")),
                status => Err(connect_error(format!(
                    "LongPoll handshake returned status {status}"
                ))),
            }
        }

        async fn post(&self, body: String) -> Result<(), TransportError> {
            let token = self.token.borrow().clone();
            let response = long_poll_request(
                &self.endpoint,
                token.as_deref(),
                self.auth_token.as_deref(),
                Some(body),
                self.request_timeout,
                &self.requests,
                &self.next_request_id,
                TransportErrorKind::Send,
            )
            .await?;
            validate_post_status(response.status)
        }
    }

    fn validate_post_status(status: u16) -> Result<(), TransportError> {
        match status {
            200 => Ok(()),
            403 => Err(TransportError::with_kind(
                TransportErrorKind::Send,
                "LongPoll POST was forbidden",
            )),
            408 => Err(TransportError::with_kind(
                TransportErrorKind::Send,
                "LongPoll POST dispatch timed out",
            )),
            410 => Err(TransportError::with_kind(
                TransportErrorKind::Send,
                "LongPoll POST session is gone",
            )),
            status => Err(TransportError::with_kind(
                TransportErrorKind::Send,
                format!("LongPoll POST returned status {status}"),
            )),
        }
    }

    fn endpoint_url(endpoint: &url::Url, token: Option<&str>) -> String {
        let mut endpoint = endpoint.clone();
        if let Some(token) = token {
            endpoint.query_pairs_mut().append_pair("token", token);
        }
        endpoint.into()
    }

    async fn long_poll_request(
        endpoint: &url::Url,
        token: Option<&str>,
        auth_token: Option<&str>,
        body: Option<String>,
        request_timeout: Duration,
        requests: &Rc<RefCell<HashMap<u64, AbortController>>>,
        next_request_id: &Rc<Cell<u64>>,
        error_kind: TransportErrorKind,
    ) -> Result<PollResponse, TransportError> {
        let controller = AbortController::new()
            .map_err(|error| TransportError::with_kind(error_kind, format!("{error:?}")))?;
        let mut request_id = next_request_id.get().wrapping_add(1);
        if request_id == 0 {
            request_id = 1;
        }
        next_request_id.set(request_id);
        requests.borrow_mut().insert(request_id, controller.clone());

        let endpoint = endpoint_url(endpoint, token);
        let mut request = if body.is_some() {
            Request::post(&endpoint).header("Content-Type", "application/x-ndjson")
        } else {
            Request::get(&endpoint).header("Accept", "application/json")
        }
        .abort_signal(Some(&controller.signal()));
        if let Some(token) = auth_token {
            request = request.header("X-Phoenix-AuthToken", token);
        }
        let request = match match body {
            Some(body) => request.body(body),
            None => request.build(),
        } {
            Ok(request) => request,
            Err(error) => {
                requests.borrow_mut().remove(&request_id);
                return Err(TransportError::with_kind(error_kind, error.to_string()));
            }
        };
        let response = async move {
            let response = request
                .send()
                .await
                .map_err(|error| TransportError::with_kind(error_kind, error.to_string()))?;
            let http_status = response.status();
            let text = response
                .text()
                .await
                .map_err(|error| TransportError::with_kind(error_kind, error.to_string()))?;
            if text.is_empty() {
                return Ok(PollResponse {
                    status: http_status,
                    token: token.map(str::to_owned),
                    messages: Vec::new(),
                });
            }
            serde_json::from_str(&text).map_err(|error| {
                TransportError::with_kind(error_kind, format!("invalid LongPoll response: {error}"))
            })
        }
        .fuse();
        let timeout = timeout(request_timeout).fuse();
        futures::pin_mut!(response, timeout);
        let result = futures::select! {
            result = response => result,
            () = timeout => {
                controller.abort();
                Err(TransportError::with_kind(error_kind, format!(
                    "LongPoll request timed out after {request_timeout:?}"
                )))
            }
        };
        requests.borrow_mut().remove(&request_id);
        result
    }

    fn timeout(duration: Duration) -> TimeoutFuture {
        TimeoutFuture::new(duration.as_millis().min(u128::from(u32::MAX)) as u32)
    }

    fn connect_error(error: impl std::fmt::Display) -> TransportError {
        TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
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
                self.inner
                    .as_mut()
                    .ok_or_else(closed_error)?
                    .send(message)
                    .await
                    .map_err(|error| {
                        TransportError::with_kind(TransportErrorKind::Send, format!("{error:?}"))
                    })
            })
        }

        fn receive<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
            Box::pin(async move {
                if let Some(event) = self.queued.pop_front() {
                    return Ok(event);
                }
                let Some(message) = self.inner.as_mut().ok_or_else(closed_error)?.next().await
                else {
                    return Ok(TransportEvent::Closed(TransportClose::connection_ended()));
                };
                websocket_event(message)
            })
        }

        fn close<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                let Some(inner) = self.inner.take() else {
                    return Ok(());
                };
                inner.close(None, None).map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Close, format!("{error:?}"))
                })
            })
        }

        fn close_with<'a>(
            &'a mut self,
            request: TransportCloseRequest,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                let Some(inner) = self.inner.take() else {
                    return Ok(());
                };
                inner
                    .close(Some(request.code), Some(&request.reason))
                    .map_err(|error| {
                        TransportError::with_kind(TransportErrorKind::Close, format!("{error:?}"))
                    })
            })
        }
    }

    fn websocket_event(
        message: Result<Message, WebSocketError>,
    ) -> Result<TransportEvent, TransportError> {
        match message {
            Ok(Message::Text(text)) => Ok(TransportEvent::Message(WireMessage::Text(text))),
            Ok(Message::Bytes(bytes)) => Ok(TransportEvent::Message(WireMessage::Binary(bytes))),
            Err(WebSocketError::ConnectionClose(close)) => Ok(TransportEvent::Closed(
                TransportClose::new(Some(close.code), close.reason, close.was_clean),
            )),
            Err(error) => Err(TransportError::with_kind(
                TransportErrorKind::Receive,
                error.to_string(),
            )),
        }
    }

    impl Transport for LongPollTransport {
        fn supports_binary(&self) -> bool {
            false
        }

        fn send<'a>(
            &'a mut self,
            message: WireMessage,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                if self.closed.get() {
                    return Err(closed_error());
                }
                let body = match message {
                    WireMessage::Text(text) => text,
                    WireMessage::Binary(bytes) => STANDARD.encode(bytes),
                };
                self.post(body).await
            })
        }

        fn receive<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
            Box::pin(async move {
                if self.closed.get() {
                    return Ok(TransportEvent::Closed(TransportClose::new(
                        Some(1000),
                        "LongPoll transport closed",
                        true,
                    )));
                }
                self.events.next().await.unwrap_or_else(|| {
                    Ok(TransportEvent::Closed(TransportClose::connection_ended()))
                })
            })
        }

        fn close<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                self.closed.set(true);
                for (_, controller) in self.requests.borrow_mut().drain() {
                    controller.abort();
                }
                self.events.close();
                Ok(())
            })
        }
    }

    fn closed_error() -> TransportError {
        TransportError::with_kind(TransportErrorKind::Other, "WebSocket is closed")
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use wasm_bindgen_test::wasm_bindgen_test;

        wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

        #[wasm_bindgen_test]
        fn classifies_long_poll_post_statuses() {
            assert!(validate_post_status(200).is_ok());
            for (status, message) in [
                (403, "forbidden"),
                (408, "timed out"),
                (410, "session is gone"),
            ] {
                let error = validate_post_status(status).unwrap_err();
                assert_eq!(error.kind(), TransportErrorKind::Send);
                assert!(error.message().contains(message));
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use web::{
    LongPollConnector, LongPollTransport, WebConnector, WebLifecycle, WebTimer, WebTransport,
};

#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::{
        sync::{Arc, OnceLock},
        time::{Duration, Instant},
    };

    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use futures::{SinkExt, StreamExt};
    use phoenix_channel_client::{ConnectContext, Connector, Endpoint, ResolvedEndpoint, Timer};
    use phoenix_channel_runtime::{
        Transport, TransportClose, TransportCloseRequest, TransportError, TransportErrorKind,
        TransportEvent, WireMessage,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    use tokio_tungstenite::{
        Connector as TlsConnector, MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
        tungstenite::{
            Message,
            client::IntoClientRequest,
            http::{HeaderName, HeaderValue, header::SEC_WEBSOCKET_PROTOCOL},
            protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
        },
    };
    use url::Url;

    /// Tokio Tungstenite transport implementing the runtime-neutral transport API.
    pub struct NativeTransport {
        inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
    }

    /// Connector for native `ws` and `wss` WebSocket endpoints.
    #[derive(Clone)]
    pub struct NativeConnector {
        endpoint: NativeEndpoint,
        options: NativeTransportOptions,
    }

    /// HTTP CONNECT proxy URL and host bypass rules.
    #[derive(Clone)]
    pub struct ProxyConfig {
        url: String,
        bypass_hosts: Vec<String>,
    }

    impl std::fmt::Debug for ProxyConfig {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("ProxyConfig")
                .field("url", &"configured")
                .field("bypass_hosts", &self.bypass_hosts)
                .finish()
        }
    }

    impl ProxyConfig {
        /// Creates proxy configuration for an HTTP proxy URL.
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                url: url.into(),
                bypass_hosts: Vec::new(),
            }
        }

        /// Bypasses the proxy for a host and all of its subdomains.
        pub fn bypass_host(mut self, host: impl Into<String>) -> Self {
            self.bypass_hosts.push(host.into());
            self
        }

        fn applies_to(&self, host: &str) -> bool {
            !self.bypass_hosts.iter().any(|entry| {
                let entry = entry.trim_start_matches('.');
                host.eq_ignore_ascii_case(entry)
                    || host
                        .to_ascii_lowercase()
                        .ends_with(&format!(".{}", entry.to_ascii_lowercase()))
            })
        }
    }

    /// TLS, proxy, header, and WebSocket limit settings for native transports.
    #[derive(Clone)]
    pub struct NativeTransportOptions {
        headers: Vec<(String, String)>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        proxy: Option<ProxyConfig>,
        max_message_size: Option<usize>,
        max_frame_size: Option<usize>,
        disable_nagle: bool,
    }

    impl std::fmt::Debug for NativeTransportOptions {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let header_names = self
                .headers
                .iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>();
            formatter
                .debug_struct("NativeTransportOptions")
                .field("header_names", &header_names)
                .field(
                    "tls_config",
                    &self.tls_config.as_ref().map(|_| "configured"),
                )
                .field("proxy", &self.proxy)
                .field("max_message_size", &self.max_message_size)
                .field("max_frame_size", &self.max_frame_size)
                .field("disable_nagle", &self.disable_nagle)
                .finish()
        }
    }

    impl Default for NativeTransportOptions {
        fn default() -> Self {
            Self {
                headers: Vec::new(),
                tls_config: None,
                proxy: None,
                max_message_size: Some(16 * 1024 * 1024),
                max_frame_size: Some(16 * 1024 * 1024),
                disable_nagle: false,
            }
        }
    }

    impl NativeTransportOptions {
        /// Adds an HTTP header to the WebSocket upgrade request.
        pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
            self.headers.push((name.into(), value.into()));
            self
        }

        /// Uses an application-provided Rustls client configuration.
        pub fn tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
            self.tls_config = Some(config);
            self
        }

        /// Routes applicable connections through an HTTP CONNECT proxy.
        pub fn proxy(mut self, proxy: ProxyConfig) -> Self {
            self.proxy = Some(proxy);
            self
        }

        /// Sets the maximum accepted WebSocket message size, or disables it.
        pub fn max_message_size(mut self, value: Option<usize>) -> Self {
            self.max_message_size = value;
            self
        }

        /// Sets the maximum accepted WebSocket frame size, or disables it.
        pub fn max_frame_size(mut self, value: Option<usize>) -> Self {
            self.max_frame_size = value;
            self
        }

        /// Enables or disables TCP `nodelay` on direct and proxied connections.
        pub fn disable_nagle(mut self, value: bool) -> Self {
            self.disable_nagle = value;
            self
        }
    }

    #[derive(Clone)]
    enum NativeEndpoint {
        Url(String),
        Phoenix(Endpoint),
    }

    impl NativeConnector {
        /// Creates a connector for an already-resolved WebSocket URL.
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                endpoint: NativeEndpoint::Url(url.into()),
                options: NativeTransportOptions::default(),
            }
        }

        /// Creates a connector that resolves a Phoenix [`Endpoint`] each attempt.
        pub fn from_endpoint(endpoint: Endpoint) -> Self {
            Self {
                endpoint: NativeEndpoint::Phoenix(endpoint),
                options: NativeTransportOptions::default(),
            }
        }

        /// Adds an HTTP header to the WebSocket upgrade request.
        pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
            self.options = self.options.header(name, value);
            self
        }

        /// Replaces the native transport options.
        pub fn options(mut self, options: NativeTransportOptions) -> Self {
            self.options = options;
            self
        }
    }

    impl Connector for NativeConnector {
        fn connect(
            &self,
            context: ConnectContext,
        ) -> futures::future::LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>
        {
            let endpoint = self.endpoint.clone();
            let options = self.options.clone();
            Box::pin(async move {
                let endpoint = match endpoint {
                    NativeEndpoint::Url(url) => ResolvedEndpoint {
                        url,
                        protocols: Vec::new(),
                    },
                    NativeEndpoint::Phoenix(endpoint) => {
                        endpoint.resolve(context).await.map_err(|error| {
                            TransportError::with_kind(
                                TransportErrorKind::Connect,
                                error.to_string(),
                            )
                        })?
                    }
                };
                let transport = NativeTransport::connect_resolved(endpoint, options).await?;
                Ok(Box::new(transport) as Box<dyn Transport>)
            })
        }
    }

    /// Tokio timer using monotonic [`std::time::Instant`] measurements.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct NativeTimer;

    impl Timer for NativeTimer {
        fn sleep(&self, duration: Duration) -> futures::future::LocalBoxFuture<'static, ()> {
            Box::pin(tokio::time::sleep(duration))
        }

        fn now(&self) -> Duration {
            static ORIGIN: OnceLock<Instant> = OnceLock::new();
            ORIGIN.get_or_init(Instant::now).elapsed()
        }
    }

    impl NativeTransport {
        /// Opens an already-resolved WebSocket URL with default options.
        pub async fn connect(url: &str) -> Result<Self, TransportError> {
            Self::connect_resolved(
                ResolvedEndpoint {
                    url: url.to_owned(),
                    protocols: Vec::new(),
                },
                NativeTransportOptions::default(),
            )
            .await
        }

        async fn connect_resolved(
            endpoint: ResolvedEndpoint,
            options: NativeTransportOptions,
        ) -> Result<Self, TransportError> {
            let mut request = endpoint.url.into_client_request().map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
            })?;
            for (name, value) in &options.headers {
                let name = HeaderName::try_from(name).map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
                })?;
                let value = HeaderValue::try_from(value).map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
                })?;
                request.headers_mut().insert(name, value);
            }
            if !endpoint.protocols.is_empty() {
                let value =
                    HeaderValue::try_from(endpoint.protocols.join(", ")).map_err(|error| {
                        TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
                    })?;
                request.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, value);
            }
            let host = request
                .uri()
                .host()
                .ok_or_else(|| connect_error("WebSocket URL has no host"))?
                .to_owned();
            let port = request.uri().port_u16().unwrap_or_else(|| {
                if request.uri().scheme_str() == Some("wss") {
                    443
                } else {
                    80
                }
            });
            let stream = if let Some(proxy) = options
                .proxy
                .as_ref()
                .filter(|proxy| proxy.applies_to(&host))
            {
                connect_proxy(proxy, &host, port).await?
            } else {
                TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(|error| connect_error(error.to_string()))?
            };
            if options.disable_nagle {
                stream
                    .set_nodelay(true)
                    .map_err(|error| connect_error(error.to_string()))?;
            }
            let websocket_config = WebSocketConfig::default()
                .max_message_size(options.max_message_size)
                .max_frame_size(options.max_frame_size);
            let tls = options.tls_config.map(TlsConnector::Rustls);
            let (inner, _) =
                client_async_tls_with_config(request, stream, Some(websocket_config), tls)
                    .await
                    .map_err(|error| {
                        TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
                    })?;
            Ok(Self { inner })
        }
    }

    fn connect_error(message: impl Into<String>) -> TransportError {
        TransportError::with_kind(TransportErrorKind::Connect, message)
    }

    async fn connect_proxy(
        proxy: &ProxyConfig,
        target_host: &str,
        target_port: u16,
    ) -> Result<TcpStream, TransportError> {
        let url = Url::parse(&proxy.url).map_err(|error| connect_error(error.to_string()))?;
        if url.scheme() != "http" {
            return Err(connect_error("only HTTP CONNECT proxies are supported"));
        }
        let host = url
            .host_str()
            .ok_or_else(|| connect_error("proxy URL has no host"))?;
        let port = url.port_or_known_default().unwrap_or(80);
        let mut stream = TcpStream::connect((host, port))
            .await
            .map_err(|error| connect_error(error.to_string()))?;
        let authority = if target_host.contains(':') {
            format!("[{target_host}]:{target_port}")
        } else {
            format!("{target_host}:{target_port}")
        };
        let authorization = if url.username().is_empty() {
            String::new()
        } else {
            let credentials = format!("{}:{}", url.username(), url.password().unwrap_or_default());
            format!(
                "Proxy-Authorization: Basic {}\r\n",
                BASE64.encode(credentials)
            )
        };
        let request =
            format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n{authorization}\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|error| connect_error(error.to_string()))?;
        let mut response = Vec::with_capacity(1024);
        let mut byte = [0_u8; 1];
        while response.len() < 8192 && !response.ends_with(b"\r\n\r\n") {
            let count = stream
                .read(&mut byte)
                .await
                .map_err(|error| connect_error(error.to_string()))?;
            if count == 0 {
                return Err(connect_error("proxy closed before completing CONNECT"));
            }
            response.push(byte[0]);
        }
        if !response.ends_with(b"\r\n\r\n") {
            return Err(connect_error(
                "proxy CONNECT response headers are too large",
            ));
        }
        let status = String::from_utf8_lossy(&response);
        let accepted = status
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .is_some_and(|code| code.starts_with('2'));
        if !accepted {
            return Err(connect_error(format!(
                "proxy CONNECT failed: {}",
                status.lines().next().unwrap_or("invalid response")
            )));
        }
        Ok(stream)
    }

    impl Transport for NativeTransport {
        fn send<'a>(
            &'a mut self,
            message: WireMessage,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                let message = match message {
                    WireMessage::Text(text) => Message::Text(text.into()),
                    WireMessage::Binary(bytes) => Message::Binary(bytes.into()),
                };
                self.inner.send(message).await.map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Send, error.to_string())
                })
            })
        }

        fn receive<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
            Box::pin(async move {
                loop {
                    let Some(message) = self.inner.next().await else {
                        return Ok(TransportEvent::Closed(TransportClose::connection_ended()));
                    };
                    let message = message.map_err(|error| {
                        TransportError::with_kind(TransportErrorKind::Receive, error.to_string())
                    })?;
                    match message {
                        Message::Text(text) => {
                            return Ok(TransportEvent::Message(WireMessage::Text(
                                text.to_string(),
                            )));
                        }
                        Message::Binary(bytes) => {
                            return Ok(TransportEvent::Message(WireMessage::Binary(
                                bytes.to_vec(),
                            )));
                        }
                        Message::Close(frame) => {
                            return Ok(TransportEvent::Closed(match frame {
                                Some(frame) => {
                                    let code = u16::from(frame.code);
                                    TransportClose::new(
                                        Some(code),
                                        frame.reason.to_string(),
                                        code == 1000,
                                    )
                                }
                                None => TransportClose::connection_ended(),
                            }));
                        }
                        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                    }
                }
            })
        }

        fn close<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                SinkExt::close(&mut self.inner).await.map_err(|error| {
                    TransportError::with_kind(TransportErrorKind::Close, error.to_string())
                })
            })
        }

        fn close_with<'a>(
            &'a mut self,
            request: TransportCloseRequest,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                self.inner
                    .send(Message::Close(Some(CloseFrame {
                        code: CloseCode::from(request.code),
                        reason: request.reason.into(),
                    })))
                    .await
                    .map_err(|error| {
                        TransportError::with_kind(TransportErrorKind::Close, error.to_string())
                    })
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use tokio::net::TcpListener;

        #[test]
        fn proxy_bypass_matches_hosts_and_subdomains() {
            let proxy = ProxyConfig::new("http://localhost:8080").bypass_host("example.com");
            assert!(!proxy.applies_to("example.com"));
            assert!(!proxy.applies_to("api.example.com"));
            assert!(proxy.applies_to("notexample.com"));
        }

        #[tokio::test(flavor = "current_thread")]
        async fn establishes_authenticated_http_connect_tunnels() {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut byte = [0_u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                }
                stream
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .unwrap();
                String::from_utf8(request).unwrap()
            });
            let proxy = ProxyConfig::new(format!("http://user:pass@{address}"));
            let _stream = connect_proxy(&proxy, "example.com", 443).await.unwrap();
            let request = server.await.unwrap();
            assert!(request.starts_with("CONNECT example.com:443 HTTP/1.1\r\n"));
            assert!(request.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod managed;

#[cfg(not(target_arch = "wasm32"))]
pub use native::{
    NativeConnector, NativeTimer, NativeTransport, NativeTransportOptions, ProxyConfig,
};

#[cfg(not(target_arch = "wasm32"))]
pub use managed::*;

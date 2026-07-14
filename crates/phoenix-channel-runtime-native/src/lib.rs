//! Native Tokio WebSocket transport.

#![forbid(unsafe_code)]

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use phoenix_channel_client::{ConnectContext, Connector, Endpoint, ResolvedEndpoint, Timer};
    use phoenix_channel_runtime::{
        Transport, TransportClose, TransportError, TransportErrorKind, TransportEvent, WireMessage,
    };
    use tokio::net::TcpStream;
    use tokio_tungstenite::{
        MaybeTlsStream, WebSocketStream, connect_async,
        tungstenite::{
            Message,
            client::IntoClientRequest,
            http::{HeaderName, HeaderValue, header::SEC_WEBSOCKET_PROTOCOL},
        },
    };

    pub struct NativeTransport {
        inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
    }

    #[derive(Clone)]
    pub struct NativeConnector {
        endpoint: NativeEndpoint,
        headers: Vec<(String, String)>,
    }

    #[derive(Clone)]
    enum NativeEndpoint {
        Url(String),
        Phoenix(Endpoint),
    }

    impl NativeConnector {
        pub fn new(url: impl Into<String>) -> Self {
            Self {
                endpoint: NativeEndpoint::Url(url.into()),
                headers: Vec::new(),
            }
        }

        pub fn from_endpoint(endpoint: Endpoint) -> Self {
            Self {
                endpoint: NativeEndpoint::Phoenix(endpoint),
                headers: Vec::new(),
            }
        }

        pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
            self.headers.push((name.into(), value.into()));
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
            let headers = self.headers.clone();
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
                let transport = NativeTransport::connect_resolved(endpoint, headers).await?;
                Ok(Box::new(transport) as Box<dyn Transport>)
            })
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    pub struct NativeTimer;

    impl Timer for NativeTimer {
        fn sleep(&self, duration: Duration) -> futures::future::LocalBoxFuture<'static, ()> {
            Box::pin(tokio::time::sleep(duration))
        }
    }

    impl NativeTransport {
        pub async fn connect(url: &str) -> Result<Self, TransportError> {
            Self::connect_resolved(
                ResolvedEndpoint {
                    url: url.to_owned(),
                    protocols: Vec::new(),
                },
                Vec::new(),
            )
            .await
        }

        async fn connect_resolved(
            endpoint: ResolvedEndpoint,
            headers: Vec<(String, String)>,
        ) -> Result<Self, TransportError> {
            let mut request = endpoint.url.into_client_request().map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
            })?;
            for (name, value) in headers {
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
            let (inner, _) = connect_async(request).await.map_err(|error| {
                TransportError::with_kind(TransportErrorKind::Connect, error.to_string())
            })?;
            Ok(Self { inner })
        }
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
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::{NativeConnector, NativeTimer, NativeTransport};

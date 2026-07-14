//! Native Tokio WebSocket transport.

#![forbid(unsafe_code)]

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use phoenix_channel_client::{Connector, Timer};
    use phoenix_channel_runtime::{Transport, TransportError, WireMessage};
    use tokio::net::TcpStream;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

    pub struct NativeTransport {
        inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
    }

    #[derive(Clone, Debug)]
    pub struct NativeConnector {
        url: String,
    }

    impl NativeConnector {
        pub fn new(url: impl Into<String>) -> Self {
            Self { url: url.into() }
        }
    }

    impl Connector for NativeConnector {
        fn connect(
            &self,
        ) -> futures::future::LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>
        {
            let url = self.url.clone();
            Box::pin(async move {
                let transport = NativeTransport::connect(&url).await?;
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
            let (inner, _) = connect_async(url)
                .await
                .map_err(|error| TransportError::new(error.to_string()))?;
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
                self.inner
                    .send(message)
                    .await
                    .map_err(|error| TransportError::new(error.to_string()))
            })
        }

        fn receive<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<Option<WireMessage>, TransportError>>
        {
            Box::pin(async move {
                loop {
                    let Some(message) = self.inner.next().await else {
                        return Ok(None);
                    };
                    let message =
                        message.map_err(|error| TransportError::new(error.to_string()))?;
                    match message {
                        Message::Text(text) => {
                            return Ok(Some(WireMessage::Text(text.to_string())));
                        }
                        Message::Binary(bytes) => {
                            return Ok(Some(WireMessage::Binary(bytes.to_vec())));
                        }
                        Message::Close(_) => return Ok(None),
                        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                    }
                }
            })
        }

        fn close<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                SinkExt::close(&mut self.inner)
                    .await
                    .map_err(|error| TransportError::new(error.to_string()))
            })
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::{NativeConnector, NativeTimer, NativeTransport};

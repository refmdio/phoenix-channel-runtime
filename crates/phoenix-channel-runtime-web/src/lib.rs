//! Browser WebSocket transport for `wasm32-unknown-unknown`.

#![forbid(unsafe_code)]

#[cfg(target_arch = "wasm32")]
mod web {
    use futures::{SinkExt, StreamExt};
    use gloo_net::websocket::{Message, futures::WebSocket};
    use phoenix_channel_runtime::{Transport, TransportError, WireMessage};

    pub struct WebTransport {
        inner: WebSocket,
    }

    impl WebTransport {
        pub fn connect(url: &str) -> Result<Self, TransportError> {
            let inner =
                WebSocket::open(url).map_err(|error| TransportError::new(format!("{error:?}")))?;
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
                self.inner
                    .send(message)
                    .await
                    .map_err(|error| TransportError::new(format!("{error:?}")))
            })
        }

        fn receive<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<Option<WireMessage>, TransportError>>
        {
            Box::pin(async move {
                let Some(message) = self.inner.next().await else {
                    return Ok(None);
                };
                let message = message.map_err(|error| TransportError::new(format!("{error:?}")))?;
                Ok(Some(match message {
                    Message::Text(text) => WireMessage::Text(text),
                    Message::Bytes(bytes) => WireMessage::Binary(bytes),
                }))
            })
        }

        fn close<'a>(
            &'a mut self,
        ) -> futures::future::LocalBoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                SinkExt::close(&mut self.inner)
                    .await
                    .map_err(|error| TransportError::new(format!("{error:?}")))
            })
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use web::WebTransport;

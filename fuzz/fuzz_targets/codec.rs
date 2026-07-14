#![no_main]

use libfuzzer_sys::fuzz_target;
use phoenix_channel_runtime::{Codec, CodecLimits, PhoenixV2Codec, WireMessage};

fuzz_target!(|data: &[u8]| {
    let codec = PhoenixV2Codec::limited(CodecLimits {
        max_frame_bytes: 1024 * 1024,
        max_binary_payload_bytes: 1024 * 1024,
    });
    let _ = codec.decode(WireMessage::Binary(data.to_vec()));
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = codec.decode(WireMessage::Text(text.to_owned()));
    }
});

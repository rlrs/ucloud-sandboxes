//! Incremental UTF-8 decoding with replacement, as CPython's
//! `codecs.getincrementaldecoder("utf-8")(errors="replace")` does it.
//!
//! Both follow Unicode's "substitution of maximal subparts": each maximal
//! invalid subsequence becomes one U+FFFD (`Utf8Error::error_len` is that
//! length). A valid but incomplete sequence at the end of a chunk waits for the
//! next chunk, and becomes one U+FFFD at end of stream. CPython also holds
//! back a truncated surrogate (`ED A0..BF`), so this does too: the text is the
//! same either way, but its split across chunks (events) would differ.

#[derive(Debug, Default)]
pub struct Utf8Decoder {
    pending: Vec<u8>,
}

impl Utf8Decoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode one chunk; an incomplete trailing sequence is held back.
    pub fn decode(&mut self, input: &[u8]) -> String {
        self.run(input, false)
    }

    /// End of stream: whatever is held back is replaced.
    pub fn finish(&mut self) -> String {
        self.run(&[], true)
    }

    fn run(&mut self, input: &[u8], last: bool) -> String {
        let joined;
        let bytes: &[u8] = if self.pending.is_empty() {
            input
        } else {
            let mut buffer = std::mem::take(&mut self.pending);
            buffer.extend_from_slice(input);
            joined = buffer;
            &joined
        };
        let mut out = String::with_capacity(bytes.len());
        let mut rest = bytes;
        loop {
            match std::str::from_utf8(rest) {
                Ok(text) => {
                    out.push_str(text);
                    break;
                }
                Err(error) => {
                    let (valid, after) = rest.split_at(error.valid_up_to());
                    out.push_str(std::str::from_utf8(valid).expect("validated prefix"));
                    match error.error_len() {
                        // CPython buffers a truncated surrogate (`ED A0..BF`)
                        // at the end of a non-final chunk (for `surrogatepass`);
                        // it is replaced only once the next byte arrives.
                        Some(_) if !last && after.len() == 2 && after[0] == 0xED && after[1] >= 0xA0 => {
                            self.pending = after.to_vec();
                            break;
                        }
                        Some(length) => {
                            out.push('\u{FFFD}');
                            rest = &after[length..];
                        }
                        None if last => {
                            out.push('\u{FFFD}');
                            break;
                        }
                        None => {
                            self.pending = after.to_vec();
                            break;
                        }
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn whole(chunks: &[&[u8]]) -> String {
        let mut decoder = Utf8Decoder::new();
        let mut out = String::new();
        for chunk in chunks {
            out.push_str(&decoder.decode(chunk));
        }
        out + &decoder.finish()
    }

    #[test]
    fn split_code_points_survive() {
        let euro = "€".as_bytes();
        assert_eq!(whole(&[&euro[..1], &euro[1..2], &euro[2..]]), "€");
        assert_eq!(whole(&[b"a\xf0\x9f", b"\x98\x80b"]), "a😀b");
    }

    #[test]
    fn invalid_bytes_are_replaced_by_maximal_subpart() {
        assert_eq!(whole(&[b"\xff"]), "\u{FFFD}");
        // A truncated sequence followed by an ASCII byte is one replacement.
        assert_eq!(whole(&[b"\xe2\x82", b"A"]), "\u{FFFD}A");
        // Surrogates are three replacements.
        assert_eq!(whole(&[b"\xed\xa0\x80"]), "\u{FFFD}\u{FFFD}\u{FFFD}");
        // Truncated at end of stream: one replacement.
        assert_eq!(whole(&[b"x\xf0\x9f\x98"]), "x\u{FFFD}");
    }
}

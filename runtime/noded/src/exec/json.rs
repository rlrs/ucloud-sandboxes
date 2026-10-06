//! Python's `json.dumps(payload, separators=(",", ":"))` (http_server.py
//! `_write_json`): insertion order, ASCII escapes, float `repr`.

use serde_json::Value;

use crate::pyjson::number_repr;

pub fn dumps(value: &Value) -> String {
    let mut out = String::new();
    write(value, &mut out);
    out
}

fn write(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => out.push_str(&number_repr(number)),
        Value::String(text) => write_string(text, out),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (index, (key, item)) in map.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_string(key, out);
                out.push(':');
                write(item, out);
            }
            out.push('}');
        }
    }
}

fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if !(' '..='~').contains(&c) => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// The length `write_string` produces for `text`, without building it.
pub fn encoded_str_len(text: &str) -> usize {
    text.chars()
        .map(|c| match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0c}' => 2,
            ' '..='~' => 1,
            c if (c as u32) > 0xffff => 12,
            _ => 6,
        })
        .sum::<usize>()
        + 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keeps_order_and_escapes_ascii() {
        let value = json!({"z": 1, "a": [true, null, 0.5], "s": "é\u{7f}😀\"\n"});
        let text = dumps(&value);
        let expected = format!("{{\"z\":1,\"a\":[true,null,0.5],\"s\":\"{0}u00e9{0}u007f{0}ud83d{0}ude00{0}\"{0}n\"}}", "\\");
        assert_eq!(text, expected);
        assert_eq!(encoded_str_len("é\u{7f}😀\"\n"), text.len() - r#"{"z":1,"a":[true,null,0.5],"s":}"#.len());
    }
}

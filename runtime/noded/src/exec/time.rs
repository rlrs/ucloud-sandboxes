//! `datetime.now(timezone.utc).isoformat()`.

use std::time::{SystemTime, UNIX_EPOCH};

/// `2026-10-06T12:00:01.000250+00:00`; the fraction is omitted when the
/// microseconds are zero, and time is floored to the microsecond, as CPython does.
pub fn isoformat(at: SystemTime) -> String {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = since.as_secs() as i64;
    let micros = since.subsec_micros();
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let of_day = seconds.rem_euclid(86_400);
    let (hour, minute, second) = (of_day / 3600, of_day % 3600 / 60, of_day % 60);
    let fraction = if micros == 0 { String::new() } else { format!(".{micros:06}") };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{fraction}+00:00")
}

/// Days since 1970-01-01 to a proleptic Gregorian date (H. Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn formats_like_python() {
        let at = UNIX_EPOCH + Duration::new(1_791_288_001, 250_999);
        assert_eq!(isoformat(at), "2026-10-06T12:00:01.000250+00:00");
        assert_eq!(isoformat(UNIX_EPOCH + Duration::from_secs(951_782_400)), "2000-02-29T00:00:00+00:00");
        assert_eq!(isoformat(UNIX_EPOCH), "1970-01-01T00:00:00+00:00");
    }
}

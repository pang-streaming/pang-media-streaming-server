pub trait LogError<T> {
    fn log_error(self, error_text: &str) -> Option<T>;
}

impl<T, E: std::fmt::Display> LogError<T> for Result<T, E> {
    fn log_error(self, error_text: &str) -> Option<T> {
        match self {
            Ok(val) => Some(val),
            Err(e) => {
                eprintln!("{}: {}", error_text, e);
                None
            }
        }
    }
}
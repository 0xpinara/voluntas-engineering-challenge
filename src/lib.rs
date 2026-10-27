pub mod book;
pub mod collector;
pub mod conn;
pub mod output;
pub mod util;
pub mod venue;
pub mod verify;

mod side;
pub use side::{InvalidSide, Side};

/// A timestamped line on stderr; stdout carries the binary feed.
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        eprintln!("{} {}", chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"), format_args!($($arg)*))
    };
}

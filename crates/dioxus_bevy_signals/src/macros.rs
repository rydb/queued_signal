#[cfg(feature = "tracing")]
use bevy_log::tracing;



#[cfg(not(feature = "tracing"))]
use macro_v::macro_v;

#[cfg(feature = "tracing")]
pub use tracing::{debug, error, info, trace, warn};

#[cfg(not(feature = "tracing"))]
#[macro_v(pub(crate))]
macro_rules! debug {
    ($($arg:tt)*) => {()};
}

#[cfg(not(feature = "tracing"))]
#[macro_v(pub(crate))]
macro_rules! error {
    ($($arg:tt)*) => {()};
}

#[cfg(not(feature = "tracing"))]
#[macro_v(pub(crate))]
macro_rules! info {
    ($($arg:tt)*) => {()};
}

#[cfg(not(feature = "tracing"))]
#[macro_v(pub(crate))]
macro_rules! trace {
    ($($arg:tt)*) => {()};
}

#[cfg(not(feature = "tracing"))]
#[macro_v(pub(crate))]
macro_rules! warn {
    ($($arg:tt)*) => {()};
}


#[cfg(not(feature = "tracing"))]
use macro_v::macro_v;

#[cfg(feature = "tracing")]
#[allow(unused_imports)]
pub use tracing::{debug, error, info, trace, warn};

#[cfg(not(feature = "tracing"))]
#[macro_v(pub(crate))]
#[allow(unused_imports)]
macro_rules! warn {
    ($($arg:tt)*) => {
        ()
    };
}

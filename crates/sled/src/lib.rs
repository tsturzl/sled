//! `sled` is a flash-sympathetic persistent lock-free B+ tree.
//!
//! # Examples
//!
//! ```
//! let config = sled::ConfigBuilder::new().temporary(true).build();
//!
//! let t = sled::Tree::start(config).unwrap();
//!
//! t.set(b"yo!".to_vec(), b"v1".to_vec());
//! assert_eq!(t.get(b"yo!"), Ok(Some(b"v1".to_vec())));
//!
//! t.cas(
//!     b"yo!".to_vec(),       // key
//!     Some(b"v1".to_vec()),  // old value, None for not present
//!     Some(b"v2".to_vec()),  // new value, None for delete
//! ).unwrap();
//!
//! let mut iter = t.scan(b"a non-present key before yo!");
//! assert_eq!(iter.next(), Some(Ok((b"yo!".to_vec(), b"v2".to_vec()))));
//! assert_eq!(iter.next(), None);
//!
//! t.del(b"yo!");
//! assert_eq!(t.get(b"yo!"), Ok(None));
//! ```

#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
#![cfg_attr(feature="clippy", allow(inline_always))]

extern crate pagecache;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate crossbeam_epoch as epoch;
extern crate bincode;
#[macro_use]
extern crate log as _log;

/// atomic lock-free tree
pub use tree::{Iter, Tree};

use pagecache::*;

pub use pagecache::{CacheResult as DbResult, Config, ConfigBuilder, Error};

mod tree;

type Key = Vec<u8>;
type KeyRef<'a> = &'a [u8];

const TX_PREFIX: &'static [u8] = b"\x00\x00tx@";
const TX_PENDING: u8 = 0;
const TX_ABORTED: u8 = 1;
const TX_COMMITTED: u8 = 2;

// type Value = Vec<u8>;

#[derive(Clone, Ord, PartialOrd, Eq, Debug, PartialEq, Serialize, Deserialize)]
enum Value {
    Present(Vec<u8>),
    Pending {
        old: Option<Vec<u8>>,
        pending: Option<Vec<u8>>,
        tx_key: Vec<u8>,
    },
}

enum RollbackResponse {
    UsePending,
    UseOld,
}

type TreePtr<'g> = pagecache::PagePtr<'g, tree::Frag>;

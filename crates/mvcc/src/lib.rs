//  txn algo:
//      read
//          get ts + id for each write with single fetch_add
//          version search
//              early abort if r.wts > t.ts
//      validation
//          if recent aborts
//              sort write set by contention
//              precheck version consistency
//          install pending versions
//              create tx pointing to writeset
//              add delta to index
//          update read ts
//          check version consistency
//      write phase
//          persist
//              for (Wts, Version) in !ts:
//                  @k <- (Wts, Version)
//              delete !ts from sled
//          commit in mem chain
//      maintenance
//          schedule gc
//              put (@k, wts, version) for last good version into epoch dropper
//
//      recovery algo:
//          bump stored ts by TS_SAFETY_BUFFER
//          for (ts, writeset) in ts range:
//              for Wts, Version in writeset:
//                  filter @k, remove (Wts, Version)
//              delete ts from sled
//
//      phantom handling
//          inserts
//              install pending has old version point to None
//          deletes
//              install pending has new version point to None
extern crate sled;
extern crate crossbeam_epoch as epoch;
extern crate serde;
extern crate bincode;

#[cfg(test)]
extern crate rand;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{Infinite, deserialize, serialize};

mod tx;
mod db;
mod mvcc;

pub use tx::{Error, Tx, TxResult};
pub use db::Db;
use mvcc::{Chain, MemRecord, Mvcc, Status};

type Version = u64;
type Ts = u64;
type Key = Vec<u8>;
type Value = Vec<u8>;

// a pending ptr (prefixed by !) points to keys in-flight
type WriteSet = Vec<Key>;

// a key (prefixed by @) points to versions
type Versions = Vec<(Ts, Version)>;

#[derive(Debug, PartialEq)]
pub enum TxRet {
    Committed,
    Conflict,
    PredicateFailure,
}

fn bytes_to_ts(bytes: &[u8]) -> Ts {
    let mut ts_arr = [0; 8];
    ts_arr.copy_from_slice(&bytes[0..8]);
    unsafe { std::mem::transmute(ts_arr) }
}

fn ts_to_bytes(ts: Ts) -> Vec<u8> {
    let bytes: [u8; 8] = unsafe { std::mem::transmute(ts) };
    bytes.to_vec()
}

fn key_safety_pad(key: &Key) -> Key {
    let mut new = Vec::with_capacity(key.len() + 1);
    unsafe {
        new.set_len(key.len() + 1);
    }
    new[0] = b'@';
    (new[1..1 + key.len()]).copy_from_slice(&*key);
    new
}

#[test]
fn it_works() {
    let conf = sled::ConfigBuilder::new().temporary(true).build();
    let db = Db::start(conf).unwrap();

    let mut tx = db.tx();
    tx.set(b"cats".to_vec(), b"meow".to_vec());
    tx.set(b"dogs".to_vec(), b"woof".to_vec());
    assert_eq!(tx.execute(), Ok(()));

    let mut tx = db.tx();
    tx.predicate(b"cats".to_vec(), |_k, v| *v == Some(b"meow".to_vec()));
    tx.predicate(b"dogs".to_vec(), |_k, v| *v == Some(b"woof".to_vec()));
    tx.set(b"cats".to_vec(), b"woof".to_vec());
    tx.set(b"dogs".to_vec(), b"meow".to_vec());
    //tx.get(b"dogs".to_vec());
    assert_eq!(tx.execute(), Ok(()));

    let mut tx = db.tx();
    tx.predicate(b"cats".to_vec(), |_k, v| *v == Some(b"meow".to_vec()));
    assert_eq!(tx.execute(), Err(Error::PredicateFailure));
}

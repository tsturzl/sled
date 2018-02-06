//! An example of how to build multi-key transactions on top of
//! sled's single-key atomicity guarantees.

extern crate sled;

use sled::{Config, DbResult, Error, Tree};
use std::ops::Deref;

const TX_PENDING: u8 = 0;
const TX_COMMITTED: u8 = 1;
const TX_ABORTED: u8 = 2;
const TX_PREFIX: &'static [u8] = b"__tx_";
const TX_MAX: &'static [u8] = b"__tx_max";

type TxId = u64;
type Key = Vec<u8>;
type Value = Vec<u8>;
type PredicateFn = Box<Fn(Key, Value) -> bool>;

enum Isolation {
    Snapshot,
    Serializable,
}

enum TxStatus {
    Committed,
    Conflict,
    PredicateFailure,
}

struct TxDb {
    isolation: Isolation,
    db: Tree,
}

unsafe impl Send for TxDb {}
unsafe impl Sync for TxDb {}

struct Tx<'a> {
    predicates: Vec<Predicate>,
    writes: Vec<Write>,
    reads: Vec<Read>,
    deletes: Vec<Delete>,
    db: &'a TxDb,
}

impl Deref for TxDb {
    type Target = Tree;

    fn deref(&self) -> &Tree {
        &self.db
    }
}

struct Predicate(Key, PredicateFn);
struct Write(Key, Value);
struct Read(Key);
struct Delete(Key);

fn txid_to_v(txid: TxId) -> Vec<u8> {
    let v: [u8; 8] = unsafe { std::mem::transmute(txid) };
    v.to_vec()
}

fn split_txid_from_v(v: Value) -> DbResult<(TxId, Value), ()> {
    if v.len() < 8 {
        return Err(Error::ReportableBug("read bad txid".to_owned()));
    }
    let mut first_8 = [0; 8];
    first_8.copy_from_slice(&*v);
    let txid = unsafe { std::mem::transmute(first_8) };
    Ok((txid, v.into_iter().skip(8).collect()))
}

impl TxDb {
    fn start(config: Config) -> DbResult<TxDb, ()> {
        Ok(TxDb {
            // pluggable
            isolation: Isolation::Serializable,
            db: Tree::start(config)?,
        })
    }

    fn tx<'a>(&'a self) -> Tx<'a> {
        Tx {
            predicates: vec![],
            reads: vec![],
            writes: vec![],
            deletes: vec![],
            db: &self,
        }
    }

    fn new_txid(&self) -> DbResult<TxId, ()> {
        // try to read last
        let mut max_v: Option<Value> = self.db.get(TX_MAX)?;

        loop {
            let max_or_default_v = max_v.clone().unwrap_or_else(|| vec![0; 8]);

            let (max_tx, rest) = split_txid_from_v(max_or_default_v)?;
            assert!(rest.is_empty());

            // bump by 1
            let new = max_tx + 1;
            let new_v = txid_to_v(new);

            // CAS old to new
            let res = self.db.cas(TX_MAX.to_owned(), max_v, Some(new_v));
            match res {
                Ok(()) => return Ok(new),
                Err(Error::CasFailed(Some(v))) => max_v = Some(v),
                _ => return res.map(|_| 0).map_err(|e| e.danger_cast()),
            }
        }

    }
}

impl<'a> Tx<'a> {
    fn predicate(&mut self, k: Key, p: PredicateFn) {
        self.predicates.push(Predicate(k, p));
    }

    fn read(&mut self, k: Key) {
        self.reads.push(Read(k));
    }

    fn write(&mut self, k: Key, v: Value) {
        self.writes.push(Write(k, v));
    }

    fn del(&mut self, k: Key) {
        self.deletes.push(Delete(k));
    }

    fn execute(self) -> DbResult<TxStatus, ()> {
        // read values
        let mut read_reads = vec![];
        let mut predicate_reads = vec![];
        let mut write_reads = vec![];
        let mut delete_reads = vec![];

        for Read(k) in self.reads {
            read_reads.push(self.db.get(&*k)?);
        }

        for Predicate(k, _p) in self.predicates {
            predicate_reads.push(self.db.get(&*k)?);
        }

        for Write(k, _v) in self.writes {
            write_reads.push(self.db.get(&*k)?);
        }

        for Delete(k) in self.deletes {
            delete_reads.push(self.db.get(&*k)?);
        }

        // create tx
        let txid = self.db.new_txid()?;
        println!("generated new txid {}", txid);

        // CAS values to refer to tx

        // CAS tx from Pending -> Committed

        // clean up

        Ok(TxStatus::Committed)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

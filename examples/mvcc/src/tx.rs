use sled::DbResult;

use super::*;

pub(super) type PredicateFn = fn(&Key, &Option<Value>) -> bool;
pub(super) struct Predicate(Key, Box<PredicateFn>);
pub(super) struct Write(Key, Value);
pub(super) struct Read(Key);
pub(super) struct Delete(Key);

pub struct Tx<'a> {
    pub(super) db: &'a Db,
    pub(super) predicates: Vec<Predicate>,
    pub(super) sets: Vec<Write>,
    pub(super) gets: Vec<Read>,
    pub(super) deletes: Vec<Delete>,
}

impl<'a> Tx<'a> {
    pub fn predicate(&mut self, k: Key, p: PredicateFn) {
        self.predicates.push(Predicate(k, Box::new(p)));
    }

    pub fn get(&mut self, k: Key) {
        self.gets.push(Read(k));
    }

    pub fn set(&mut self, k: Key, v: Value) {
        self.sets.push(Write(k, v));
    }

    pub fn del(&mut self, k: Key) {
        self.deletes.push(Delete(k));
    }

    pub fn execute(self) -> DbResult<TxRet, ()> {
        let base_ts = self.db.ts(1 + self.sets.len() + self.deletes.len());

        // claim locks
        let mut get_keys = vec![];
        let mut set_keys = vec![];

        for &Predicate(ref k, _) in &self.predicates {
            get_keys.push(k);
        }

        for &Read(ref k) in &self.gets {
            get_keys.push(k);
        }

        for &Write(ref k, _) in &self.sets {
            set_keys.push(k);
        }

        for &Delete(ref k) in &self.deletes {
            set_keys.push(k);
        }

        /*
        let mut set_hashes: Vec<u16> =
            set_keys.into_iter().map(|k| crc16::crc16(k)).collect();
        set_hashes.sort();
        set_hashes.dedup();

        let mut get_hashes: Vec<u16> = get_keys
            .into_iter()
            .map(|k| crc16::crc16(k))
            .filter(|hash| !set_hashes.contains(hash))
            .collect();
        get_hashes.sort();
        get_hashes.dedup();

        // perform predicate matches
        for &Predicate(ref k, ref p) in &self.predicates {
            let current = self.db.tree.get(k)?;
            if !p(&k, &current) {
                return Ok(TxRet::PredicateFailure);
            }
        }

        // perform sets
        for &Write(ref k, ref v) in &self.sets {
            self.db.tree.set(k.clone(), v.clone())?;
        }

        // perform deletes
        for &Delete(ref k) in &self.deletes {
            self.db.tree.del(k)?;
        }

        // perform gets
        let mut ret_gets = vec![];
        for &Read(ref k) in &self.gets {
            ret_gets.push((k.clone(), self.db.tree.get(k)?));
        }
        */

        Ok(TxRet::Committed(vec![]))
    }
}

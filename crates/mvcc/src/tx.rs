use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::sync::Arc;

use sled::Error as DbError;
use epoch::{Guard, pin};

use super::*;

pub(super) type PredicateFn = fn(&Key, &Option<Value>) -> bool;
pub(super) struct Predicate(Key, Box<PredicateFn>);
pub(super) struct Read(Key);
pub(super) struct Write(Key, Option<Value>);

pub type TxResult<T> = Result<T, Error>;

#[derive(Debug, PartialEq)]
pub enum Error {
    Abort,
    PredicateFailure,
    Db(DbError<()>),
    #[test]
    Blocked,
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::Abort => write!(f, "Transaction aborted"),
            Error::PredicateFailure => {
                write!(f, "Transaction predicate failed")
            }
            Error::Db(ref dbe) => write!(f, "Underlying DB error: {}", dbe),
            #[test]
            Error::Blocked => {
                write!(f, "Transaction currently blocked on another")
            }
        }
    }
}

impl<T> From<DbError<T>> for Error {
    #[inline]
    fn from(db_error: DbError<T>) -> Error {
        Error::Db(db_error.danger_cast())
    }
}

struct VersionedChain {
    initial_visible: Ts,
    chain: Arc<Chain>,
}

pub struct Tx<'a> {
    pub(super) db: &'a Db,
    pub(super) reads: Vec<Read>,
    pub(super) predicates: Vec<Predicate>,
    pub(super) sets: Vec<Write>,
    base_ts: Ts,
    chains: HashMap<Key, VersionedChain>,
    epoch: Guard,
}

impl<'a> Tx<'a> {
    pub(super) fn new(db: &'a Db) -> Tx<'a> {
        Tx {
            db: db,
            reads: vec![],
            predicates: vec![],
            sets: vec![],
            base_ts: 0,
            chains: HashMap::new(),
            epoch: pin(),
        }
    }

    pub fn set(&mut self, k: Key, v: Value) {
        self.sets.push(Write(k, Some(v)));
    }

    pub fn del(&mut self, k: Key) {
        self.sets.push(Write(k, None));
    }

    pub fn get(&mut self, k: Key) {
        self.reads.push(Read(k.clone()));
    }

    pub fn predicate(&mut self, k: Key, p: PredicateFn) {
        self.reads.push(Read(k.clone()));
        self.predicates.push(Predicate(k, Box::new(p)));
    }

    pub fn execute(mut self) -> TxResult<()> {
        let res = self._execute();

        // TODO propagate errors during rollback (maintenance(false))
        self.maintenance(res.is_ok());

        // self.db.debug_str();

        res
    }

    fn _execute(&mut self) -> TxResult<()> {
        self.set_ts();
        self.version_search()?;
        self.install_pending()?;
        self.update_read_ts()?;
        self.check_predicates()?;
        self.check_version_consistency()?;
        self.write()?;
        Ok(())
    }

    fn set_ts(&mut self) {
        // allocate timestamps for txn and versions
        self.base_ts = self.db.ts(self.sets.len());
        unsafe {
            self.epoch.defer(
                || self.db.bump_low_water_mark(self.base_ts),
            );
        }
    }

    fn version_search(&mut self) -> TxResult<()> {
        let mut keyset = HashSet::new();

        for &Predicate(ref k, _) in &self.predicates {
            keyset.insert(k.clone());
        }

        for &Write(ref k, _) in &self.sets {
            keyset.insert(k.clone());
        }

        for key in keyset.into_iter() {
            println!("version search for key {:?}", key);
            // pull in chains, block if pending && wts < t.ts
            let chain = self.db.get_chain(&key).unwrap();
            let last_ts = chain.visible_ts(self.base_ts)?;
            if last_ts > self.base_ts {
                // abort if any wts > t.ts
                println!(
                    "aborting because chain visible ts {} > our ts {}",
                    last_ts,
                    self.base_ts
                );
                return Err(Error::Abort);
            }
            self.chains.insert(
                key,
                VersionedChain {
                    initial_visible: last_ts,
                    chain: chain,
                },
            );
        }

        Ok(())
    }

    fn check_predicates(&mut self) -> TxResult<()> {
        // perform predicate matches

        for (i, &Predicate(ref k, ref predicate)) in
            self.predicates.iter().enumerate()
        {
            let versioned_chain = self.chains.get(k).unwrap();
            let visible_ts = versioned_chain.chain.visible_ts(self.base_ts)?;

            if versioned_chain.initial_visible != visible_ts &&
                self.base_ts != visible_ts
            {
                println!(
                    "aborting because our predicate version chain has \
                    advanced beyond where we initially read from: {} != {:?}",
                    versioned_chain.initial_visible,
                    versioned_chain.chain.visible_ts(self.base_ts)
                );
                return Err(Error::Abort);
            }

            let key = ts_to_bytes(versioned_chain.initial_visible + i as Ts);

            let current = self.db.tree.get(&*key)?;
            if !predicate(&k, &current) {
                return Err(Error::PredicateFailure);
            }
        }

        Ok(())
    }

    fn install_pending(&mut self) -> TxResult<()> {
        // install pending into chain
        for (i, &Write(ref k, _)) in self.sets.iter().enumerate() {
            let versioned_chain = self.chains.get(k).unwrap();
            let version = self.base_ts + i as Ts;

            let pending = MemRecord {
                rts: AtomicUsize::new(0),
                wts: self.base_ts,
                data: Some(version),
                status: Status::Pending,
            };

            versioned_chain.chain.install(
                versioned_chain.initial_visible,
                pending,
            )?;
        }

        Ok(())
    }

    fn update_read_ts(&mut self) -> TxResult<()> {
        for &Predicate(ref k, _) in &self.predicates {
            let versioned_chain = self.chains.get(k).unwrap();
            versioned_chain.chain.bump_rts(self.base_ts);
        }

        Ok(())
    }

    fn check_version_consistency(&mut self) -> TxResult<()> {
        // go back to all things we read and ensure the initial visible
        // version is still visible
        for &Predicate(ref k, _) in &self.predicates {
            let versioned_chain = self.chains.get(k).unwrap();
            let last_ts = versioned_chain.chain.visible_ts(self.base_ts)?;
            if last_ts != versioned_chain.initial_visible &&
                last_ts != self.base_ts
            {
                println!(
                    "aborting because a previously read item \
                has changed before our transaction could finish"
                );
                return Err(Error::Abort);
            }
        }

        Ok(())
    }

    fn write(&mut self) -> TxResult<()> {
        // put writeset into Tree
        let writeset: Vec<Key> =
            self.sets.iter().map(|p| p.0.clone()).collect();
        let writeset_bytes = serialize(&writeset, Infinite).unwrap();

        let mut writeset_k = vec![b'!' as u8; 9];
        writeset_k[1..9].copy_from_slice(&*ts_to_bytes(self.base_ts));

        self.db.tree.set(writeset_k.clone(), writeset_bytes)?;

        // put versions into Tree
        for (i, &Write(ref k, ref v)) in self.sets.iter().enumerate() {
            let version = self.base_ts + i as Ts;
            let key = ts_to_bytes(version);
            if let &Some(ref value) = v {
                self.db.tree.set(key, value.clone())?;
            } else {
                unimplemented!("deletes are not yet supported");
            }

            // cas @Key to refer to new writes for each write
            self.db.add_version_to_key(k, self.base_ts, version)?;
        }

        // NB remove writeset from disk, this is the linearizing point
        // of the entire transaction as far as recovery is concerned!
        self.db.tree.del(&writeset_k)?;

        Ok(())
    }

    fn maintenance(&mut self, success: bool) {
        // put (@k, wts, version) for last good version into epoch dropper

        for &Write(ref k, _) in &self.sets {
            let versioned_chain = self.chains.get(k).unwrap();
            if success {
                versioned_chain.chain.commit(self.base_ts);
            } else {
                versioned_chain.chain.abort(self.base_ts);
            }

            if !success {
                println!("cleaning up base ts {}", self.base_ts);
                for &Write(ref k, _) in &self.sets {
                    self.db
                        .purge_version_from_key(k, self.base_ts, success)
                        .unwrap();
                }
            }

            // remove the pending tx writeset to signal completeness
            let mut writeset_k = vec![b'!' as u8; 9];
            writeset_k[1..9].copy_from_slice(&*ts_to_bytes(self.base_ts));
            self.db.tree.del(&*writeset_k).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn cursor_stability() {}

    #[test]
    fn monotonic_view() {}

    #[test]
    fn monotonic_snapshot_reads() {}

    #[test]
    fn consistent_view() {}

    #[test]
    fn forward_consistent_view() {}

    #[test]
    fn repeatable_reads() {}

    #[test]
    fn snapshot_isolation() {}

    #[test]
    fn update_serializability() {}

    #[test]
    fn full_serializability() {}

    #[test]
    fn phen_write_cycles_g0() {}
    #[test]
    fn aborted_reads_g1a() {}
    #[test]
    fn phen_intermediate_reads_g1b() {}
    #[test]
    fn phen_circular_information_flow_g1c() {}
    #[test]
    fn phen_non_atomic_predicate_based_reads_g1_preda() {}
    #[test]
    fn phen_non_atomic_predicate_based_reads_wrt_transactions_g1_predb() {}
    #[test]
    fn phen_anti_dependency_cycles_g2() {}
    #[test]
    fn phen_item_anti_dependency_cycles_g2_item() {}
    #[test]
    fn phen_non_atomic_sql_statements_g_sql_atomic() {}
    #[test]
    fn phen_anti_dependency_cycles_at_runtime_e2() {}
    #[test]
    fn phen_single_anti_dependency_cycles_g_single() {}
    #[test]
    fn phen_monotonic_reads_g_monotonic() {}
    #[test]
    fn phen_interference_g_sia() {}
    #[test]
    fn phen_missed_effects_g_sib() {}
    #[test]
    fn phen_action_interference_g_msra() {}
    #[test]
    fn phen_action_missed_effects_g_msrb() {}
    #[test]
    fn phen_labeled_single_anti_dependency_cycles_g_cursor_x() {}
    #[test]
    fn phen_single_anti_dependency_cycles_with_update_transactions_g_update() {}
    #[test]
    fn phen_single_anti_dependency_cycles_at_runtime_e_single() {}
    #[test]
    fn phen_monotonic_reads_at_runtime_e_monotonic() {}

    #[test]
fn phen_single_anti_dependency_cycles_with_update_transactions_at_runtime_e_update(){
    }

    #[test]
    fn phen_dirty_write_p0() {}
    #[test]
    fn phen_dirty_read_p1() {}
    #[test]
    fn phen_cursor_lost_update() {}
    #[test]
    fn phen_lost_update_p4() {}
    #[test]
    fn phen_non_repeatable_read_p2() {}
    #[test]
    fn phen_phantom_p3() {}
    #[test]
    fn read_skew_a5a() {}

    #[test]
    fn write_skew_a5b() {
        // conflicting disjoint writes to dependent read values are not materialized
        let mut rng = rand::thread_rng();

        // try 100 random orderings
        for _ in 0..100 {
            // setup
            let db = db();
            let mut tx_setup = db.tx();
            tx_setup.set(vec![1], vec![0]);
            tx_setup.set(vec![2], vec![0]);
            tx_setup.execute().unwrap();

            let (mut tx1, mut tx2) = (db.tx(), db.tx());
            tx1.predicate(vec![1], |_k, v| *v == Some(vec![0]));
            tx1.predicate(vec![2], |_k, v| *v == Some(vec![0]));
            tx1.set(vec![1], vec![1]);
            tx2.predicate(vec![1], |_k, v| *v == Some(vec![0]));
            tx2.predicate(vec![2], |_k, v| *v == Some(vec![0]));
            tx2.set(vec![2], vec![2]);

            // execution
            let txs = &mut [TestExecutor::new(tx1), TestExecutor::new(tx2)];
            while !(txs[0].cleaned && txs[1].cleaned) {
                let choice = rng.gen_range(0, 2);
                txs[choice].step();
            }
            let res1 = txs[0].result();
            let res2 = txs[1].result();
            assert_ne!(res1.is_ok(), res2.is_ok());
        }
    }

    fn db() -> Db {
        let conf = sled::ConfigBuilder::new().temporary(true).build();
        Db::start(conf).unwrap()
    }

    struct TestExecutor<'a> {
        tx: Tx<'a>,
        result: Option<TxResult<()>>,
        cleaned: bool,
        step: usize,
    }

    impl<'a> TestExecutor<'a> {
        fn new(tx: Tx<'a>) -> TestExecutor<'a> {
            TestExecutor {
                tx: tx,
                result: None,
                cleaned: false,
                step: 0,
            }
        }

        fn step(&mut self) {
            if let Some(ref result) = self.result {
                if !self.cleaned {
                    self.tx.maintenance(result.is_ok());
                    self.cleaned = true;
                }
                return;
            }
            let res = match self.step {
                0 => {
                    self.tx.set_ts();
                    Ok(())
                }
                1 => self.tx.version_search(),
                2 => self.tx.install_pending(),
                3 => self.tx.update_read_ts(),
                4 => self.tx.check_predicates(),
                5 => self.tx.check_version_consistency(),
                6 => {
                    let res = self.tx.write();
                    self.result = Some(res);
                    return;
                }
                _ => panic!("trying to execute non-existant transaction step"),
            };
            if res == Err(Error::Blocked) {
                return;
            }
            self.step += 1;
            if res.is_err() {
                self.result = Some(res);
            }
        }

        fn result(&mut self) -> TxResult<()> {
            self.result.take().unwrap()
        }
    }
}

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::*;

#[derive(Debug, Default)]
pub struct Mvcc {
    // TODO replace with ART
    inner: RwLock<HashMap<Key, Arc<Chain>>>,
}

impl Mvcc {
    pub(super) fn get(&self, k: &Key) -> Option<Arc<Chain>> {
        let inner = self.inner.read().unwrap();
        inner.get(k).cloned()
    }

    pub(super) fn insert(&self, k: Key, chain: Chain) -> Result<(), ()> {
        let mut inner = self.inner.write().unwrap();
        if inner.contains_key(&k) {
            return Err(());
        }
        inner.insert(k, Arc::new(chain));
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Status {
    Pending,
    Aborted,
    Committed,
}

impl Default for Status {
    fn default() -> Status {
        Status::Committed
    }
}

#[derive(Debug, Default)]
pub struct MemRecord {
    pub wts: Ts,
    pub rts: AtomicUsize,
    pub data: Option<Version>,
    pub status: Status,
}

unsafe impl Send for MemRecord {}
unsafe impl Sync for MemRecord {}

// this is how we materialize updates in-memory
#[derive(Debug)]
pub struct Chain {
    records: RwLock<Vec<MemRecord>>,
}

impl Default for Chain {
    fn default() -> Chain {
        let records = vec![MemRecord::default()];
        Chain {
            records: RwLock::new(records),
        }
    }
}

impl Chain {
    pub fn new(records: Vec<MemRecord>) -> Chain {
        Chain {
            records: RwLock::new(records),
        }
    }

    pub fn commit(&self, ts: Ts) {
        let mut records = self.records.write().unwrap();
        let record = records.last_mut().unwrap();
        assert_eq!(record.wts, ts);
        record.status = Status::Committed;
    }

    pub fn abort(&self, ts: Ts) {
        let mut records = self.records.write().unwrap();
        let record = records.last_mut().unwrap();
        assert_eq!(record.wts, ts);
        record.status = Status::Aborted;
    }

    pub fn visible_ts(&self, ts: Ts) -> TxResult<Ts> {
        #[test]
        let mut i = 0;
        loop {
            #[test]
            {
                i += 1;
                if i > 1 {
                    return Err(Error::Blocked);
                }
            }
            let records = self.records.read().unwrap();
            for record in records.iter().rev() {
                if record.wts == ts {
                    return Ok(record.wts);
                }
                match record.status {
                    Status::Committed => return Ok(record.wts),
                    Status::Aborted => continue,
                    Status::Pending => break,
                }
            }
        }
    }

    pub fn bump_rts(&self, ts: Ts) {
        let records = self.records.read().unwrap();

        for record in records.iter().rev() {
            if record.wts < ts {
                let mut rts = record.rts.load(SeqCst) as Ts;
                loop {
                    if rts >= ts {
                        return;
                    }
                    let last = record.rts.compare_and_swap(
                        rts as usize,
                        ts as usize,
                        SeqCst,
                    );
                    if last as Ts == rts {
                        // we succeeded.
                        return;
                    }
                    rts = last as Ts;
                }
            }
        }
    }

    pub fn install(&self, last_ts: Ts, record: MemRecord) -> TxResult<()> {
        let mut records = self.records.write().unwrap();

        if let Some(last_record) = records.last() {
            if last_ts != last_record.wts {
                println!(
                    "early aborting because last ts {} != last_record.wts {}",
                    last_ts,
                    last_record.wts
                );
                return Err(Error::Abort);
            }
        }

        records.push(record);
        Ok(())
    }
}

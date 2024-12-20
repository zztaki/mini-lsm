use serde::{Deserialize, Serialize};

use anyhow::{Error, Result};

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    lsm_storage::LsmStorageState,
    table::{SsTableBuilder, SsTableIterator},
};

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // unimplemented!()
        let l0_sstables = _snapshot.l0_sstables.clone();
        let levels = _snapshot.levels.clone();
        assert!(!levels.is_empty());
        if l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!(
                "compaction triggered at level 0 with {} files",
                l0_sstables.len()
            );
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: l0_sstables,
                lower_level: 1,
                lower_level_sst_ids: levels[0].clone().1,
                is_lower_level_bottom_level: levels.len() == 1,
            });
        }

        for i in 1..levels.len() {
            let upper_level = levels[i - 1].clone().1;
            let lower_level = levels[i].clone().1;
            if !upper_level.is_empty()
                && (lower_level.len() as f64 / upper_level.len() as f64) * 100.0
                    < self.options.size_ratio_percent as f64
            {
                println!(
                    "compaction triggered at level {} and {} with size ratio {}",
                    i,
                    i + 1,
                    lower_level.len() as f64 / upper_level.len() as f64
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(i),
                    upper_level_sst_ids: upper_level,
                    lower_level: i + 1,
                    lower_level_sst_ids: lower_level,
                    is_lower_level_bottom_level: levels.len() == i + 1,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        // unimplemented!()
        let mut new_snapshot = _snapshot.clone();

        if _task.upper_level.is_none() {
            // L0 + L1 compaction -> L1
            new_snapshot
                .l0_sstables
                .retain(|x| !_task.upper_level_sst_ids.contains(x));
        } else {
            new_snapshot.levels[_task.upper_level.unwrap() - 1]
                .1
                .retain(|x| !_task.upper_level_sst_ids.contains(x));
        }

        new_snapshot.levels[_task.lower_level - 1].1 = _output.to_vec();

        let old_sst_ids = _task
            .upper_level_sst_ids
            .iter()
            .chain(_task.lower_level_sst_ids.iter())
            .cloned()
            .collect::<Vec<_>>();
        (new_snapshot, old_sst_ids)
    }
}

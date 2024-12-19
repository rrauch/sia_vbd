use crate::nbd::block_device::read_reply::Payload::Zeroes;
use crate::nbd::block_device::read_reply::{Payload, PayloadWriter};
use crate::nbd::transmission::fragment::FragmentError::Overlap;
use async_trait::async_trait;
use futures::AsyncWrite;
use rangemap::RangeMap;
use std::cmp::min;
use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Instant;
use thiserror::Error;

const MAX_MERGABLE_WRITER_PAYLOAD: u64 = 1024 * 256;

pub(in crate::nbd) struct Fragment {
    pub(in crate::nbd) offset: u64,
    pub(in crate::nbd) length: u64,
    range: Range<u64>,
    pub(in crate::nbd) payload: Payload,
}

impl Fragment {
    pub(crate) fn new(offset: u64, length: u64, payload: Payload) -> Self {
        assert!(length > 0);
        Self {
            range: offset..offset + length,
            offset,
            length,
            payload,
        }
    }

    fn can_merge(&self, other: &Self) -> bool {
        let (first, second) = if self.range.start <= other.range.start {
            (&self.range, &other.range)
        } else {
            (&other.range, &self.range)
        };

        let mut overlaps = false;
        if first.end == second.start {
            // ranges are adjacent
        } else if first.end > second.start {
            // ranges overlap
            overlaps = true;
        } else {
            // no relation
            return false;
        }

        match (&self.payload, &other.payload) {
            (Zeroes, Zeroes) => {
                // zeroes can be merged
                true
            }
            (Payload::Writer(_), Payload::Writer(_)) if !overlaps => {
                // writers can be merged if the combined payload length does not exceed the maximum
                self.length + other.length <= MAX_MERGABLE_WRITER_PAYLOAD
            }
            _ => false,
        }
    }

    fn merge(self, other: Self) -> Result<Self, FragmentError> {
        if !&self.can_merge(&other) {
            return Err(FragmentError::MergeFailure);
        }

        let (first, second) = if self.range.start <= other.range.start {
            (self, other)
        } else {
            (other, self)
        };

        let offset = first.offset;
        let length = first.length + second.length;

        let payload = match (first.payload, second.payload) {
            (Zeroes, Zeroes) => Zeroes,
            (Payload::Writer(first), Payload::Writer(second)) => {
                ChainedPayloadWriter { first, second }.into()
            }
            _ => unreachable!("merging incompatible fragment"),
        };

        Ok(Fragment::new(offset, length, payload))
    }
}

pub(in crate::nbd) struct FragmentProcessor {
    strict: bool,
    processed: RangeMap<u64, ProcessedState>,
    buffer: BTreeMap<u64, (Fragment, Instant)>,
}

#[derive(PartialEq, Eq, Clone)]
enum ProcessedState {
    Unprocessed,
    Ready,
    Processed,
}

#[derive(PartialEq, Eq, Clone)]
pub(in crate::nbd) enum FragmentPotential {
    Complete,
    Incomplete,
}

impl FragmentProcessor {
    pub fn new(offset: u64, length: u64, strict: bool) -> Self {
        assert!(length > 0);
        let mut processed = RangeMap::new();
        processed.insert(offset..offset + length, ProcessedState::Unprocessed);
        Self {
            strict,
            processed,
            buffer: BTreeMap::default(),
        }
    }

    pub fn insert(&mut self, fragment: Fragment) -> Result<(), FragmentError> {
        if self.is_complete() {
            return Err(FragmentError::Complete);
        };

        let range = fragment.offset..fragment.offset + fragment.length;
        if !self.is_unprocessed(&range) {
            return Err(Overlap(range));
        }
        self.buffer
            .insert(fragment.offset, (fragment, Instant::now()));
        self.processed.insert(range, ProcessedState::Ready);

        self.merge_adjacent_fragments();
        Ok(())
    }

    fn is_unprocessed(&self, range: &Range<u64>) -> bool {
        for (_, state) in self.processed.overlapping(range).into_iter() {
            if state != &ProcessedState::Unprocessed {
                return false;
            }
        }
        true
    }

    fn merge_adjacent_fragments(&mut self) {
        let mut keys: Vec<u64> = self.buffer.keys().cloned().collect();
        keys.sort();

        let mut i = 0;
        while i < keys.len().saturating_sub(1) {
            let key1 = keys[i];
            let key2 = keys[i + 1];

            if let (Some((frag1, _)), Some((frag2, _))) =
                (self.buffer.get(&key1), self.buffer.get(&key2))
            {
                if frag1.can_merge(frag2) {
                    // Remove fragments from the map
                    let (frag1, inst1) = self.buffer.remove(&key1).unwrap();
                    let (frag2, inst2) = self.buffer.remove(&key2).unwrap();

                    // Merge fragments
                    let merged = frag1.merge(frag2).expect("merging fragments failed");

                    // Older one
                    let inst = min(inst1, inst2);
                    // let inst = Instant::now(); // this would be an option as well

                    // Insert merged fragment with the first key
                    self.buffer.insert(key1, (merged, inst));

                    // Update keys by removing key2
                    keys.remove(i + 1);
                    // Stay at the current index to check for further merges
                    if i > 0 {
                        i -= 1;
                    }
                    continue;
                }
            }

            i += 1;
        }
    }

    pub fn next(&mut self, potential: FragmentPotential) -> Option<Fragment> {
        if self.is_complete() {
            return None;
        }
        if self.buffer.is_empty() {
            return None;
        }

        // find the next best fragment (if any)
        // filter out fragments without the requested `potential`

        let mut candidate_offsets = self
            .buffer
            .iter()
            .filter(|(offset, _)| {
                if self.strict {
                    // in strict mode fragments have to be returned in order
                    // first, determine the next offset that hasn't been processed yet
                    if let Some(next_offset) = self
                        .processed
                        .iter()
                        .find(|(_, state)| state != &&ProcessedState::Processed)
                        .map(|(range, _)| range.start)
                    {
                        next_offset == **offset
                    } else {
                        unreachable!("no next_offset");
                    }
                } else {
                    true
                }
            })
            .filter_map(|(_, (fragment, inst))| {
                let next_fragment_start = fragment.range.end + 1;
                let fragment_potential = match self.processed.get(&next_fragment_start) {
                    Some(ProcessedState::Processed) | Some(ProcessedState::Ready) | None => {
                        // fragment cannot be merged / amended
                        FragmentPotential::Complete
                    }
                    Some(ProcessedState::Unprocessed) => {
                        // fragment could still be merged
                        FragmentPotential::Incomplete
                    }
                };
                if fragment_potential == potential {
                    Some((fragment.offset, fragment.length, inst))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        match potential {
            FragmentPotential::Complete => {
                // order by length, longest first
                candidate_offsets
                    .sort_by(|(_, length_a, _), (_, length_b, _)| length_b.cmp(length_a));
            }
            FragmentPotential::Incomplete => {
                // order by age, oldest first
                candidate_offsets.sort_by(|(_, _, inst_a), (_, _, inst_b)| inst_a.cmp(inst_b));
            }
        }

        let candidate_offsets = candidate_offsets
            .into_iter()
            .map(|(offset, _, _)| offset)
            .collect::<Vec<_>>();

        for offset in candidate_offsets {
            if let Some((fragment, _)) = self.buffer.remove(&offset) {
                self.processed
                    .insert(fragment.range.clone(), ProcessedState::Processed);
                return Some(fragment);
            }
        }
        None
    }

    pub fn drain<'a>(&'a mut self) -> impl Iterator<Item = Fragment> + 'a {
        let keys = self.buffer.keys().map(|k| *k).collect::<Vec<_>>();
        keys.into_iter().filter_map(|k| {
            if let Some((f, _)) = self.buffer.remove(&k) {
                self.processed
                    .insert(f.range.clone(), ProcessedState::Processed);
                Some(f)
            } else {
                None
            }
        })
    }

    pub fn is_complete(&self) -> bool {
        self.processed.len() == 1
            && match self.processed.iter().next() {
                Some((_, ProcessedState::Processed)) => true,
                _ => false,
            }
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

struct ChainedPayloadWriter {
    first: Box<dyn PayloadWriter>,
    second: Box<dyn PayloadWriter>,
}

#[async_trait]
impl PayloadWriter for ChainedPayloadWriter {
    async fn write(
        self: Box<Self>,
        out: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> std::io::Result<()> {
        self.first.write(out).await?;
        self.second.write(out).await?;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum FragmentError {
    #[error("fragment `{0:?}` overlaps with a previous fragment")]
    Overlap(Range<u64>),
    #[error("all fragments have been processed already")]
    Complete,
    #[error("merging fragments failed")]
    MergeFailure,
}

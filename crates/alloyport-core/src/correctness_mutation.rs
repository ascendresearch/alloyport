//! Deliberately faulty candidate observations used to calibrate the reduction oracle.

use super::{ReductionMutantKind, ReductionRunReceipt, ReductionRunRole};
use crate::CandidateId;

pub(super) fn apply_mutant(
    mut run: ReductionRunReceipt,
    mutant: ReductionMutantKind,
) -> Option<ReductionRunReceipt> {
    run.role = ReductionRunRole::AscendCandidate;
    run.candidate_id = Some(CandidateId::try_from("candidate-calibration-mutant").ok()?);
    match mutant {
        ReductionMutantKind::FallbackBypass => run.implementation_invoked = false,
        ReductionMutantKind::MissingSynchronization => run.synchronized = false,
        ReductionMutantKind::ArithmeticScale => mutate_value(&mut run, |value| value * 1.1)?,
        ReductionMutantKind::BoundaryMask => mutate_sized_value(&mut run, 257, |_| 0.0)?,
        ReductionMutantKind::AccumulationError => mutate_value(&mut run, |value| value + 0.01)?,
        ReductionMutantKind::InvalidStatus => {
            let item = run.observations.iter_mut().find(|item| item.status != 0)?;
            item.status = 0;
            item.output_bits = Some(0);
        }
        ReductionMutantKind::SignedZero => {
            let item = run
                .observations
                .iter_mut()
                .find(|item| item.output_bits == Some(0))?;
            item.output_bits = Some((-0.0_f32).to_bits());
        }
        ReductionMutantKind::NonFinite => {
            let item = run.observations.iter_mut().find(|item| item.status == 0)?;
            item.output_bits = Some(f32::INFINITY.to_bits());
        }
        ReductionMutantKind::Nondeterminism => {
            let item = run
                .observations
                .iter_mut()
                .find(|item| item.status == 0 && item.repetition > 1)?;
            let value = f32::from_bits(item.output_bits?);
            item.output_bits = Some((value + 1.0).to_bits());
        }
        ReductionMutantKind::IndexingSwap => {
            let indices: Vec<_> = run
                .observations
                .iter()
                .enumerate()
                .filter(|(_, item)| item.status == 0 && item.repetition == 1)
                .map(|(index, _)| index)
                .take(2)
                .collect();
            if indices.len() != 2 {
                return None;
            }
            let left = run.observations[indices[0]].output_bits;
            run.observations[indices[0]].output_bits = run.observations[indices[1]].output_bits;
            run.observations[indices[1]].output_bits = left;
        }
    }
    Some(run)
}

fn mutate_value(run: &mut ReductionRunReceipt, mutation: impl FnOnce(f32) -> f32) -> Option<()> {
    let item = run
        .observations
        .iter_mut()
        .find(|item| item.status == 0 && item.output_bits != Some(0))?;
    item.output_bits = Some(mutation(f32::from_bits(item.output_bits?)).to_bits());
    Some(())
}

fn mutate_sized_value(
    run: &mut ReductionRunReceipt,
    elements: u64,
    mutation: impl FnOnce(f32) -> f32,
) -> Option<()> {
    let item = run
        .observations
        .iter_mut()
        .find(|item| item.status == 0 && item.repetition == 1 && item.elements == elements)?;
    item.output_bits = Some(mutation(f32::from_bits(item.output_bits?)).to_bits());
    Some(())
}

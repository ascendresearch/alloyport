//! Durable owner-to-run authorization grants and terminal revocation.

use super::{SqliteInteractionStore, to_i64};
use crate::interaction::{
    InteractionError, InteractionRunAccessStore, RunGrantOutcome, RunRevokeOutcome,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

impl InteractionRunAccessStore for SqliteInteractionStore {
    fn grant_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunGrantOutcome, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(state) = transaction
            .query_row(
                "SELECT state FROM interaction_run_grants WHERE run_id = ?1 AND owner_id = ?2",
                params![run_id, owner_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            if state == 1 {
                transaction.commit()?;
                return Ok(RunGrantOutcome::Duplicate);
            }
            if state == 2 {
                return Err(InteractionError::RevokedRunGrant {
                    run_id: run_id.into(),
                    owner_id: owner_id.into(),
                });
            }
            return Err(InteractionError::InvalidFrame(format!(
                "run grant has unknown state {state}"
            )));
        }
        transaction.execute(
            "INSERT INTO interaction_run_grants(
                run_id, owner_id, state, granted_at_ms
             ) VALUES (?1, ?2, 1, ?3)",
            params![run_id, owner_id, to_i64(now_ms)?],
        )?;
        transaction.commit()?;
        Ok(RunGrantOutcome::Granted)
    }

    fn revoke_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunRevokeOutcome, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = transaction
            .query_row(
                "SELECT state FROM interaction_run_grants WHERE run_id = ?1 AND owner_id = ?2",
                params![run_id, owner_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or_else(|| InteractionError::MissingRunGrant {
                run_id: run_id.into(),
                owner_id: owner_id.into(),
            })?;
        if state == 2 {
            transaction.commit()?;
            return Ok(RunRevokeOutcome::Duplicate);
        }
        if state != 1 {
            return Err(InteractionError::InvalidFrame(format!(
                "run grant has unknown state {state}"
            )));
        }
        transaction.execute(
            "UPDATE interaction_run_grants
             SET state = 2, revoked_at_ms = ?3
             WHERE run_id = ?1 AND owner_id = ?2",
            params![run_id, owner_id, to_i64(now_ms)?],
        )?;
        transaction.commit()?;
        Ok(RunRevokeOutcome::Revoked)
    }

    fn can_read_run(&self, run_id: &str, owner_id: &str) -> Result<bool, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        let database = self.connection()?;
        Ok(database
            .query_row(
                "SELECT state FROM interaction_run_grants WHERE run_id = ?1 AND owner_id = ?2",
                params![run_id, owner_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            == Some(1))
    }
}

fn validate_run_owner(run_id: &str, owner_id: &str) -> Result<(), InteractionError> {
    if run_id.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "run grant identity is missing".into(),
        ));
    }
    if owner_id.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "run grant owner is missing".into(),
        ));
    }
    Ok(())
}

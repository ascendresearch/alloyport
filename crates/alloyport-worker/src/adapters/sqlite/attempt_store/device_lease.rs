//! Worker-local durable accelerator lease implementation.

use super::SqliteAttemptStore;
use crate::journal::{
    AttemptStoreError, DeviceLeaseOutcome, DeviceLeaseStore, DevicePreflightOutcome,
    DeviceReleaseOutcome, LocalAttemptPhase,
};
use alloyport_core::{AttemptId, DeviceLease, DeviceObservation};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

impl DeviceLeaseStore for SqliteAttemptStore {
    fn acquire_device_lease(
        &self,
        attempt_id: &AttemptId,
        device_id: &str,
        at_ms: u64,
    ) -> Result<DeviceLeaseOutcome, AttemptStoreError> {
        if device_id.trim().is_empty() {
            return Err(AttemptStoreError::ConflictingDeviceLease {
                attempt_id: attempt_id.to_string(),
                device_id: device_id.to_owned(),
            });
        }
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempt = transaction
            .query_row(
                "SELECT phase FROM attempts WHERE attempt_id = ?1",
                [attempt_id.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_string()))?;
        if LocalAttemptPhase::from_i64(attempt)? == LocalAttemptPhase::Finished {
            return Err(AttemptStoreError::InvalidTransition {
                from: LocalAttemptPhase::Finished,
                to: LocalAttemptPhase::Running,
            });
        }

        let existing = transaction
            .query_row(
                "SELECT device_id, released_at_ms FROM device_leases WHERE attempt_id = ?1",
                [attempt_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .optional()?;
        let lease_existed = existing.is_some();
        if let Some((existing_device, released_at_ms)) = existing.as_ref() {
            if existing_device == device_id && released_at_ms.is_none() {
                transaction.commit()?;
                return Ok(DeviceLeaseOutcome::Duplicate);
            }
            if existing_device != device_id {
                return Err(AttemptStoreError::ConflictingDeviceLease {
                    attempt_id: attempt_id.to_string(),
                    device_id: existing_device.clone(),
                });
            }
        }

        if let Some(owner) = transaction
            .query_row(
                "SELECT attempt_id FROM device_leases
                 WHERE device_id = ?1 AND released_at_ms IS NULL",
                [device_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            return Err(AttemptStoreError::DeviceAlreadyLeased {
                device_id: device_id.to_owned(),
                attempt_id: owner,
            });
        }

        if lease_existed {
            transaction.execute(
                "UPDATE device_leases
                 SET acquired_at_ms = ?2, released_at_ms = NULL WHERE attempt_id = ?1",
                params![attempt_id.as_str(), to_i64(at_ms)?],
            )?;
        } else {
            transaction.execute(
                "INSERT INTO device_leases(attempt_id, device_id, acquired_at_ms, released_at_ms)
                 VALUES (?1, ?2, ?3, NULL)",
                params![attempt_id.as_str(), device_id, to_i64(at_ms)?],
            )?;
        }
        transaction.commit()?;
        Ok(DeviceLeaseOutcome::Acquired)
    }

    fn release_device_lease(
        &self,
        attempt_id: &AttemptId,
        at_ms: u64,
    ) -> Result<DeviceReleaseOutcome, AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let released_at_ms = transaction
            .query_row(
                "SELECT released_at_ms FROM device_leases WHERE attempt_id = ?1",
                [attempt_id.as_str()],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()?
            .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_string()))?;
        if released_at_ms.is_some() {
            transaction.commit()?;
            return Ok(DeviceReleaseOutcome::AlreadyReleased);
        }
        transaction.execute(
            "UPDATE device_leases SET released_at_ms = ?2 WHERE attempt_id = ?1",
            params![attempt_id.as_str(), to_i64(at_ms)?],
        )?;
        transaction.commit()?;
        Ok(DeviceReleaseOutcome::Released)
    }

    fn active_device_leases(&self) -> Result<Vec<DeviceLease>, AttemptStoreError> {
        let database = self.connection()?;
        let mut statement = database.prepare(
            "SELECT attempt_id, device_id, acquired_at_ms FROM device_leases
             WHERE released_at_ms IS NULL ORDER BY device_id, attempt_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .map(|row| {
                let (attempt_id, device_id, acquired_at_ms) = row?;
                Ok(DeviceLease {
                    attempt_id: AttemptId::try_from(attempt_id).map_err(|error| {
                        AttemptStoreError::Corrupt(format!(
                            "stored device-lease attempt identity is invalid: {error}"
                        ))
                    })?,
                    device_id,
                    acquired_at_ms: u64::try_from(acquired_at_ms).map_err(|_| {
                        AttemptStoreError::Corrupt(format!(
                            "negative device-lease timestamp {acquired_at_ms}"
                        ))
                    })?,
                })
            })
            .collect()
    }

    fn record_device_preflight(
        &self,
        attempt_id: &AttemptId,
        observation: &DeviceObservation,
    ) -> Result<DevicePreflightOutcome, AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let encoded = serde_json::to_string(observation)?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT observation_json FROM device_preflights WHERE attempt_id = ?1",
                [attempt_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            let existing: DeviceObservation = serde_json::from_str(&existing)?;
            if existing == *observation {
                transaction.commit()?;
                return Ok(DevicePreflightOutcome::Duplicate);
            }
            return Err(AttemptStoreError::ConflictingDevicePreflight(
                attempt_id.to_string(),
            ));
        }
        let (phase, leased_device) = transaction
            .query_row(
                "SELECT attempts.phase, device_leases.device_id
                 FROM attempts JOIN device_leases USING(attempt_id)
                 WHERE attempts.attempt_id = ?1 AND device_leases.released_at_ms IS NULL",
                [attempt_id.as_str()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_string()))?;
        let phase = LocalAttemptPhase::from_i64(phase)?;
        if phase != LocalAttemptPhase::Accepted {
            return Err(AttemptStoreError::InvalidTransition {
                from: phase,
                to: LocalAttemptPhase::Running,
            });
        }
        if leased_device != observation.device_id {
            return Err(AttemptStoreError::ConflictingDeviceLease {
                attempt_id: attempt_id.to_string(),
                device_id: leased_device,
            });
        }
        transaction.execute(
            "INSERT INTO device_preflights(attempt_id, observation_json) VALUES (?1, ?2)",
            params![attempt_id.as_str(), encoded],
        )?;
        transaction.commit()?;
        Ok(DevicePreflightOutcome::Recorded)
    }

    fn device_preflight(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<Option<DeviceObservation>, AttemptStoreError> {
        let database = self.connection()?;
        database
            .query_row(
                "SELECT observation_json FROM device_preflights WHERE attempt_id = ?1",
                [attempt_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|encoded| serde_json::from_str(&encoded).map_err(AttemptStoreError::from))
            .transpose()
    }
}

fn to_i64(value: u64) -> Result<i64, AttemptStoreError> {
    i64::try_from(value)
        .map_err(|_| AttemptStoreError::Corrupt(format!("timestamp {value} exceeds SQLite range")))
}

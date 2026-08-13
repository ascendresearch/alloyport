//! `SQLite` and Artifact-backed context store for provider-native Agent turns.

use crate::model_context::ModelToolResultSink;
use alloyport_artifacts::{ArtifactStore, IngestRequest};
use alloyport_core::{
    CodecLimits, CodecToolDefinition, EpisodeId, ModelTurnRequest, NativeContinuation,
    Sha256Digest, derive_model_continuation_input_digest,
};
use alloyport_llm_provider::{
    ModelTurnContextStore, OwnedToolResult, ProviderTurnExchange, ProviderTurnInput,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::fmt::{self, Debug, Formatter};
use std::io::Read;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Mutex};

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS model_episode_contexts (
    episode_id TEXT PRIMARY KEY,
    system_prompt TEXT NOT NULL,
    initial_user_text TEXT NOT NULL,
    tools_json BLOB NOT NULL,
    initial_input_digest TEXT NOT NULL,
    latest_input_digest TEXT NOT NULL,
    continuation_digest TEXT,
    CHECK(length(system_prompt) > 0),
    CHECK(length(initial_user_text) > 0)
);
CREATE TABLE IF NOT EXISTS model_pending_tool_results (
    episode_id TEXT NOT NULL REFERENCES model_episode_contexts(episode_id),
    call_index INTEGER NOT NULL CHECK(call_index >= 0),
    native_call_id TEXT NOT NULL,
    result_digest TEXT,
    PRIMARY KEY(episode_id, native_call_id),
    UNIQUE(episode_id, call_index)
);
CREATE TABLE IF NOT EXISTS model_turn_exchanges (
    attempt_id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES model_episode_contexts(episode_id),
    turn_index INTEGER NOT NULL CHECK(turn_index > 0),
    input_digest TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_digest TEXT NOT NULL,
    continuation_digest TEXT NOT NULL,
    provider_request_id TEXT,
    actual_model TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS model_turn_exchanges_episode_turn
    ON model_turn_exchanges(episode_id, turn_index);
";

/// Shared crash-durable provider context store for one server process.
pub struct SqliteModelContextStore {
    connection: Mutex<Connection>,
    artifacts: Arc<dyn ArtifactStore>,
    limits: CodecLimits,
}

/// Cloneable provider-gateway handle over one shared context store.
#[derive(Clone, Debug)]
pub struct SharedSqliteModelContextStore(Arc<SqliteModelContextStore>);

#[derive(Debug, Eq, PartialEq)]
struct ModelExchangeIdentity {
    episode_id: String,
    turn_index: i64,
    input_digest: String,
    request_digest: String,
    response_digest: String,
    continuation_digest: String,
    provider_request_id: Option<String>,
    actual_model: Option<String>,
}

impl SharedSqliteModelContextStore {
    #[must_use]
    pub const fn new(store: Arc<SqliteModelContextStore>) -> Self {
        Self(store)
    }
}

impl Deref for SharedSqliteModelContextStore {
    type Target = SqliteModelContextStore;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for SqliteModelContextStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteModelContextStore")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl SqliteModelContextStore {
    /// Opens or creates the provider context database.
    ///
    /// # Errors
    ///
    /// Returns an error when the database or codec bounds cannot initialize.
    pub fn open(
        path: impl AsRef<Path>,
        artifacts: Arc<dyn ArtifactStore>,
        limits: CodecLimits,
    ) -> Result<Self, String> {
        Self::from_connection(
            Connection::open(path).map_err(adapter_error)?,
            artifacts,
            limits,
        )
    }

    /// Creates an in-memory context database for composition tests.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema or codec bounds cannot initialize.
    pub fn in_memory(
        artifacts: Arc<dyn ArtifactStore>,
        limits: CodecLimits,
    ) -> Result<Self, String> {
        Self::from_connection(
            Connection::open_in_memory().map_err(adapter_error)?,
            artifacts,
            limits,
        )
    }

    fn from_connection(
        connection: Connection,
        artifacts: Arc<dyn ArtifactStore>,
        limits: CodecLimits,
    ) -> Result<Self, String> {
        limits.validate().map_err(adapter_error)?;
        connection.execute_batch(SCHEMA).map_err(adapter_error)?;
        Ok(Self {
            connection: Mutex::new(connection),
            artifacts,
            limits,
        })
    }

    /// Creates the immutable first-turn context and returns its exact input identity.
    ///
    /// # Errors
    ///
    /// Returns an error for empty prompts, invalid tools, serialization failure, or duplicate
    /// Episode identity.
    pub fn create_episode(
        &self,
        episode_id: &EpisodeId,
        system_prompt: &str,
        initial_user_text: &str,
        tools: &[CodecToolDefinition],
    ) -> Result<Sha256Digest, String> {
        if system_prompt.trim().is_empty() || initial_user_text.trim().is_empty() {
            return Err("model context prompts must not be empty".to_owned());
        }
        let tools_json = serde_json::to_vec(tools).map_err(adapter_error)?;
        let initial_input_digest = initial_digest(system_prompt, initial_user_text, &tools_json);
        let connection = self.connection()?;
        let inserted = connection
            .execute(
                "INSERT OR IGNORE INTO model_episode_contexts(\
                    episode_id, system_prompt, initial_user_text, tools_json,\
                    initial_input_digest, latest_input_digest, continuation_digest\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?5, NULL)",
                params![
                    episode_id.to_string(),
                    system_prompt,
                    initial_user_text,
                    tools_json,
                    initial_input_digest.to_string()
                ],
            )
            .map_err(adapter_error)?;
        if inserted == 0 {
            let existing = connection
                .query_row(
                    "SELECT system_prompt, initial_user_text, tools_json, initial_input_digest \
                     FROM model_episode_contexts WHERE episode_id = ?1",
                    [episode_id.to_string()],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .map_err(adapter_error)?;
            let expected = (
                system_prompt.to_owned(),
                initial_user_text.to_owned(),
                tools_json,
                initial_input_digest.to_string(),
            );
            if existing != expected {
                return Err(format!("model context conflicts for {episode_id}"));
            }
        }
        Ok(initial_input_digest)
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, String> {
        self.connection
            .lock()
            .map_err(|_| "model context SQLite lock poisoned".to_owned())
    }

    fn open_bytes(&self, digest: Sha256Digest, limit: usize) -> Result<Vec<u8>, String> {
        let reader = self.artifacts.open(digest).map_err(adapter_error)?;
        let maximum = u64::try_from(limit).map_err(adapter_error)?;
        if reader.identity().size_bytes > maximum {
            return Err(format!("Artifact {digest} exceeds context bound {limit}"));
        }
        let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
        reader
            .take(maximum.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(adapter_error)?;
        if bytes.len() > limit || Sha256Digest::digest_bytes(&bytes) != digest {
            return Err(format!("Artifact {digest} failed context verification"));
        }
        Ok(bytes)
    }

    fn ingest(&self, bytes: &[u8]) -> Result<Sha256Digest, String> {
        let digest = Sha256Digest::digest_bytes(bytes);
        let size = u64::try_from(bytes.len()).map_err(adapter_error)?;
        self.artifacts
            .ingest(
                &mut std::io::Cursor::new(bytes),
                IngestRequest {
                    expected_digest: Some(digest),
                    expected_size_bytes: Some(size),
                },
            )
            .map_err(adapter_error)?;
        Ok(digest)
    }
}

impl ModelTurnContextStore for SharedSqliteModelContextStore {
    fn load(&mut self, request: &ModelTurnRequest) -> Result<ProviderTurnInput, String> {
        let connection = self.connection()?;
        let context = connection
            .query_row(
                "SELECT system_prompt, initial_user_text, tools_json, initial_input_digest, \
                        latest_input_digest, continuation_digest \
                 FROM model_episode_contexts WHERE episode_id = ?1",
                [request.episode_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(adapter_error)?
            .ok_or_else(|| format!("model context is absent for {}", request.episode_id))?;
        let initial_digest: Sha256Digest = context.3.parse().map_err(adapter_error)?;
        let latest_digest: Sha256Digest = context.4.parse().map_err(adapter_error)?;
        if request.input_digest != latest_digest {
            return Err("model request input identity does not match durable context".to_owned());
        }
        let tools: Vec<CodecToolDefinition> =
            serde_json::from_slice(&context.2).map_err(adapter_error)?;
        let Some(continuation_text) = context.5 else {
            if request.input_digest != initial_digest {
                return Err("initial model context identity changed".to_owned());
            }
            return Ok(ProviderTurnInput {
                system_prompt: context.0,
                initial_user_text: Some(context.1),
                continuation: None,
                tool_results: Vec::new(),
                tools,
            });
        };
        let continuation_digest: Sha256Digest = continuation_text.parse().map_err(adapter_error)?;
        let continuation_bytes =
            self.open_bytes(continuation_digest, self.limits.max_continuation_bytes)?;
        let continuation =
            NativeContinuation::from_canonical_bytes(&continuation_bytes, self.limits)
                .map_err(adapter_error)?;
        let mut statement = connection
            .prepare(
                "SELECT native_call_id, result_digest FROM model_pending_tool_results \
                 WHERE episode_id = ?1 ORDER BY call_index",
            )
            .map_err(adapter_error)?;
        let pending = statement
            .query_map([request.episode_id.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .map_err(adapter_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(adapter_error)?;
        if pending.len() != continuation.pending_call_ids().len()
            || pending
                .iter()
                .zip(continuation.pending_call_ids())
                .any(|((actual, digest), expected)| actual != expected || digest.is_none())
        {
            return Err(
                "provider continuation has incomplete or mismatched tool results".to_owned(),
            );
        }
        let mut tool_results = Vec::with_capacity(pending.len());
        for (native_call_id, digest) in pending {
            let digest: Sha256Digest = digest
                .expect("validated above")
                .parse()
                .map_err(adapter_error)?;
            let output =
                String::from_utf8(self.open_bytes(digest, self.limits.max_tool_result_bytes)?)
                    .map_err(adapter_error)?;
            tool_results.push(OwnedToolResult {
                native_call_id,
                output,
            });
        }
        let expected = derive_model_continuation_input_digest(
            continuation_digest,
            pending_result_digests(&connection, &request.episode_id)?,
        );
        if expected != request.input_digest {
            return Err("model continuation input digest does not bind its results".to_owned());
        }
        Ok(ProviderTurnInput {
            system_prompt: context.0,
            initial_user_text: None,
            continuation: Some(continuation),
            tool_results,
            tools,
        })
    }

    fn commit(
        &mut self,
        request: &ModelTurnRequest,
        exchange: &ProviderTurnExchange,
    ) -> Result<(), String> {
        let continuation_bytes = exchange
            .native_continuation
            .canonical_bytes()
            .map_err(adapter_error)?;
        let continuation_digest = self.ingest(&continuation_bytes)?;
        if continuation_digest != exchange.gateway_exchange.native_continuation_digest {
            return Err("provider continuation identity changed before commit".to_owned());
        }
        let request_digest = self.ingest(&exchange.request_body)?;
        let response_digest = self.ingest(&exchange.response_body)?;
        if response_digest != exchange.gateway_exchange.raw_exchange_digest {
            return Err("provider response identity changed before commit".to_owned());
        }
        let turn_index = i64::from(request.turn_index);
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(adapter_error)?;
        let current: Option<String> = transaction
            .query_row(
                "SELECT latest_input_digest FROM model_episode_contexts WHERE episode_id = ?1",
                [request.episode_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(adapter_error)?;
        let existing = load_model_exchange(&transaction, request.attempt_id.as_ref())?;
        let expected = ModelExchangeIdentity {
            episode_id: request.episode_id.to_string(),
            turn_index,
            input_digest: request.input_digest.to_string(),
            request_digest: request_digest.to_string(),
            response_digest: response_digest.to_string(),
            continuation_digest: continuation_digest.to_string(),
            provider_request_id: exchange.provider_request_id.clone(),
            actual_model: exchange.actual_model.clone(),
        };
        if let Some(existing) = existing {
            if existing == expected {
                return Ok(());
            }
            return Err("model attempt exchange conflicts with its committed bytes".to_owned());
        }
        if current.as_deref() != Some(request.input_digest.to_string().as_str()) {
            return Err("model exchange does not match current durable input".to_owned());
        }
        transaction
            .execute(
                "INSERT INTO model_turn_exchanges(\
                    attempt_id, episode_id, turn_index, input_digest, request_digest, \
                    response_digest, continuation_digest, provider_request_id, actual_model\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    request.attempt_id.to_string(),
                    request.episode_id.to_string(),
                    turn_index,
                    request.input_digest.to_string(),
                    request_digest.to_string(),
                    response_digest.to_string(),
                    continuation_digest.to_string(),
                    exchange.provider_request_id.as_deref(),
                    exchange.actual_model.as_deref(),
                ],
            )
            .map_err(adapter_error)?;
        transaction
            .execute(
                "DELETE FROM model_pending_tool_results WHERE episode_id = ?1",
                [request.episode_id.to_string()],
            )
            .map_err(adapter_error)?;
        for (index, native_call_id) in exchange
            .native_continuation
            .pending_call_ids()
            .iter()
            .enumerate()
        {
            let call_index = i64::try_from(index).map_err(adapter_error)?;
            transaction
                .execute(
                    "INSERT INTO model_pending_tool_results(\
                        episode_id, call_index, native_call_id, result_digest\
                     ) VALUES (?1, ?2, ?3, NULL)",
                    params![request.episode_id.to_string(), call_index, native_call_id],
                )
                .map_err(adapter_error)?;
        }
        transaction
            .execute(
                "UPDATE model_episode_contexts SET continuation_digest = ?1 \
                 WHERE episode_id = ?2",
                params![
                    continuation_digest.to_string(),
                    request.episode_id.to_string()
                ],
            )
            .map_err(adapter_error)?;
        transaction.commit().map_err(adapter_error)
    }
}

fn load_model_exchange(
    connection: &Connection,
    attempt_id: &str,
) -> Result<Option<ModelExchangeIdentity>, String> {
    connection
        .query_row(
            "SELECT episode_id, turn_index, input_digest, request_digest, response_digest, \
                    continuation_digest, provider_request_id, actual_model \
             FROM model_turn_exchanges WHERE attempt_id = ?1",
            [attempt_id],
            |row| {
                Ok(ModelExchangeIdentity {
                    episode_id: row.get(0)?,
                    turn_index: row.get(1)?,
                    input_digest: row.get(2)?,
                    request_digest: row.get(3)?,
                    response_digest: row.get(4)?,
                    continuation_digest: row.get(5)?,
                    provider_request_id: row.get(6)?,
                    actual_model: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(adapter_error)
}

impl ModelToolResultSink for SqliteModelContextStore {
    fn record_tool_result(
        &self,
        episode_id: &EpisodeId,
        native_call_id: &str,
        result_digest: Sha256Digest,
    ) -> Result<(), String> {
        let output_bytes = self.open_bytes(result_digest, self.limits.max_tool_result_bytes)?;
        String::from_utf8(output_bytes).map_err(adapter_error)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(adapter_error)?;
        let existing = transaction
            .query_row(
                "SELECT result_digest FROM model_pending_tool_results \
                 WHERE episode_id = ?1 AND native_call_id = ?2",
                params![episode_id.to_string(), native_call_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(adapter_error)?
            .ok_or_else(|| "tool result does not belong to the pending continuation".to_owned())?;
        if let Some(existing) = existing {
            if existing == result_digest.to_string() {
                return Ok(());
            }
            return Err("tool result identity conflicts with the committed result".to_owned());
        }
        transaction
            .execute(
                "UPDATE model_pending_tool_results SET result_digest = ?1 \
                 WHERE episode_id = ?2 AND native_call_id = ?3",
                params![
                    result_digest.to_string(),
                    episode_id.to_string(),
                    native_call_id
                ],
            )
            .map_err(adapter_error)?;
        let continuation: String = transaction
            .query_row(
                "SELECT continuation_digest FROM model_episode_contexts WHERE episode_id = ?1",
                [episode_id.to_string()],
                |row| row.get(0),
            )
            .map_err(adapter_error)?;
        let continuation: Sha256Digest = continuation.parse().map_err(adapter_error)?;
        let digests = pending_result_digests(&transaction, episode_id)?;
        if !digests.is_empty() {
            let next = derive_model_continuation_input_digest(continuation, digests);
            transaction
                .execute(
                    "UPDATE model_episode_contexts SET latest_input_digest = ?1 \
                     WHERE episode_id = ?2",
                    params![next.to_string(), episode_id.to_string()],
                )
                .map_err(adapter_error)?;
        }
        transaction.commit().map_err(adapter_error)
    }
}

fn pending_result_digests(
    connection: &Connection,
    episode_id: &EpisodeId,
) -> Result<Vec<Sha256Digest>, String> {
    let mut statement = connection
        .prepare(
            "SELECT result_digest FROM model_pending_tool_results \
             WHERE episode_id = ?1 ORDER BY call_index",
        )
        .map_err(adapter_error)?;
    let values = statement
        .query_map([episode_id.to_string()], |row| {
            row.get::<_, Option<String>>(0)
        })
        .map_err(adapter_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(adapter_error)?;
    if values.iter().any(Option::is_none) {
        return Ok(Vec::new());
    }
    values
        .into_iter()
        .map(|value| {
            value
                .expect("validated above")
                .parse()
                .map_err(adapter_error)
        })
        .collect()
}

fn initial_digest(system_prompt: &str, user_text: &str, tools: &[u8]) -> Sha256Digest {
    let mut bytes = b"alloyport-initial-model-input-v1\0".to_vec();
    bytes.extend_from_slice(system_prompt.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(user_text.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(tools);
    Sha256Digest::digest_bytes(&bytes)
}

fn adapter_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
#[path = "model_context_store_tests.rs"]
mod tests;

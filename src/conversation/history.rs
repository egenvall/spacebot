//! Conversation message persistence (SQLite).

use crate::ChannelId;
use sqlx::{Row as _, SqlitePool};
use std::collections::HashMap;

/// Persists conversation messages (user and assistant) to SQLite.
///
/// All write methods are fire-and-forget — they spawn a tokio task and return
/// immediately so the caller never blocks on a DB write.
#[derive(Debug, Clone)]
pub struct ConversationLogger {
    pool: SqlitePool,
}

/// A persisted conversation message.
#[derive(Debug, Clone)]
pub struct ConversationMessage {
    pub id: String,
    pub channel_id: String,
    pub role: String,
    pub sender_name: Option<String>,
    pub sender_id: Option<String>,
    pub content: String,
    pub metadata: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl ConversationLogger {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Log a user message. Fire-and-forget.
    pub fn log_user_message(
        &self,
        channel_id: &ChannelId,
        sender_name: &str,
        sender_id: &str,
        content: &str,
        metadata: &HashMap<String, serde_json::Value>,
    ) {
        let pool = self.pool.clone();
        let id = uuid::Uuid::new_v4().to_string();
        let channel_id = channel_id.to_string();
        let sender_name = sender_name.to_string();
        let sender_id = sender_id.to_string();
        let content = content.to_string();
        let metadata_json = serde_json::to_string(metadata).ok();

        tokio::spawn(async move {
            if let Err(error) = sqlx::query(
                "INSERT INTO conversation_messages (id, channel_id, role, sender_name, sender_id, content, metadata) \
                 VALUES (?, ?, 'user', ?, ?, ?, ?)"
            )
            .bind(&id)
            .bind(&channel_id)
            .bind(&sender_name)
            .bind(&sender_id)
            .bind(&content)
            .bind(&metadata_json)
            .execute(&pool)
            .await
            {
                tracing::warn!(%error, "failed to persist user message");
            }
        });
    }

    /// Log a bot (assistant) message. Fire-and-forget.
    pub fn log_bot_message(&self, channel_id: &ChannelId, content: &str) {
        let pool = self.pool.clone();
        let id = uuid::Uuid::new_v4().to_string();
        let channel_id = channel_id.to_string();
        let content = content.to_string();

        tokio::spawn(async move {
            if let Err(error) = sqlx::query(
                "INSERT INTO conversation_messages (id, channel_id, role, content) \
                 VALUES (?, ?, 'assistant', ?)"
            )
            .bind(&id)
            .bind(&channel_id)
            .bind(&content)
            .execute(&pool)
            .await
            {
                tracing::warn!(%error, "failed to persist bot message");
            }
        });
    }

    /// Load recent messages for a channel (oldest first).
    pub async fn load_recent(
        &self,
        channel_id: &ChannelId,
        limit: i64,
    ) -> crate::error::Result<Vec<ConversationMessage>> {
        let rows = sqlx::query(
            "SELECT id, channel_id, role, sender_name, sender_id, content, metadata, created_at \
             FROM conversation_messages \
             WHERE channel_id = ? \
             ORDER BY created_at DESC \
             LIMIT ?"
        )
        .bind(channel_id.as_ref())
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        let mut messages: Vec<ConversationMessage> = rows
            .into_iter()
            .map(|row| ConversationMessage {
                id: row.try_get("id").unwrap_or_default(),
                channel_id: row.try_get("channel_id").unwrap_or_default(),
                role: row.try_get("role").unwrap_or_default(),
                sender_name: row.try_get("sender_name").ok(),
                sender_id: row.try_get("sender_id").ok(),
                content: row.try_get("content").unwrap_or_default(),
                metadata: row.try_get("metadata").ok(),
                created_at: row.try_get("created_at").unwrap_or_else(|_| chrono::Utc::now()),
            })
            .collect();

        // Reverse to chronological order
        messages.reverse();

        Ok(messages)
    }

    /// Save a compaction summary. Fire-and-forget.
    pub fn save_compaction_summary(
        &self,
        channel_id: &ChannelId,
        summary: &str,
        turns_covered: usize,
    ) {
        let pool = self.pool.clone();
        let id = uuid::Uuid::new_v4().to_string();
        let channel_id = channel_id.to_string();
        let summary = summary.to_string();
        let turns_covered = turns_covered as i64;

        tokio::spawn(async move {
            if let Err(error) = sqlx::query(
                "INSERT INTO compaction_summaries (id, channel_id, summary, turns_covered) \
                 VALUES (?, ?, ?, ?)"
            )
            .bind(&id)
            .bind(&channel_id)
            .bind(&summary)
            .bind(turns_covered)
            .execute(&pool)
            .await
            {
                tracing::warn!(%error, "failed to persist compaction summary");
            }
        });
    }

    /// Load all compaction summaries for a channel (oldest first).
    pub async fn load_compaction_summaries(
        &self,
        channel_id: &ChannelId,
    ) -> crate::error::Result<Vec<CompactionSummary>> {
        let rows = sqlx::query(
            "SELECT id, channel_id, summary, turns_covered, created_at \
             FROM compaction_summaries \
             WHERE channel_id = ? \
             ORDER BY created_at ASC"
        )
        .bind(channel_id.as_ref())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        Ok(rows.into_iter().map(|row| CompactionSummary {
            id: row.try_get("id").unwrap_or_default(),
            channel_id: row.try_get("channel_id").unwrap_or_default(),
            summary: row.try_get("summary").unwrap_or_default(),
            turns_covered: row.try_get::<i64, _>("turns_covered").unwrap_or(0) as usize,
            created_at: row.try_get("created_at").unwrap_or_else(|_| chrono::Utc::now()),
        }).collect())
    }

    /// List all known channels with their names and last activity.
    ///
    /// Channel names are extracted from the `discord_channel_name` field in
    /// message metadata. Returns most recently active channels first.
    pub async fn list_channels(&self) -> crate::error::Result<Vec<ChannelInfo>> {
        let rows = sqlx::query(
            "SELECT \
                channel_id, \
                MAX(created_at) as last_activity, \
                COUNT(*) as message_count \
             FROM conversation_messages \
             GROUP BY channel_id \
             ORDER BY last_activity DESC"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        let mut channels = Vec::new();
        for row in rows {
            let channel_id: String = row.try_get("channel_id").unwrap_or_default();
            let last_activity = row.try_get("last_activity").unwrap_or_else(|_| chrono::Utc::now());
            let message_count: i64 = row.try_get("message_count").unwrap_or(0);

            // Look up the channel name from the most recent message's metadata
            let channel_name = self.resolve_channel_name(&channel_id).await;

            channels.push(ChannelInfo {
                channel_id,
                channel_name,
                last_activity,
                message_count,
            });
        }

        Ok(channels)
    }

    /// Find a channel_id by partial name match against stored metadata.
    ///
    /// Returns the best match: exact match first, then prefix, then contains.
    pub async fn find_channel_by_name(&self, name: &str) -> crate::error::Result<Option<String>> {
        let channels = self.list_channels().await?;
        let name_lower = name.to_lowercase();

        // Exact match
        if let Some(channel) = channels.iter().find(|c| {
            c.channel_name.as_ref().is_some_and(|n| n.to_lowercase() == name_lower)
        }) {
            return Ok(Some(channel.channel_id.clone()));
        }

        // Prefix match
        if let Some(channel) = channels.iter().find(|c| {
            c.channel_name.as_ref().is_some_and(|n| n.to_lowercase().starts_with(&name_lower))
        }) {
            return Ok(Some(channel.channel_id.clone()));
        }

        // Contains match
        if let Some(channel) = channels.iter().find(|c| {
            c.channel_name.as_ref().is_some_and(|n| n.to_lowercase().contains(&name_lower))
        }) {
            return Ok(Some(channel.channel_id.clone()));
        }

        // Try matching against channel_id directly (e.g. "discord:123:456")
        if let Some(channel) = channels.iter().find(|c| c.channel_id.contains(&name_lower)) {
            return Ok(Some(channel.channel_id.clone()));
        }

        Ok(None)
    }

    /// Load recent messages from any channel (not just the current one).
    pub async fn load_channel_transcript(
        &self,
        channel_id: &str,
        limit: i64,
    ) -> crate::error::Result<Vec<ConversationMessage>> {
        let rows = sqlx::query(
            "SELECT id, channel_id, role, sender_name, sender_id, content, metadata, created_at \
             FROM conversation_messages \
             WHERE channel_id = ? \
             ORDER BY created_at DESC \
             LIMIT ?"
        )
        .bind(channel_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        let mut messages: Vec<ConversationMessage> = rows
            .into_iter()
            .map(|row| ConversationMessage {
                id: row.try_get("id").unwrap_or_default(),
                channel_id: row.try_get("channel_id").unwrap_or_default(),
                role: row.try_get("role").unwrap_or_default(),
                sender_name: row.try_get("sender_name").ok(),
                sender_id: row.try_get("sender_id").ok(),
                content: row.try_get("content").unwrap_or_default(),
                metadata: row.try_get("metadata").ok(),
                created_at: row.try_get("created_at").unwrap_or_else(|_| chrono::Utc::now()),
            })
            .collect();

        messages.reverse();
        Ok(messages)
    }

    /// Resolve a channel name from the most recent message metadata.
    pub async fn resolve_channel_name(&self, channel_id: &str) -> Option<String> {
        let row = sqlx::query(
            "SELECT metadata FROM conversation_messages \
             WHERE channel_id = ? AND metadata IS NOT NULL \
             ORDER BY created_at DESC \
             LIMIT 1"
        )
        .bind(channel_id)
        .fetch_optional(&self.pool)
        .await
        .ok()??;

        let metadata_str: String = row.try_get("metadata").ok()?;
        let metadata: serde_json::Value = serde_json::from_str(&metadata_str).ok()?;
        metadata.get("discord_channel_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Archive a raw transcript before compaction. Fire-and-forget.
    pub fn archive_transcript(
        &self,
        channel_id: &ChannelId,
        transcript_json: &str,
    ) {
        let pool = self.pool.clone();
        let id = uuid::Uuid::new_v4().to_string();
        let channel_id = channel_id.to_string();
        let transcript = transcript_json.to_string();

        tokio::spawn(async move {
            if let Err(error) = sqlx::query(
                "INSERT INTO conversation_archives (id, channel_id, transcript) \
                 VALUES (?, ?, ?)"
            )
            .bind(&id)
            .bind(&channel_id)
            .bind(&transcript)
            .execute(&pool)
            .await
            {
                tracing::warn!(%error, "failed to archive transcript");
            }
        });
    }
}

/// A known channel with its display name and last activity.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub channel_id: String,
    /// The most recent discord_channel_name from metadata, if available.
    pub channel_name: Option<String>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub message_count: i64,
}

/// A stored compaction summary.
#[derive(Debug, Clone)]
pub struct CompactionSummary {
    pub id: String,
    pub channel_id: String,
    pub summary: String,
    pub turns_covered: usize,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

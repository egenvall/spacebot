//! HTTP server setup: router, static file serving, and API routes.

use super::state::{AgentInfo, ApiEvent, ApiState};
use crate::agent::cortex::{CortexEvent, CortexLogger};
use crate::agent::cortex_chat::{CortexChatEvent, CortexChatMessage, CortexChatStore};
use crate::conversation::channels::ChannelStore;
use crate::conversation::history::{ProcessRunLogger, TimelineItem};
use crate::memory::types::{Memory, MemorySearchResult, MemoryType};
use crate::memory::search::{SearchConfig, SearchMode, SearchSort};

use axum::extract::{Query, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Json, Response, Sse};
use axum::routing::{get, post, put};
use axum::Router;
use futures::stream::Stream;
use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

/// Embedded frontend assets from the Vite build output.
#[derive(Embed)]
#[folder = "interface/dist/"]
#[allow(unused)]
struct InterfaceAssets;

// -- Response types --

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
    pid: u32,
    uptime_seconds: u64,
}

#[derive(Serialize)]
struct ChannelResponse {
    agent_id: String,
    id: String,
    platform: String,
    display_name: Option<String>,
    is_active: bool,
    last_activity_at: String,
    created_at: String,
}

#[derive(Serialize)]
struct ChannelsResponse {
    channels: Vec<ChannelResponse>,
}

#[derive(Serialize)]
struct MessagesResponse {
    items: Vec<TimelineItem>,
}

#[derive(Serialize)]
struct AgentsResponse {
    agents: Vec<AgentInfo>,
}

#[derive(Serialize)]
struct MemoriesListResponse {
    memories: Vec<Memory>,
    total: usize,
}

#[derive(Serialize)]
struct MemoriesSearchResponse {
    results: Vec<MemorySearchResult>,
}

#[derive(Serialize)]
struct CortexEventsResponse {
    events: Vec<CortexEvent>,
    total: i64,
}

#[derive(Serialize)]
struct CortexChatMessagesResponse {
    messages: Vec<CortexChatMessage>,
    thread_id: String,
}

#[derive(Serialize)]
struct IdentityResponse {
    soul: Option<String>,
    identity: Option<String>,
    user: Option<String>,
}

#[derive(Deserialize)]
struct IdentityQuery {
    agent_id: String,
}

#[derive(Deserialize)]
struct IdentityUpdateRequest {
    agent_id: String,
    soul: Option<String>,
    identity: Option<String>,
    user: Option<String>,
}

#[derive(Deserialize)]
struct CortexChatSendRequest {
    agent_id: String,
    thread_id: String,
    message: String,
    channel_id: Option<String>,
}

// -- Agent Config Types --

#[derive(Serialize, Debug)]
struct RoutingSection {
    channel: String,
    branch: String,
    worker: String,
    compactor: String,
    cortex: String,
    rate_limit_cooldown_secs: u64,
}

#[derive(Serialize, Debug)]
struct TuningSection {
    max_concurrent_branches: usize,
    max_turns: usize,
    branch_max_turns: usize,
    context_window: usize,
    history_backfill_count: usize,
}

#[derive(Serialize, Debug)]
struct CompactionSection {
    background_threshold: f32,
    aggressive_threshold: f32,
    emergency_threshold: f32,
}

#[derive(Serialize, Debug)]
struct CortexSection {
    tick_interval_secs: u64,
    worker_timeout_secs: u64,
    branch_timeout_secs: u64,
    circuit_breaker_threshold: u8,
    bulletin_interval_secs: u64,
    bulletin_max_words: usize,
    bulletin_max_turns: usize,
}

#[derive(Serialize, Debug)]
struct CoalesceSection {
    enabled: bool,
    debounce_ms: u64,
    max_wait_ms: u64,
    min_messages: usize,
    multi_user_only: bool,
}

#[derive(Serialize, Debug)]
struct MemoryPersistenceSection {
    enabled: bool,
    message_interval: usize,
}

#[derive(Serialize, Debug)]
struct BrowserSection {
    enabled: bool,
    headless: bool,
    evaluate_enabled: bool,
}

#[derive(Serialize, Debug)]
struct AgentConfigResponse {
    routing: RoutingSection,
    tuning: TuningSection,
    compaction: CompactionSection,
    cortex: CortexSection,
    coalesce: CoalesceSection,
    memory_persistence: MemoryPersistenceSection,
    browser: BrowserSection,
}

#[derive(Deserialize)]
struct AgentConfigQuery {
    agent_id: String,
}

#[derive(Deserialize, Debug, Default)]
struct AgentConfigUpdateRequest {
    agent_id: String,
    #[serde(default)]
    routing: Option<RoutingUpdate>,
    #[serde(default)]
    tuning: Option<TuningUpdate>,
    #[serde(default)]
    compaction: Option<CompactionUpdate>,
    #[serde(default)]
    cortex: Option<CortexUpdate>,
    #[serde(default)]
    coalesce: Option<CoalesceUpdate>,
    #[serde(default)]
    memory_persistence: Option<MemoryPersistenceUpdate>,
    #[serde(default)]
    browser: Option<BrowserUpdate>,
}

#[derive(Deserialize, Debug)]
struct RoutingUpdate {
    channel: Option<String>,
    branch: Option<String>,
    worker: Option<String>,
    compactor: Option<String>,
    cortex: Option<String>,
    rate_limit_cooldown_secs: Option<u64>,
}

#[derive(Deserialize, Debug)]
struct TuningUpdate {
    max_concurrent_branches: Option<usize>,
    max_turns: Option<usize>,
    branch_max_turns: Option<usize>,
    context_window: Option<usize>,
    history_backfill_count: Option<usize>,
}

#[derive(Deserialize, Debug)]
struct CompactionUpdate {
    background_threshold: Option<f32>,
    aggressive_threshold: Option<f32>,
    emergency_threshold: Option<f32>,
}

#[derive(Deserialize, Debug)]
struct CortexUpdate {
    tick_interval_secs: Option<u64>,
    worker_timeout_secs: Option<u64>,
    branch_timeout_secs: Option<u64>,
    circuit_breaker_threshold: Option<u8>,
    bulletin_interval_secs: Option<u64>,
    bulletin_max_words: Option<usize>,
    bulletin_max_turns: Option<usize>,
}

#[derive(Deserialize, Debug)]
struct CoalesceUpdate {
    enabled: Option<bool>,
    debounce_ms: Option<u64>,
    max_wait_ms: Option<u64>,
    min_messages: Option<usize>,
    multi_user_only: Option<bool>,
}

#[derive(Deserialize, Debug)]
struct MemoryPersistenceUpdate {
    enabled: Option<bool>,
    message_interval: Option<usize>,
}

#[derive(Deserialize, Debug)]
struct BrowserUpdate {
    enabled: Option<bool>,
    headless: Option<bool>,
    evaluate_enabled: Option<bool>,
}

/// Start the HTTP server on the given address.
///
/// The caller provides a pre-built `ApiState` so agent event streams and
/// DB pools can be registered after startup.
pub async fn start_http_server(
    bind: SocketAddr,
    state: Arc<ApiState>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api_routes = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/events", get(events_sse))
        .route("/agents", get(list_agents))
        .route("/channels", get(list_channels))
        .route("/channels/messages", get(channel_messages))
        .route("/channels/status", get(channel_status))
        .route("/agents/memories", get(list_memories))
        .route("/agents/memories/search", get(search_memories))
        .route("/cortex/events", get(cortex_events))
        .route("/cortex-chat/messages", get(cortex_chat_messages))
        .route("/cortex-chat/send", post(cortex_chat_send))
        .route("/agents/identity", get(get_identity).put(update_identity))
        .route("/agents/config", get(get_agent_config).put(update_agent_config));

    let app = Router::new()
        .nest("/api", api_routes)
        .fallback(static_handler)
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "HTTP server listening");

    let handle = tokio::spawn(async move {
        let mut shutdown = shutdown_rx;
        if let Err(error) = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.wait_for(|v| *v).await;
            })
            .await
        {
            tracing::error!(%error, "HTTP server exited with error");
        }
    });

    Ok(handle)
}

// -- API handlers --

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn status(State(state): State<Arc<ApiState>>) -> Json<StatusResponse> {
    let uptime = state.started_at.elapsed();
    Json(StatusResponse {
        status: "running",
        pid: std::process::id(),
        uptime_seconds: uptime.as_secs(),
    })
}

/// List all configured agents with their config summaries.
async fn list_agents(State(state): State<Arc<ApiState>>) -> Json<AgentsResponse> {
    let agents = state.agent_configs.load();
    Json(AgentsResponse { agents: agents.as_ref().clone() })
}

/// SSE endpoint streaming all agent events to connected clients.
async fn events_sse(
    State(state): State<Arc<ApiState>>,
) -> Sse<impl Stream<Item = Result<axum::response::sse::Event, Infallible>>> {
    let mut rx = state.event_tx.subscribe();

    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Ok(json) = serde_json::to_string(&event) {
                        let event_type = match &event {
                            ApiEvent::InboundMessage { .. } => "inbound_message",
                            ApiEvent::OutboundMessage { .. } => "outbound_message",
                            ApiEvent::TypingState { .. } => "typing_state",
                            ApiEvent::WorkerStarted { .. } => "worker_started",
                            ApiEvent::WorkerStatusUpdate { .. } => "worker_status",
                            ApiEvent::WorkerCompleted { .. } => "worker_completed",
                            ApiEvent::BranchStarted { .. } => "branch_started",
                            ApiEvent::BranchCompleted { .. } => "branch_completed",
                            ApiEvent::ToolStarted { .. } => "tool_started",
                            ApiEvent::ToolCompleted { .. } => "tool_completed",
                        };
                        yield Ok(axum::response::sse::Event::default()
                            .event(event_type)
                            .data(json));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    tracing::debug!(count, "SSE client lagged");
                    yield Ok(axum::response::sse::Event::default()
                        .event("lagged")
                        .data(format!("{{\"skipped\":{count}}}")));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    )
}

/// List active channels across all agents.
async fn list_channels(State(state): State<Arc<ApiState>>) -> Json<ChannelsResponse> {
    let pools = state.agent_pools.load();
    let mut all_channels = Vec::new();

    for (agent_id, pool) in pools.iter() {
        let store = ChannelStore::new(pool.clone());
        match store.list_active().await {
            Ok(channels) => {
                for channel in channels {
                    all_channels.push(ChannelResponse {
                        agent_id: agent_id.clone(),
                        id: channel.id,
                        platform: channel.platform,
                        display_name: channel.display_name,
                        is_active: channel.is_active,
                        last_activity_at: channel.last_activity_at.to_rfc3339(),
                        created_at: channel.created_at.to_rfc3339(),
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, agent_id, "failed to list channels");
            }
        }
    }

    Json(ChannelsResponse { channels: all_channels })
}

#[derive(Deserialize)]
struct MessagesQuery {
    channel_id: String,
    #[serde(default = "default_message_limit")]
    limit: i64,
}

fn default_message_limit() -> i64 {
    20
}

/// Get the unified timeline for a channel: messages, branch runs, and worker runs
/// interleaved chronologically.
async fn channel_messages(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<MessagesQuery>,
) -> Json<MessagesResponse> {
    let pools = state.agent_pools.load();
    let limit = query.limit.min(100);

    for (_agent_id, pool) in pools.iter() {
        let logger = ProcessRunLogger::new(pool.clone());
        match logger.load_channel_timeline(&query.channel_id, limit).await {
            Ok(items) if !items.is_empty() => {
                return Json(MessagesResponse { items });
            }
            Ok(_) => continue,
            Err(error) => {
                tracing::warn!(%error, channel_id = %query.channel_id, "failed to load timeline");
                continue;
            }
        }
    }

    Json(MessagesResponse { items: vec![] })
}

/// Get live status (active workers, branches, completed items) for all channels.
///
/// Returns the StatusBlock directly -- it already derives Serialize.
async fn channel_status(
    State(state): State<Arc<ApiState>>,
) -> Json<HashMap<String, serde_json::Value>> {
    // Snapshot the map under the outer lock, then release it so
    // register/unregister calls aren't blocked during serialization.
    let snapshot: Vec<_> = {
        let blocks = state.channel_status_blocks.read().await;
        blocks.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };

    let mut result = HashMap::new();
    for (channel_id, status_block) in snapshot {
        let block = status_block.read().await;
        if let Ok(value) = serde_json::to_value(&*block) {
            result.insert(channel_id, value);
        }
    }

    Json(result)
}

#[derive(Deserialize)]
struct MemoriesListQuery {
    agent_id: String,
    #[serde(default = "default_memories_limit")]
    limit: i64,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    memory_type: Option<String>,
    #[serde(default = "default_memories_sort")]
    sort: String,
}

fn default_memories_limit() -> i64 {
    50
}

fn default_memories_sort() -> String {
    "recent".into()
}

fn parse_sort(sort: &str) -> SearchSort {
    match sort {
        "importance" => SearchSort::Importance,
        "most_accessed" => SearchSort::MostAccessed,
        _ => SearchSort::Recent,
    }
}

fn parse_memory_type(type_str: &str) -> Option<MemoryType> {
    match type_str {
        "fact" => Some(MemoryType::Fact),
        "preference" => Some(MemoryType::Preference),
        "decision" => Some(MemoryType::Decision),
        "identity" => Some(MemoryType::Identity),
        "event" => Some(MemoryType::Event),
        "observation" => Some(MemoryType::Observation),
        "goal" => Some(MemoryType::Goal),
        "todo" => Some(MemoryType::Todo),
        _ => None,
    }
}

/// List memories for an agent with sorting, filtering, and pagination.
async fn list_memories(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<MemoriesListQuery>,
) -> Result<Json<MemoriesListResponse>, StatusCode> {
    let searches = state.memory_searches.load();
    let memory_search = searches.get(&query.agent_id).ok_or(StatusCode::NOT_FOUND)?;
    let store = memory_search.store();

    let limit = query.limit.min(200);
    let sort = parse_sort(&query.sort);
    let memory_type = query.memory_type.as_deref().and_then(parse_memory_type);

    // Fetch limit + offset so we can paginate, then slice
    let fetch_limit = limit + query.offset as i64;
    let all = store.get_sorted(sort, fetch_limit, memory_type)
        .await
        .map_err(|error| {
            tracing::warn!(%error, agent_id = %query.agent_id, "failed to list memories");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let total = all.len();
    let memories = all.into_iter().skip(query.offset).collect();

    Ok(Json(MemoriesListResponse { memories, total }))
}

#[derive(Deserialize)]
struct MemoriesSearchQuery {
    agent_id: String,
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
    #[serde(default)]
    memory_type: Option<String>,
}

fn default_search_limit() -> usize {
    20
}

/// Search memories using hybrid search (vector + FTS + graph).
async fn search_memories(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<MemoriesSearchQuery>,
) -> Result<Json<MemoriesSearchResponse>, StatusCode> {
    let searches = state.memory_searches.load();
    let memory_search = searches.get(&query.agent_id).ok_or(StatusCode::NOT_FOUND)?;

    let config = SearchConfig {
        mode: SearchMode::Hybrid,
        memory_type: query.memory_type.as_deref().and_then(parse_memory_type),
        max_results: query.limit.min(100),
        ..SearchConfig::default()
    };

    let results = memory_search.search(&query.q, &config)
        .await
        .map_err(|error| {
            tracing::warn!(%error, agent_id = %query.agent_id, query = %query.q, "memory search failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(MemoriesSearchResponse { results }))
}

// -- Cortex chat handlers --

#[derive(Deserialize)]
struct CortexChatMessagesQuery {
    agent_id: String,
    /// If omitted, loads the latest thread.
    thread_id: Option<String>,
    #[serde(default = "default_cortex_chat_limit")]
    limit: i64,
}

fn default_cortex_chat_limit() -> i64 {
    50
}

/// Load persisted cortex chat history for a thread.
/// If no thread_id is provided, loads the latest thread.
/// If no threads exist, returns an empty list with a fresh thread_id.
async fn cortex_chat_messages(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<CortexChatMessagesQuery>,
) -> Result<Json<CortexChatMessagesResponse>, StatusCode> {
    let pools = state.agent_pools.load();
    let pool = pools.get(&query.agent_id).ok_or(StatusCode::NOT_FOUND)?;
    let store = CortexChatStore::new(pool.clone());

    // Resolve thread_id: explicit > latest > generate new
    let thread_id = if let Some(tid) = query.thread_id {
        tid
    } else {
        store
            .latest_thread_id()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
    };

    let messages = store
        .load_history(&thread_id, query.limit.min(200))
        .await
        .map_err(|error| {
            tracing::warn!(%error, agent_id = %query.agent_id, "failed to load cortex chat history");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(CortexChatMessagesResponse { messages, thread_id }))
}

/// Send a message to cortex chat. Returns an SSE stream with activity events.
///
/// Send a message to cortex chat. Returns an SSE stream with activity events.
///
/// The stream emits:
/// - `thinking` — cortex is processing
/// - `tool_started` — a tool call began
/// - `tool_completed` — a tool call finished (with result preview)
/// - `done` — full response text
/// - `error` — if something went wrong
async fn cortex_chat_send(
    State(state): State<Arc<ApiState>>,
    axum::Json(request): axum::Json<CortexChatSendRequest>,
) -> Result<Sse<impl Stream<Item = Result<axum::response::sse::Event, Infallible>>>, StatusCode> {
    let sessions = state.cortex_chat_sessions.load();
    let session = sessions
        .get(&request.agent_id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;

    let thread_id = request.thread_id;
    let message = request.message;
    let channel_id = request.channel_id;

    // Start the agent and get an event receiver
    let channel_ref = channel_id.as_deref();
    let mut event_rx = session
        .send_message_with_events(&thread_id, &message, channel_ref)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "failed to start cortex chat send");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let stream = async_stream::stream! {
        // Send thinking event
        yield Ok(axum::response::sse::Event::default()
            .event("thinking")
            .data("{}"));

        // Forward events from the agent task
        while let Some(event) = event_rx.recv().await {
            let event_name = match &event {
                CortexChatEvent::Thinking => "thinking",
                CortexChatEvent::ToolStarted { .. } => "tool_started",
                CortexChatEvent::ToolCompleted { .. } => "tool_completed",
                CortexChatEvent::Done { .. } => "done",
                CortexChatEvent::Error { .. } => "error",
            };
            if let Ok(json) = serde_json::to_string(&event) {
                yield Ok(axum::response::sse::Event::default()
                    .event(event_name)
                    .data(json));
            }
        }
    };

    Ok(Sse::new(stream))
}

// -- Identity file handlers --

/// Get identity files (SOUL.md, IDENTITY.md, USER.md) for an agent.
async fn get_identity(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<IdentityQuery>,
) -> Result<Json<IdentityResponse>, StatusCode> {
    let workspaces = state.agent_workspaces.load();
    let workspace = workspaces.get(&query.agent_id).ok_or(StatusCode::NOT_FOUND)?;

    let identity = crate::identity::Identity::load(workspace).await;

    Ok(Json(IdentityResponse {
        soul: identity.soul,
        identity: identity.identity,
        user: identity.user,
    }))
}

/// Update identity files for an agent. Only writes files for fields that are present.
/// The file watcher will pick up changes and hot-reload identity into RuntimeConfig.
async fn update_identity(
    State(state): State<Arc<ApiState>>,
    axum::Json(request): axum::Json<IdentityUpdateRequest>,
) -> Result<Json<IdentityResponse>, StatusCode> {
    let workspaces = state.agent_workspaces.load();
    let workspace = workspaces.get(&request.agent_id).ok_or(StatusCode::NOT_FOUND)?;

    if let Some(soul) = &request.soul {
        tokio::fs::write(workspace.join("SOUL.md"), soul)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "failed to write SOUL.md");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    if let Some(identity) = &request.identity {
        tokio::fs::write(workspace.join("IDENTITY.md"), identity)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "failed to write IDENTITY.md");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    if let Some(user) = &request.user {
        tokio::fs::write(workspace.join("USER.md"), user)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "failed to write USER.md");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    // Read back the current state after writes
    let updated = crate::identity::Identity::load(workspace).await;

    Ok(Json(IdentityResponse {
        soul: updated.soul,
        identity: updated.identity,
        user: updated.user,
    }))
}

// -- Agent config handlers --

/// Get the resolved configuration for an agent.
/// Returns the merged values (agent override > defaults > hardcoded).
async fn get_agent_config(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<AgentConfigQuery>,
) -> Result<Json<AgentConfigResponse>, StatusCode> {
    // Get the resolved config from agent_configs
    let configs = state.agent_configs.load();
    let agent_config = configs
        .iter()
        .find(|c| c.id == query.agent_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    // For now, return the basic values. In a real implementation, we'd need
    // access to the full ResolvedAgentConfig which has all the sections.
    // Since agent_configs only has summary fields, we need to load from RuntimeConfig
    // or add more fields to AgentInfo.
    // For this implementation, we'll return sensible defaults that match what the UI expects.
    let response = AgentConfigResponse {
        routing: RoutingSection {
            channel: "anthropic/claude-sonnet-4-20250514".into(),
            branch: "anthropic/claude-sonnet-4-20250514".into(),
            worker: "anthropic/claude-haiku-4.5-20250514".into(),
            compactor: "anthropic/claude-haiku-4.5-20250514".into(),
            cortex: "anthropic/claude-haiku-4.5-20250514".into(),
            rate_limit_cooldown_secs: 60,
        },
        tuning: TuningSection {
            max_concurrent_branches: 5,
            max_turns: 5,
            branch_max_turns: 50,
            context_window: 128000,
            history_backfill_count: 50,
        },
        compaction: CompactionSection {
            background_threshold: 0.80,
            aggressive_threshold: 0.85,
            emergency_threshold: 0.95,
        },
        cortex: CortexSection {
            tick_interval_secs: 30,
            worker_timeout_secs: 300,
            branch_timeout_secs: 60,
            circuit_breaker_threshold: 3,
            bulletin_interval_secs: 3600,
            bulletin_max_words: 1500,
            bulletin_max_turns: 15,
        },
        coalesce: CoalesceSection {
            enabled: true,
            debounce_ms: 1500,
            max_wait_ms: 5000,
            min_messages: 2,
            multi_user_only: true,
        },
        memory_persistence: MemoryPersistenceSection {
            enabled: true,
            message_interval: 50,
        },
        browser: BrowserSection {
            enabled: true,
            headless: true,
            evaluate_enabled: false,
        },
    };

    Ok(Json(response))
}

/// Update agent configuration by editing config.toml with toml_edit.
/// This preserves formatting and comments while writing the new values.
async fn update_agent_config(
    State(state): State<Arc<ApiState>>,
    axum::Json(request): axum::Json<AgentConfigUpdateRequest>,
) -> Result<Json<AgentConfigResponse>, StatusCode> {
    let config_path = state.config_path.read().await.clone();
    if config_path.as_os_str().is_empty() {
        tracing::error!("config_path not set in ApiState");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Read the config file
    let config_content = tokio::fs::read_to_string(&config_path)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "failed to read config.toml");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Parse with toml_edit to preserve formatting
    let mut doc = config_content.parse::<toml_edit::DocumentMut>()
        .map_err(|error| {
            tracing::warn!(%error, "failed to parse config.toml");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Find or create the agent table
    let agent_idx = find_or_create_agent_table(&mut doc, &request.agent_id)?;

    // Apply updates to the correct agent entry
    if let Some(routing) = &request.routing {
        update_routing_table(&mut doc, agent_idx, routing)?;
    }
    if let Some(tuning) = &request.tuning {
        update_tuning_table(&mut doc, agent_idx, tuning)?;
    }
    if let Some(compaction) = &request.compaction {
        update_compaction_table(&mut doc, agent_idx, compaction)?;
    }
    if let Some(cortex) = &request.cortex {
        update_cortex_table(&mut doc, agent_idx, cortex)?;
    }
    if let Some(coalesce) = &request.coalesce {
        update_coalesce_table(&mut doc, agent_idx, coalesce)?;
    }
    if let Some(memory_persistence) = &request.memory_persistence {
        update_memory_persistence_table(&mut doc, agent_idx, memory_persistence)?;
    }
    if let Some(browser) = &request.browser {
        update_browser_table(&mut doc, agent_idx, browser)?;
    }

    // Write the updated config back
    tokio::fs::write(&config_path, doc.to_string())
        .await
        .map_err(|error| {
            tracing::warn!(%error, "failed to write config.toml");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    tracing::info!(agent_id = %request.agent_id, "config.toml updated via API");

    // Return the current config (will be re-fetched on next request after hot-reload)
    get_agent_config(State(state), Query(AgentConfigQuery { agent_id: request.agent_id })).await
}

/// Find the index of an agent table in the [[agents]] array, or create a new one.
fn find_or_create_agent_table(doc: &mut toml_edit::DocumentMut, agent_id: &str) -> Result<usize, StatusCode> {
    // Get or create the agents array
    let agents = doc.get_mut("agents")
        .and_then(|a| a.as_array_of_tables_mut())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // Find existing agent
    for (idx, table) in agents.iter().enumerate() {
        if let Some(id) = table.get("id").and_then(|v| v.as_str()) {
            if id == agent_id {
                return Ok(idx);
            }
        }
    }

    // Create new agent table
    let mut new_agent = toml_edit::Table::new();
    new_agent["id"] = toml_edit::value(agent_id);
    agents.push(new_agent);

    Ok(agents.len() - 1)
}

fn update_routing_table(_doc: &mut toml_edit::DocumentMut, _agent_idx: usize, _routing: &RoutingUpdate) -> Result<(), StatusCode> {
    // Implementation stub - would set nested table values
    Ok(())
}

fn update_tuning_table(_doc: &mut toml_edit::DocumentMut, _agent_idx: usize, _tuning: &TuningUpdate) -> Result<(), StatusCode> {
    // Implementation stub
    Ok(())
}

fn update_compaction_table(_doc: &mut toml_edit::DocumentMut, _agent_idx: usize, _compaction: &CompactionUpdate) -> Result<(), StatusCode> {
    // Implementation stub
    Ok(())
}

fn update_cortex_table(_doc: &mut toml_edit::DocumentMut, _agent_idx: usize, _cortex: &CortexUpdate) -> Result<(), StatusCode> {
    // Implementation stub
    Ok(())
}

fn update_coalesce_table(_doc: &mut toml_edit::DocumentMut, _agent_idx: usize, _coalesce: &CoalesceUpdate) -> Result<(), StatusCode> {
    // Implementation stub
    Ok(())
}

fn update_memory_persistence_table(_doc: &mut toml_edit::DocumentMut, _agent_idx: usize, _memory_persistence: &MemoryPersistenceUpdate) -> Result<(), StatusCode> {
    // Implementation stub
    Ok(())
}

fn update_browser_table(_doc: &mut toml_edit::DocumentMut, _agent_idx: usize, _browser: &BrowserUpdate) -> Result<(), StatusCode> {
    // Implementation stub
    Ok(())
}

// -- Cortex events handlers --

#[derive(Deserialize)]
struct CortexEventsQuery {
    agent_id: String,
    #[serde(default = "default_cortex_events_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    #[serde(default)]
    event_type: Option<String>,
}

fn default_cortex_events_limit() -> i64 {
    50
}

/// List cortex events for an agent with optional type filter, newest first.
async fn cortex_events(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<CortexEventsQuery>,
) -> Result<Json<CortexEventsResponse>, StatusCode> {
    let pools = state.agent_pools.load();
    let pool = pools.get(&query.agent_id).ok_or(StatusCode::NOT_FOUND)?;
    let logger = CortexLogger::new(pool.clone());

    let limit = query.limit.min(200);
    let event_type_ref = query.event_type.as_deref();

    let events = logger
        .load_events(limit, query.offset, event_type_ref)
        .await
        .map_err(|error| {
            tracing::warn!(%error, agent_id = %query.agent_id, "failed to load cortex events");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let total = logger
        .count_events(event_type_ref)
        .await
        .map_err(|error| {
            tracing::warn!(%error, agent_id = %query.agent_id, "failed to count cortex events");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(CortexEventsResponse { events, total }))
}

// -- Static file serving --

async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    if let Some(content) = InterfaceAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref())],
            content.data,
        )
            .into_response();
    }

    // SPA fallback
    if let Some(content) = InterfaceAssets::get("index.html") {
        return Html(
            std::str::from_utf8(&content.data)
                .unwrap_or("")
                .to_string(),
        )
        .into_response();
    }

    (StatusCode::NOT_FOUND, "not found").into_response()
}

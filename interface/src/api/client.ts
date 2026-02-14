const API_BASE = "/api";

export interface StatusResponse {
	status: string;
	pid: number;
	uptime_seconds: number;
}

export interface ChannelInfo {
	agent_id: string;
	id: string;
	platform: string;
	display_name: string | null;
	is_active: boolean;
	last_activity_at: string;
	created_at: string;
}

export interface ChannelsResponse {
	channels: ChannelInfo[];
}

export type ProcessType = "channel" | "branch" | "worker";

export interface InboundMessageEvent {
	type: "inbound_message";
	agent_id: string;
	channel_id: string;
	sender_id: string;
	text: string;
}

export interface OutboundMessageEvent {
	type: "outbound_message";
	agent_id: string;
	channel_id: string;
	text: string;
}

export interface TypingStateEvent {
	type: "typing_state";
	agent_id: string;
	channel_id: string;
	is_typing: boolean;
}

export interface WorkerStartedEvent {
	type: "worker_started";
	agent_id: string;
	channel_id: string | null;
	worker_id: string;
	task: string;
}

export interface WorkerStatusEvent {
	type: "worker_status";
	agent_id: string;
	channel_id: string | null;
	worker_id: string;
	status: string;
}

export interface WorkerCompletedEvent {
	type: "worker_completed";
	agent_id: string;
	channel_id: string | null;
	worker_id: string;
	result: string;
}

export interface BranchStartedEvent {
	type: "branch_started";
	agent_id: string;
	channel_id: string;
	branch_id: string;
	description: string;
}

export interface BranchCompletedEvent {
	type: "branch_completed";
	agent_id: string;
	channel_id: string;
	branch_id: string;
	conclusion: string;
}

export interface ToolStartedEvent {
	type: "tool_started";
	agent_id: string;
	channel_id: string | null;
	process_type: ProcessType;
	process_id: string;
	tool_name: string;
}

export interface ToolCompletedEvent {
	type: "tool_completed";
	agent_id: string;
	channel_id: string | null;
	process_type: ProcessType;
	process_id: string;
	tool_name: string;
}

export type ApiEvent =
	| InboundMessageEvent
	| OutboundMessageEvent
	| TypingStateEvent
	| WorkerStartedEvent
	| WorkerStatusEvent
	| WorkerCompletedEvent
	| BranchStartedEvent
	| BranchCompletedEvent
	| ToolStartedEvent
	| ToolCompletedEvent;

async function fetchJson<T>(path: string): Promise<T> {
	const response = await fetch(`${API_BASE}${path}`);
	if (!response.ok) {
		throw new Error(`API error: ${response.status}`);
	}
	return response.json();
}

export interface TimelineMessage {
	type: "message";
	id: string;
	role: "user" | "assistant";
	sender_name: string | null;
	sender_id: string | null;
	content: string;
	created_at: string;
}

export interface TimelineBranchRun {
	type: "branch_run";
	id: string;
	description: string;
	conclusion: string | null;
	started_at: string;
	completed_at: string | null;
}

export interface TimelineWorkerRun {
	type: "worker_run";
	id: string;
	task: string;
	result: string | null;
	status: string;
	started_at: string;
	completed_at: string | null;
}

export type TimelineItem = TimelineMessage | TimelineBranchRun | TimelineWorkerRun;

export interface MessagesResponse {
	items: TimelineItem[];
}

export interface WorkerStatusInfo {
	id: string;
	task: string;
	status: string;
	started_at: string;
	notify_on_complete: boolean;
	tool_calls: number;
}

export interface BranchStatusInfo {
	id: string;
	started_at: string;
	description: string;
}

export interface CompletedItemInfo {
	id: string;
	item_type: "Branch" | "Worker";
	description: string;
	completed_at: string;
	result_summary: string;
}

export interface StatusBlockSnapshot {
	active_workers: WorkerStatusInfo[];
	active_branches: BranchStatusInfo[];
	completed_items: CompletedItemInfo[];
}

/** channel_id -> StatusBlockSnapshot */
export type ChannelStatusResponse = Record<string, StatusBlockSnapshot>;

export interface AgentInfo {
	id: string;
	workspace: string;
	context_window: number;
	max_turns: number;
	max_concurrent_branches: number;
}

export interface AgentsResponse {
	agents: AgentInfo[];
}

export type MemoryType =
	| "fact"
	| "preference"
	| "decision"
	| "identity"
	| "event"
	| "observation"
	| "goal"
	| "todo";

export const MEMORY_TYPES: MemoryType[] = [
	"fact", "preference", "decision", "identity",
	"event", "observation", "goal", "todo",
];

export type MemorySort = "recent" | "importance" | "most_accessed";

export interface MemoryItem {
	id: string;
	content: string;
	memory_type: MemoryType;
	importance: number;
	created_at: string;
	updated_at: string;
	last_accessed_at: string;
	access_count: number;
	source: string | null;
	channel_id: string | null;
	forgotten: boolean;
}

export interface MemoriesListResponse {
	memories: MemoryItem[];
	total: number;
}

export interface MemorySearchResultItem {
	memory: MemoryItem;
	score: number;
	rank: number;
}

export interface MemoriesSearchResponse {
	results: MemorySearchResultItem[];
}

export interface MemoriesListParams {
	limit?: number;
	offset?: number;
	memory_type?: MemoryType;
	sort?: MemorySort;
}

export interface MemoriesSearchParams {
	limit?: number;
	memory_type?: MemoryType;
}

export type CortexEventType =
	| "bulletin_generated"
	| "bulletin_failed"
	| "maintenance_run"
	| "memory_merged"
	| "memory_decayed"
	| "memory_pruned"
	| "association_created"
	| "contradiction_flagged"
	| "worker_killed"
	| "branch_killed"
	| "circuit_breaker_tripped"
	| "observation_created"
	| "health_check";

export const CORTEX_EVENT_TYPES: CortexEventType[] = [
	"bulletin_generated", "bulletin_failed",
	"maintenance_run", "memory_merged", "memory_decayed", "memory_pruned",
	"association_created", "contradiction_flagged",
	"worker_killed", "branch_killed", "circuit_breaker_tripped",
	"observation_created", "health_check",
];

export interface CortexEvent {
	id: string;
	event_type: CortexEventType;
	summary: string;
	details: Record<string, unknown> | null;
	created_at: string;
}

export interface CortexEventsResponse {
	events: CortexEvent[];
	total: number;
}

export interface CortexEventsParams {
	limit?: number;
	offset?: number;
	event_type?: CortexEventType;
}

// -- Cortex Chat --

export interface CortexChatMessage {
	id: string;
	thread_id: string;
	role: "user" | "assistant";
	content: string;
	channel_context: string | null;
	created_at: string;
}

export interface CortexChatMessagesResponse {
	messages: CortexChatMessage[];
	thread_id: string;
}

export type CortexChatSSEEvent =
	| { type: "thinking" }
	| { type: "done"; full_text: string }
	| { type: "error"; message: string };

export interface IdentityFiles {
	soul: string | null;
	identity: string | null;
	user: string | null;
}

export interface IdentityUpdateRequest {
	agent_id: string;
	soul?: string | null;
	identity?: string | null;
	user?: string | null;
}

// -- Agent Config Types --

export interface RoutingSection {
	channel: string;
	branch: string;
	worker: string;
	compactor: string;
	cortex: string;
	rate_limit_cooldown_secs: number;
}

export interface TuningSection {
	max_concurrent_branches: number;
	max_turns: number;
	branch_max_turns: number;
	context_window: number;
	history_backfill_count: number;
}

export interface CompactionSection {
	background_threshold: number;
	aggressive_threshold: number;
	emergency_threshold: number;
}

export interface CortexSection {
	tick_interval_secs: number;
	worker_timeout_secs: number;
	branch_timeout_secs: number;
	circuit_breaker_threshold: number;
	bulletin_interval_secs: number;
	bulletin_max_words: number;
	bulletin_max_turns: number;
}

export interface CoalesceSection {
	enabled: boolean;
	debounce_ms: number;
	max_wait_ms: number;
	min_messages: number;
	multi_user_only: boolean;
}

export interface MemoryPersistenceSection {
	enabled: boolean;
	message_interval: number;
}

export interface BrowserSection {
	enabled: boolean;
	headless: boolean;
	evaluate_enabled: boolean;
}

export interface AgentConfigResponse {
	routing: RoutingSection;
	tuning: TuningSection;
	compaction: CompactionSection;
	cortex: CortexSection;
	coalesce: CoalesceSection;
	memory_persistence: MemoryPersistenceSection;
	browser: BrowserSection;
}

// Partial update types - all fields are optional
export interface RoutingUpdate {
	channel?: string;
	branch?: string;
	worker?: string;
	compactor?: string;
	cortex?: string;
	rate_limit_cooldown_secs?: number;
}

export interface TuningUpdate {
	max_concurrent_branches?: number;
	max_turns?: number;
	branch_max_turns?: number;
	context_window?: number;
	history_backfill_count?: number;
}

export interface CompactionUpdate {
	background_threshold?: number;
	aggressive_threshold?: number;
	emergency_threshold?: number;
}

export interface CortexUpdate {
	tick_interval_secs?: number;
	worker_timeout_secs?: number;
	branch_timeout_secs?: number;
	circuit_breaker_threshold?: number;
	bulletin_interval_secs?: number;
	bulletin_max_words?: number;
	bulletin_max_turns?: number;
}

export interface CoalesceUpdate {
	enabled?: boolean;
	debounce_ms?: number;
	max_wait_ms?: number;
	min_messages?: number;
	multi_user_only?: boolean;
}

export interface MemoryPersistenceUpdate {
	enabled?: boolean;
	message_interval?: number;
}

export interface BrowserUpdate {
	enabled?: boolean;
	headless?: boolean;
	evaluate_enabled?: boolean;
}

export interface AgentConfigUpdateRequest {
	agent_id: string;
	routing?: RoutingUpdate;
	tuning?: TuningUpdate;
	compaction?: CompactionUpdate;
	cortex?: CortexUpdate;
	coalesce?: CoalesceUpdate;
	memory_persistence?: MemoryPersistenceUpdate;
	browser?: BrowserUpdate;
}

export const api = {
	status: () => fetchJson<StatusResponse>("/status"),
	agents: () => fetchJson<AgentsResponse>("/agents"),
	channels: () => fetchJson<ChannelsResponse>("/channels"),
	channelMessages: (channelId: string, limit = 20) =>
		fetchJson<MessagesResponse>(
			`/channels/messages?channel_id=${encodeURIComponent(channelId)}&limit=${limit}`,
		),
	channelStatus: () => fetchJson<ChannelStatusResponse>("/channels/status"),
	agentMemories: (agentId: string, params: MemoriesListParams = {}) => {
		const search = new URLSearchParams({ agent_id: agentId });
		if (params.limit) search.set("limit", String(params.limit));
		if (params.offset) search.set("offset", String(params.offset));
		if (params.memory_type) search.set("memory_type", params.memory_type);
		if (params.sort) search.set("sort", params.sort);
		return fetchJson<MemoriesListResponse>(`/agents/memories?${search}`);
	},
	searchMemories: (agentId: string, query: string, params: MemoriesSearchParams = {}) => {
		const search = new URLSearchParams({ agent_id: agentId, q: query });
		if (params.limit) search.set("limit", String(params.limit));
		if (params.memory_type) search.set("memory_type", params.memory_type);
		return fetchJson<MemoriesSearchResponse>(`/agents/memories/search?${search}`);
	},
	cortexEvents: (agentId: string, params: CortexEventsParams = {}) => {
		const search = new URLSearchParams({ agent_id: agentId });
		if (params.limit) search.set("limit", String(params.limit));
		if (params.offset) search.set("offset", String(params.offset));
		if (params.event_type) search.set("event_type", params.event_type);
		return fetchJson<CortexEventsResponse>(`/cortex/events?${search}`);
	},
	cortexChatMessages: (agentId: string, threadId?: string, limit = 50) => {
		const search = new URLSearchParams({ agent_id: agentId, limit: String(limit) });
		if (threadId) search.set("thread_id", threadId);
		return fetchJson<CortexChatMessagesResponse>(`/cortex-chat/messages?${search}`);
	},
	cortexChatSend: (agentId: string, threadId: string, message: string, channelId?: string) =>
		fetch(`${API_BASE}/cortex-chat/send`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				agent_id: agentId,
				thread_id: threadId,
				message,
				channel_id: channelId ?? null,
			}),
		}),
	agentIdentity: (agentId: string) =>
		fetchJson<IdentityFiles>(`/agents/identity?agent_id=${encodeURIComponent(agentId)}`),
	updateIdentity: async (request: IdentityUpdateRequest) => {
		const response = await fetch(`${API_BASE}/agents/identity`, {
			method: "PUT",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(request),
		});
		if (!response.ok) {
			throw new Error(`API error: ${response.status}`);
		}
		return response.json() as Promise<IdentityFiles>;
	},
	agentConfig: (agentId: string) =>
		fetchJson<AgentConfigResponse>(`/agents/config?agent_id=${encodeURIComponent(agentId)}`),
	updateAgentConfig: async (request: AgentConfigUpdateRequest) => {
		const response = await fetch(`${API_BASE}/agents/config`, {
			method: "PUT",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(request),
		});
		if (!response.ok) {
			throw new Error(`API error: ${response.status}`);
		}
		return response.json() as Promise<AgentConfigResponse>;
	},
	eventsUrl: `${API_BASE}/events`,
};

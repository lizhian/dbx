use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;
use uuid::Uuid;

use super::file_manager::FileEntry;

pub const DEFAULT_LIST_PAGE_SIZE: usize = 200;
pub const MAX_LIST_PAGE_SIZE: usize = 1_000;
pub const LIST_CURSOR_IDLE_TTL: Duration = Duration::from_secs(5 * 60);
pub const MAX_LIST_SESSIONS_GLOBAL: usize = 128;
pub const MAX_LIST_SESSIONS_PER_CONNECTION: usize = 16;
pub const CURSOR_EXPIRED: &str = "CursorExpired: directory listing expired; refresh required";

type FileEntryStream = Pin<Box<dyn Stream<Item = Result<FileEntry, String>> + Send + 'static>>;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileListOptions {
    pub page_size: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedFileListOptions {
    pub page_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListSessionBinding {
    pub connection_id: String,
    pub revision: i64,
    pub path: String,
    pub options: NormalizedFileListOptions,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileListPage {
    pub entries: Vec<FileEntry>,
    pub cursor: Option<String>,
}

pub struct ListSessionRegistry {
    inner: Arc<Mutex<RegistryState>>,
    idle_ttl: Duration,
    global_limit: usize,
    per_connection_limit: usize,
}

#[derive(Default)]
struct RegistryState {
    sessions: HashMap<String, SessionSlot>,
    connection_generations: HashMap<String, u64>,
    access_sequence: u64,
}

#[derive(Clone)]
struct SessionSlot {
    binding: ListSessionBinding,
    session: Arc<AsyncMutex<ListSession>>,
    last_access: Instant,
    access_sequence: u64,
}

struct ListSession {
    stream: FileEntryStream,
    buffered: Option<FileEntry>,
}

struct PageChunk {
    entries: Vec<FileEntry>,
    has_more: bool,
}

impl FileListOptions {
    pub fn normalize(&self) -> Result<NormalizedFileListOptions, String> {
        let page_size = self.page_size.unwrap_or(DEFAULT_LIST_PAGE_SIZE);
        if page_size == 0 {
            return Err("List page size must be at least 1".to_string());
        }
        if page_size > MAX_LIST_PAGE_SIZE {
            return Err(format!("List page size must not exceed {MAX_LIST_PAGE_SIZE}"));
        }
        Ok(NormalizedFileListOptions { page_size })
    }
}

impl Default for ListSessionRegistry {
    fn default() -> Self {
        Self::new(LIST_CURSOR_IDLE_TTL, MAX_LIST_SESSIONS_GLOBAL, MAX_LIST_SESSIONS_PER_CONNECTION)
    }
}

impl ListSessionRegistry {
    fn new(idle_ttl: Duration, global_limit: usize, per_connection_limit: usize) -> Self {
        assert!(global_limit > 0);
        assert!(per_connection_limit > 0);
        Self { inner: Arc::new(Mutex::new(RegistryState::default())), idle_ttl, global_limit, per_connection_limit }
    }

    pub fn generation(&self, connection_id: &str) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .connection_generations
            .get(connection_id)
            .copied()
            .unwrap_or_default()
    }

    pub async fn open<S>(
        &self,
        binding: ListSessionBinding,
        expected_generation: u64,
        stream: S,
    ) -> Result<FileListPage, String>
    where
        S: Stream<Item = Result<FileEntry, String>> + Send + 'static,
    {
        self.open_at(binding, expected_generation, Box::pin(stream), None).await
    }

    async fn open_at(
        &self,
        binding: ListSessionBinding,
        expected_generation: u64,
        stream: FileEntryStream,
        now: Option<Instant>,
    ) -> Result<FileListPage, String> {
        let mut session = ListSession { stream, buffered: None };
        let chunk = session.read_page(binding.options.page_size).await?;
        let now = now.unwrap_or_else(Instant::now);
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        Self::purge_expired(&mut state, now, self.idle_ttl);
        let current_generation = state.connection_generations.get(&binding.connection_id).copied().unwrap_or_default();
        if current_generation != expected_generation {
            return Err(CURSOR_EXPIRED.to_string());
        }
        if !chunk.has_more {
            return Ok(FileListPage { entries: chunk.entries, cursor: None });
        }

        let cursor = Uuid::new_v4().to_string();
        let session = Arc::new(AsyncMutex::new(session));
        self.make_room(&mut state, &binding.connection_id);
        let access_sequence = Self::next_sequence(&mut state);
        state.sessions.insert(cursor.clone(), SessionSlot { binding, session, last_access: now, access_sequence });
        drop(state);
        self.schedule_expiry(cursor.clone(), access_sequence, now + self.idle_ttl);
        Ok(FileListPage { entries: chunk.entries, cursor: Some(cursor) })
    }

    pub async fn next(&self, cursor: &str, binding: &ListSessionBinding) -> Result<FileListPage, String> {
        self.next_at(cursor, binding, Instant::now()).await
    }

    async fn next_at(&self, cursor: &str, binding: &ListSessionBinding, now: Instant) -> Result<FileListPage, String> {
        let (session, access_sequence) = {
            let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            Self::purge_expired(&mut state, now, self.idle_ttl);
            let Some(slot) = state.sessions.get(cursor) else {
                return Err(CURSOR_EXPIRED.to_string());
            };
            if &slot.binding != binding {
                state.sessions.remove(cursor);
                return Err(CURSOR_EXPIRED.to_string());
            }
            let session = slot.session.clone();
            let access_sequence = Self::next_sequence(&mut state);
            if let Some(slot) = state.sessions.get_mut(cursor) {
                slot.last_access = now;
                slot.access_sequence = access_sequence;
            }
            (session, access_sequence)
        };
        self.schedule_expiry(cursor.to_string(), access_sequence, now + self.idle_ttl);

        let mut session_guard = session.lock().await;
        {
            let state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            let still_current = state
                .sessions
                .get(cursor)
                .is_some_and(|slot| Arc::ptr_eq(&slot.session, &session) && &slot.binding == binding);
            if !still_current {
                return Err(CURSOR_EXPIRED.to_string());
            }
        }

        let chunk = match session_guard.read_page(binding.options.page_size).await {
            Ok(chunk) => chunk,
            Err(error) => {
                self.remove_if_current(cursor, &session);
                return Err(error);
            }
        };
        drop(session_guard);

        if chunk.has_more {
            let next_cursor = Uuid::new_v4().to_string();
            let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            let access_sequence = Self::next_sequence(&mut state);
            let Some(slot) = state.sessions.get(cursor) else {
                return Err(CURSOR_EXPIRED.to_string());
            };
            if !Arc::ptr_eq(&slot.session, &session) || &slot.binding != binding {
                return Err(CURSOR_EXPIRED.to_string());
            }
            state.sessions.remove(cursor);
            let last_access = Instant::now();
            state.sessions.insert(
                next_cursor.clone(),
                SessionSlot { binding: binding.clone(), session, last_access, access_sequence },
            );
            drop(state);
            self.schedule_expiry(next_cursor.clone(), access_sequence, last_access + self.idle_ttl);
            Ok(FileListPage { entries: chunk.entries, cursor: Some(next_cursor) })
        } else {
            self.remove_if_current(cursor, &session);
            Ok(FileListPage { entries: chunk.entries, cursor: None })
        }
    }

    pub fn validate(&self, cursor: &str, binding: &ListSessionBinding) -> Result<(), String> {
        let now = Instant::now();
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        Self::purge_expired(&mut state, now, self.idle_ttl);
        let access_sequence = match state.sessions.get(cursor) {
            Some(slot) if &slot.binding == binding => {
                let access_sequence = Self::next_sequence(&mut state);
                if let Some(slot) = state.sessions.get_mut(cursor) {
                    slot.last_access = now;
                    slot.access_sequence = access_sequence;
                }
                access_sequence
            }
            Some(_) => {
                state.sessions.remove(cursor);
                return Err(CURSOR_EXPIRED.to_string());
            }
            None => return Err(CURSOR_EXPIRED.to_string()),
        };
        drop(state);
        self.schedule_expiry(cursor.to_string(), access_sequence, now + self.idle_ttl);
        Ok(())
    }

    pub fn invalidate_cursor(&self, connection_id: &str, cursor: &str) -> Result<(), String> {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        match state.sessions.get(cursor) {
            Some(slot) if slot.binding.connection_id == connection_id => {
                state.sessions.remove(cursor);
                Ok(())
            }
            Some(_) => Err(CURSOR_EXPIRED.to_string()),
            None => Ok(()),
        }
    }

    pub fn invalidate_connection(&self, connection_id: &str) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.sessions.retain(|_, slot| slot.binding.connection_id != connection_id);
        let generation = state.connection_generations.entry(connection_id.to_string()).or_default();
        *generation = generation.wrapping_add(1);
    }

    fn remove_if_current(&self, cursor: &str, session: &Arc<AsyncMutex<ListSession>>) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if state.sessions.get(cursor).is_some_and(|slot| Arc::ptr_eq(&slot.session, session)) {
            state.sessions.remove(cursor);
        }
    }

    fn schedule_expiry(&self, cursor: String, access_sequence: u64, deadline: Instant) {
        let state = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            let Some(state) = state.upgrade() else {
                return;
            };
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            if state.sessions.get(&cursor).is_some_and(|slot| slot.access_sequence == access_sequence) {
                state.sessions.remove(&cursor);
            }
        });
    }

    fn make_room(&self, state: &mut RegistryState, connection_id: &str) {
        while state.sessions.values().filter(|slot| slot.binding.connection_id == connection_id).count()
            >= self.per_connection_limit
        {
            if let Some(cursor) = Self::least_recent_cursor(state, Some(connection_id)) {
                state.sessions.remove(&cursor);
            }
        }
        while state.sessions.len() >= self.global_limit {
            if let Some(cursor) = Self::least_recent_cursor(state, None) {
                state.sessions.remove(&cursor);
            }
        }
    }

    fn least_recent_cursor(state: &RegistryState, connection_id: Option<&str>) -> Option<String> {
        state
            .sessions
            .iter()
            .filter(|(_, slot)| connection_id.is_none_or(|connection_id| slot.binding.connection_id == connection_id))
            .min_by_key(|(_, slot)| (slot.last_access, slot.access_sequence))
            .map(|(cursor, _)| cursor.clone())
    }

    fn purge_expired(state: &mut RegistryState, now: Instant, idle_ttl: Duration) {
        state
            .sessions
            .retain(|_, slot| now.checked_duration_since(slot.last_access).is_none_or(|idle| idle < idle_ttl));
    }

    fn next_sequence(state: &mut RegistryState) -> u64 {
        state.access_sequence = state.access_sequence.wrapping_add(1);
        state.access_sequence
    }

    #[cfg(test)]
    fn session_count(&self) -> usize {
        self.inner.lock().unwrap_or_else(|error| error.into_inner()).sessions.len()
    }
}

impl ListSession {
    async fn read_page(&mut self, page_size: usize) -> Result<PageChunk, String> {
        let mut entries = Vec::with_capacity(page_size);
        if let Some(entry) = self.buffered.take() {
            entries.push(entry);
        }
        while entries.len() < page_size {
            match self.stream.next().await {
                Some(Ok(entry)) => entries.push(entry),
                Some(Err(error)) => return Err(error),
                None => {
                    return Ok(PageChunk { entries, has_more: false });
                }
            }
        }
        match self.stream.next().await {
            Some(Ok(entry)) => {
                self.buffered = Some(entry);
                Ok(PageChunk { entries, has_more: true })
            }
            Some(Err(error)) => Err(error),
            None => Ok(PageChunk { entries, has_more: false }),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    fn entry(path: &str, kind: &str) -> FileEntry {
        FileEntry {
            path: path.to_string(),
            name: path.trim_end_matches('/').rsplit('/').next().unwrap_or_default().to_string(),
            kind: kind.to_string(),
            size: if kind == "file" { 42 } else { 0 },
            last_modified: Some("2026-07-24T00:00:00Z".to_string()),
        }
    }

    fn binding(connection_id: &str, revision: i64, path: &str, page_size: usize) -> ListSessionBinding {
        ListSessionBinding {
            connection_id: connection_id.to_string(),
            revision,
            path: path.to_string(),
            options: NormalizedFileListOptions { page_size },
        }
    }

    fn entries(count: usize) -> impl Stream<Item = Result<FileEntry, String>> + Send {
        stream::iter((0..count).map(|index| {
            let kind = if index % 2 == 0 { "file" } else { "directory" };
            Ok(entry(&format!("{index}{}", if kind == "directory" { "/" } else { "" }), kind))
        }))
    }

    #[test]
    fn page_size_defaults_to_200_and_is_bounded() {
        assert_eq!(FileListOptions::default().normalize().unwrap().page_size, 200);
        assert_eq!(FileListOptions { page_size: Some(1_000) }.normalize().unwrap().page_size, 1_000);
        assert!(FileListOptions { page_size: Some(0) }.normalize().is_err());
        assert!(FileListOptions { page_size: Some(1_001) }.normalize().is_err());
    }

    #[tokio::test]
    async fn lists_multiple_pages_with_single_use_opaque_cursors_and_entry_kinds() {
        let registry = ListSessionRegistry::default();
        let binding = binding("ftp-1", 3, "", 2);
        let first = registry.open(binding.clone(), registry.generation("ftp-1"), entries(5)).await.unwrap();
        assert_eq!(first.entries.len(), 2);
        assert_eq!(first.entries[0].kind, "file");
        assert_eq!(first.entries[1].kind, "directory");
        let cursor = first.cursor.expect("continuation cursor");
        assert_eq!(cursor.len(), 36);

        let second = registry.next(&cursor, &binding).await.unwrap();
        assert_eq!(second.entries.len(), 2);
        let next_cursor = second.cursor.expect("rotated continuation cursor");
        assert_ne!(next_cursor, cursor);
        assert_eq!(registry.next(&cursor, &binding).await.unwrap_err(), CURSOR_EXPIRED);
        let third = registry.next(&next_cursor, &binding).await.unwrap();
        assert_eq!(third.entries.len(), 1);
        assert!(third.cursor.is_none());
        assert_eq!(registry.next(&next_cursor, &binding).await.unwrap_err(), CURSOR_EXPIRED);
    }

    #[tokio::test]
    async fn an_empty_listing_returns_an_empty_terminal_page() {
        let registry = ListSessionRegistry::default();
        let binding = binding("ftp-1", 1, "", 200);
        let page = registry.open(binding, registry.generation("ftp-1"), entries(0)).await.unwrap();
        assert!(page.entries.is_empty());
        assert!(page.cursor.is_none());
        assert_eq!(registry.session_count(), 0);
    }

    #[tokio::test]
    async fn idle_expiry_returns_cursor_expired_instead_of_restarting() {
        let registry = ListSessionRegistry::new(Duration::from_secs(300), 10, 10);
        let binding = binding("ftp-1", 1, "", 1);
        let opened_at = Instant::now();
        let first = registry
            .open_at(binding.clone(), registry.generation("ftp-1"), Box::pin(entries(2)), Some(opened_at))
            .await
            .unwrap();
        let cursor = first.cursor.unwrap();
        let error = registry.next_at(&cursor, &binding, opened_at + Duration::from_secs(300)).await.unwrap_err();
        assert_eq!(error, CURSOR_EXPIRED);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timer_actively_drops_a_session_without_registry_traffic() {
        let registry = ListSessionRegistry::new(Duration::from_secs(60), 10, 10);
        let binding = binding("ftp-1", 1, "", 1);
        let page = registry.open(binding, registry.generation("ftp-1"), entries(2)).await.unwrap();
        assert!(page.cursor.is_some());
        assert_eq!(registry.session_count(), 1);

        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;

        assert_eq!(registry.session_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cursor_access_renews_idle_ttl_and_stale_timer_cannot_remove_it() {
        let registry = ListSessionRegistry::new(Duration::from_secs(60), 10, 10);
        let binding = binding("ftp-1", 1, "", 1);
        let page = registry.open(binding.clone(), registry.generation("ftp-1"), entries(3)).await.unwrap();
        let cursor = page.cursor.unwrap();

        tokio::time::advance(Duration::from_secs(40)).await;
        registry.validate(&cursor, &binding).unwrap();
        tokio::time::advance(Duration::from_secs(21)).await;
        tokio::task::yield_now().await;
        assert_eq!(registry.session_count(), 1);

        tokio::time::advance(Duration::from_secs(40)).await;
        tokio::task::yield_now().await;
        assert_eq!(registry.session_count(), 0);
    }

    #[tokio::test]
    async fn revision_path_and_options_are_part_of_the_cursor_binding() {
        for changed in [
            binding("ftp-1", 2, "child", 2),
            binding("ftp-1", 1, "other", 2),
            binding("ftp-1", 1, "child", 3),
            binding("ftp-2", 1, "child", 2),
        ] {
            let registry = ListSessionRegistry::default();
            let original = binding("ftp-1", 1, "child", 2);
            let page = registry.open(original, registry.generation("ftp-1"), entries(4)).await.unwrap();
            assert_eq!(registry.next(page.cursor.as_deref().unwrap(), &changed).await.unwrap_err(), CURSOR_EXPIRED);
        }
    }

    #[tokio::test]
    async fn refresh_edit_and_delete_invalidate_existing_and_in_flight_sessions() {
        let registry = ListSessionRegistry::default();
        let binding = binding("ftp-1", 1, "", 1);
        let generation = registry.generation("ftp-1");
        registry.invalidate_connection("ftp-1");
        assert_eq!(registry.open(binding.clone(), generation, entries(3)).await.unwrap_err(), CURSOR_EXPIRED);
        assert_eq!(registry.open(binding.clone(), generation, entries(0)).await.unwrap_err(), CURSOR_EXPIRED);

        let page = registry.open(binding.clone(), registry.generation("ftp-1"), entries(3)).await.unwrap();
        let cursor = page.cursor.unwrap();
        registry.invalidate_connection("ftp-1");
        assert_eq!(registry.next(&cursor, &binding).await.unwrap_err(), CURSOR_EXPIRED);
    }

    #[tokio::test]
    async fn explicit_refresh_close_makes_the_old_cursor_expired() {
        let registry = ListSessionRegistry::default();
        let binding = binding("ftp-1", 1, "", 1);
        let page = registry.open(binding.clone(), registry.generation("ftp-1"), entries(3)).await.unwrap();
        let cursor = page.cursor.unwrap();
        registry.invalidate_cursor("ftp-1", &cursor).unwrap();
        assert_eq!(registry.next(&cursor, &binding).await.unwrap_err(), CURSOR_EXPIRED);
    }

    #[tokio::test]
    async fn concurrent_next_calls_allow_only_one_consumer_for_each_cursor() {
        let registry = ListSessionRegistry::default();
        let binding = binding("ftp-1", 1, "", 1);
        let page = registry.open(binding.clone(), registry.generation("ftp-1"), entries(4)).await.unwrap();
        let cursor = page.cursor.unwrap();
        let (left, right) = tokio::join!(registry.next(&cursor, &binding), registry.next(&cursor, &binding));
        let outcomes = [left, right];
        let successful = outcomes.iter().filter(|result| result.is_ok()).count();
        let expired =
            outcomes.iter().filter(|result| result.as_ref().is_err_and(|error| error == CURSOR_EXPIRED)).count();
        assert_eq!((successful, expired), (1, 1));
        let success = outcomes.into_iter().find_map(Result::ok).unwrap();
        assert_eq!(success.entries[0].path, "1/");
        let final_page = registry.next(success.cursor.as_deref().unwrap(), &binding).await.unwrap();
        assert_eq!(final_page.entries[0].path, "2");
    }

    #[tokio::test]
    async fn per_connection_and_global_pressure_evict_the_least_recent_session() {
        let registry = ListSessionRegistry::new(Duration::from_secs(300), 3, 2);
        let mut cursors = Vec::new();
        for (connection, path) in [("a", "one"), ("a", "two"), ("a", "three")] {
            let binding = binding(connection, 1, path, 1);
            cursors.push((
                binding.clone(),
                registry.open(binding, registry.generation(connection), entries(2)).await.unwrap().cursor.unwrap(),
            ));
        }
        assert_eq!(registry.session_count(), 2);
        assert_eq!(registry.next(&cursors[0].1, &cursors[0].0).await.unwrap_err(), CURSOR_EXPIRED);

        for connection in ["b", "c"] {
            let binding = binding(connection, 1, "", 1);
            registry.open(binding, registry.generation(connection), entries(2)).await.unwrap();
        }
        assert_eq!(registry.session_count(), 3);
        assert_eq!(registry.next(&cursors[1].1, &cursors[1].0).await.unwrap_err(), CURSOR_EXPIRED);
    }

    #[tokio::test]
    async fn sustained_cursor_pressure_never_exceeds_the_global_cap() {
        let registry = ListSessionRegistry::new(Duration::from_secs(300), 32, 4);
        for index in 0..2_000 {
            let connection = format!("ftp-{}", index % 50);
            let binding = binding(&connection, 1, &index.to_string(), 1);
            let page = registry.open(binding, registry.generation(&connection), entries(2)).await.unwrap();
            assert!(page.cursor.is_some());
            assert!(registry.session_count() <= 32);
        }
        assert_eq!(registry.session_count(), 32);
    }
}

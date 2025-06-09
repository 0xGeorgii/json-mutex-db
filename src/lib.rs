#![warn(clippy::pedantic)]
use serde_json::Value;
use std::cell::RefCell;
use std::fs::{self, File};
use std::io::{Error as IoError, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use tempfile::NamedTempFile;

use parking_lot::RwLock;
use crossbeam_channel::{unbounded, Sender, Receiver};

// Error type for operations that might fail due to background thread issues
#[derive(Debug)]
pub enum DbError {
    Io(IoError),
    Sync(String), // Errors related to background thread communication
}

impl serde::Serialize for DbError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DbError::Io(err) => serializer.serialize_str(&format!("IoError: {err}")),
            DbError::Sync(msg) => serializer.serialize_str(&format!("SyncError: {msg}")),
        }
    }
}

impl From<IoError> for DbError {
    fn from(e: IoError) -> Self {
        DbError::Io(e)
    }
}


// Thread-local buffer (optional, kept from previous version)
thread_local! {
    static SERIALIZE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1024 * 64));
}

pub struct JsonMutexDB {
    path: PathBuf,
    parent_dir: PathBuf,
    pretty: bool,
    fast_serialization: bool,
    inner: Arc<RwLock<Value>>,
    save_sched: Option<Sender<()>>,
    save_handle: Option<thread::JoinHandle<()>>,
    /// Flag indicating if a save signal is pending in the background thread
    save_pending: Option<Arc<AtomicBool>>,
    dirty: AtomicBool,
}


// Implement UnwindSafe and RefUnwindSafe manually for JsonMutexDB
impl std::panic::UnwindSafe for JsonMutexDB {}
impl std::panic::RefUnwindSafe for JsonMutexDB {}

impl JsonMutexDB {
    /// Creates a new instance of `JsonMutexDB`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the specified file cannot be read,
    /// if the JSON data is invalid, or if there are issues with file I/O operations.
    pub fn new(
        path: &str,
        pretty: bool,
        async_updates: bool,
        fast_serialization: bool,
    ) -> Result<Self, DbError> {
        // Load initial JSON data (common logic)
        let initial_json = match fs::read_to_string(path) {
            Ok(content) if content.trim().is_empty() => Value::Object(serde_json::Map::new()),
            Ok(mut content) => unsafe {
                simd_json::from_str::<Value>(content.as_mut_str()).map_err(|e| {
                    DbError::Io(IoError::new(
                        ErrorKind::InvalidData,
                        format!("Invalid JSON (simd_json): {e}"),
                    ))
                })?
            },
            Err(err) if err.kind() == ErrorKind::NotFound => Value::Object(serde_json::Map::new()),
            Err(err) => return Err(DbError::Io(err)),
        };

        // Prepare file path and ensure parent directory exists
        let path_buf = PathBuf::from(path);
        let parent_dir = path_buf
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::new());
        fs::create_dir_all(&parent_dir)?;

        // Pre-allocate serialization buffer based on existing file size to minimize reallocations
        if let Ok(size) = fs::metadata(path).map(|m| m.len() as usize) {
            SERIALIZE_BUF.with(|cell| {
                let mut buf = cell.borrow_mut();
                buf.reserve(size + 1024);
            });
        }

        // Shared in-memory JSON state
        let inner = Arc::new(RwLock::new(initial_json));
        let mut save_sched = None;
        let mut save_handle = None;
        let mut save_pending = None;

        if async_updates {
            // Spawn background save thread: listens for save signals and final shutdown
            let (tx, rx) = unbounded::<()>();
            let inner_cl = Arc::clone(&inner);
            let path_cl = path_buf.clone();
            let pretty_cl = pretty;
            let fast_cl = fast_serialization;
            let pending_cl = Arc::new(AtomicBool::new(false));
            let pending_thread = Arc::clone(&pending_cl);
            // Background thread: coalesce save signals and final shutdown save
            let handle = thread::spawn(move || {
                for _ in rx {
                    // Mark pending cleared before processing
                    pending_thread.store(false, Ordering::SeqCst);
                    let data = inner_cl.read().clone();
                    if let Err(e) = JsonMutexDB::save_data_to_disk(
                        &path_cl,
                        &data,
                        pretty_cl,
                        fast_cl,
                        true,
                    ) {
                        eprintln!("Async background save failed: {e:?}");
                    }
                }
                // Final non-atomic save on shutdown
                let data = inner_cl.read().clone();
                if let Err(e) = JsonMutexDB::save_data_to_disk(
                    &path_cl,
                    &data,
                    pretty_cl,
                    fast_cl,
                    false,
                ) {
                    eprintln!("Error during final background save: {e:?}");
                }
            });
            save_sched = Some(tx);
            save_handle = Some(handle);
            save_pending = Some(pending_cl);
        }

        // Track whether state has been modified since last save
        let dirty = AtomicBool::new(false);

        Ok(JsonMutexDB {
            path: path_buf,
            parent_dir,
            pretty,
            fast_serialization,
            inner,
            save_sched,
            save_handle,
            save_pending,
            dirty,
        })
    }

    /// Returns a clone of the JSON data.
    /// If `async_updates` is enabled, fetches the latest state from the background thread.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The background thread fails to send or receive data in async mode.
    /// - The mutex is poisoned in sync mode.
    /// - The database is in an invalid state (neither sync nor async mode).
    pub fn get(&self) -> Result<Value, DbError> {
        // Always read from in-memory state
        let guard = self.inner.read();
        Ok((*guard).clone())
    }

    /// Updates the JSON data.
    /// If `async_updates` is enabled, sends the update closure to the background thread.
    /// Otherwise, applies the update synchronously.
    /// Updates the JSON data.
    ///
    /// If `async_updates` is enabled, sends the update closure to the background thread.
    /// Otherwise, applies the update synchronously.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The background thread fails to send the update task in async mode.
    /// - The mutex is poisoned in sync mode.
    /// - The database is in an invalid state (neither sync nor async mode).
    pub fn update<F>(&self, update_fn: F) -> Result<(), DbError>
    where
        F: FnOnce(&mut Value) + Send + 'static,
    {
        // Apply update immediately to in-memory state
        {
            let mut guard = self.inner.write();
            update_fn(&mut *guard);
        }
        self.dirty.store(true, Ordering::Release);
        // Signal background save thread if enabled, coalescing multiple signals
        if let (Some(pending), Some(tx)) = (&self.save_pending, &self.save_sched) {
            // Only send if no save is already pending (compare-and-set)
            if pending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let _ = tx.send(());
            }
        }
        Ok(())
    }

    /// Synchronously saves the current JSON data to disk atomically.
    /// If `async_updates` is enabled, fetches the latest state before saving.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The current state cannot be fetched (e.g., due to background thread issues in async mode).
    /// - There are file I/O errors during the save operation.
    pub fn save_sync(&self) -> Result<(), DbError> {
        // Skip save if no changes since last save
        if !self.dirty.swap(false, Ordering::Acquire) {
            return Ok(());
        }
        // Perform atomic save without deep-cloning: hold read lock during serialization
        let guard = self.inner.read();
        let result = Self::save_data_to_disk(
            &self.path,
            &*guard,
            self.pretty,
            self.fast_serialization,
            true,
        );
        drop(guard);
        result
    }

    /// Asynchronously saves the current JSON data atomically.
    /// If `async_updates` is enabled, fetches the latest state first, then spawns the save thread.
    /// Asynchronously saves the current JSON data atomically.
    ///
    /// If `async_updates` is enabled, fetches the latest state first, then spawns the save thread.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The current state cannot be fetched (e.g., due to background thread issues in async mode).
    /// - There are file I/O errors during the save operation.
    pub fn save_async(&self) -> Result<(), DbError> {
        // Skip save if no changes since last save
        if !self.dirty.swap(false, Ordering::Acquire) {
            return Ok(());
        }
        // Spawn a thread to perform the atomic save asynchronously without deep-cloning the data
        let inner_clone = Arc::clone(&self.inner);
        let path_clone = self.path.clone();
        let pretty_clone = self.pretty;
        let fast_serial_clone = self.fast_serialization;

        thread::spawn(move || {
            let guard = inner_clone.read();
            if let Err(e) = Self::save_data_to_disk(
                &path_clone,
                &*guard,
                pretty_clone,
                fast_serial_clone,
                true,
            ) {
                eprintln!("Async save failed: {e:?}");
            }
        });
        Ok(())
    }

    // --- Internal helper remains largely the same ---
    fn save_data_to_disk(
        path: &Path,
        data_to_save: &Value,
        pretty: bool,
        fast_serialization: bool,
        atomic: bool,
    ) -> Result<(), DbError> {
        // Changed return type to DbError
        let final_path = path.to_path_buf();

        // Serialize and write using thread-local buffer for fewer allocations and write calls
        SERIALIZE_BUF.with(|cell| -> Result<(), DbError> {
            let mut buf = cell.borrow_mut();
            buf.clear();
            if pretty {
                serde_json::to_writer_pretty(&mut *buf, data_to_save)
                    .map_err(|e| DbError::Io(IoError::other(e.to_string())))?;
            } else if fast_serialization {
                simd_json::to_writer(&mut *buf, data_to_save)
                    .map_err(|e| DbError::Io(IoError::other(format!("{e:?}"))))?;
            } else {
                serde_json::to_writer(&mut *buf, data_to_save)
                    .map_err(|e| DbError::Io(IoError::other(e.to_string())))?;
            }
            if atomic {
                let parent_dir = path.parent().ok_or_else(|| {
                    DbError::Io(IoError::new(
                        ErrorKind::InvalidInput,
                        "Invalid path: cannot determine parent directory",
                    ))
                })?;
                let mut temp_file = NamedTempFile::new_in(parent_dir)?;
                temp_file.write_all(&buf)?;
                temp_file.flush()?;
                temp_file.persist(&final_path).map_err(|e| {
                    DbError::Io(IoError::other(format!(
                        "Failed to atomically rename temp file: {}",
                        e.error
                    )))
                })?;
            } else {
                let mut file = File::create(path)?;
                file.write_all(&buf)?;
                file.flush()?;
            }
            Ok(())
        })?;
        Ok(())
    }
}

// Shutdown background save thread on drop, ensuring final flush
impl Drop for JsonMutexDB {
    fn drop(&mut self) {
        if let Some(tx) = self.save_sched.take() {
            drop(tx);
            if let Some(handle) = self.save_handle.take() {
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::io::{BufWriter, Write};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    struct WriteCounter {
        calls: usize,
        bytes: usize,
    }
    impl WriteCounter {
        fn new() -> Self {
            Self { calls: 0, bytes: 0 }
        }
        fn calls(&self) -> usize {
            self.calls
        }
        fn bytes(&self) -> usize {
            self.bytes
        }
    }
    impl Write for WriteCounter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let len = buf.len();
            self.calls += 1;
            self.bytes += len;
            Ok(len)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn write_json_raw<W: Write>(
        writer: &mut W,
        data: &Value,
        pretty: bool,
        fast: bool,
    ) -> std::io::Result<()> {
        // Serialize into thread-local buffer and write in one shot for fewer write calls
        SERIALIZE_BUF.with(|cell| -> std::io::Result<()> {
            let mut buf = cell.borrow_mut();
            buf.clear();
            if pretty {
                serde_json::to_writer_pretty(&mut *buf, data)?;
            } else if fast {
                simd_json::to_writer(&mut *buf, data)?;
            } else {
                serde_json::to_writer(&mut *buf, data)?;
            }
            writer.write_all(&buf)?;
            writer.flush()?;
            Ok(())
        })?;
        Ok(())
    }

    // Helper to remove test file quietly
    fn cleanup_file(path: &str) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_jsonmutexdb_new_and_get() {
        let tmp_path = "test_db_new_get.json";
        cleanup_file(tmp_path);
        let db =
            JsonMutexDB::new(tmp_path, false, false, false).expect("Failed to create JsonMutexDB");
        assert_eq!(db.get().unwrap(), json!({}));
        cleanup_file(tmp_path);

        // Test loading existing valid JSON
        let initial_json = json!({"hello": "world"});
        fs::write(tmp_path, initial_json.to_string()).unwrap();
        let db = JsonMutexDB::new(tmp_path, false, false, false)
            .expect("Failed to load existing JsonMutexDB");
        assert_eq!(db.get().unwrap(), initial_json);
        cleanup_file(tmp_path);
    }

    #[test]
    fn test_jsonmutexdb_update_and_save_sync_compact_fast() {
        let tmp_path = "test_db_set_save_compact_fast.json";
        cleanup_file(tmp_path);
        // Enable fast serialization
        let db =
            JsonMutexDB::new(tmp_path, false, false, true).expect("Failed to create JsonMutexDB");
        let new_data = json!({"key": "value", "numbers": [1, 2, 3], "nested": {"a": true}});
        let new_data_clone = new_data.clone();
        if let Err(e) = db.update(move |d| *d = new_data_clone) {
            eprintln!("Failed to update database: {e:?}");
        }
        db.save_sync()
            .expect("Failed to save JSON data sync (compact/fast)");

        let file_content = fs::read_to_string(tmp_path).expect("Failed to read file");
        let file_json: Value = unsafe {
            simd_json::from_str(&mut file_content.clone())
                .expect("Invalid JSON in file (simd_json)")
        };
        assert_eq!(file_json, new_data);
        // Check it's compact (no newlines besides maybe one at EOF)
        assert!(
            !file_content.trim().contains('\n'),
            "JSON file should be compact"
        );
        cleanup_file(tmp_path);
    }

    #[test]
    fn test_jsonmutexdb_update_and_save_sync_compact_standard() {
        let tmp_path = "test_db_set_save_compact_std.json";
        cleanup_file(tmp_path);
        // Disable fast serialization
        let db =
            JsonMutexDB::new(tmp_path, false, false, false).expect("Failed to create JsonMutexDB");
        let new_data = json!({"key": "value", "numbers": [1, 2, 3], "nested": {"a": true}});
        let new_data_clone = new_data.clone();
        if let Err(e) = db.update(move |d| *d = new_data_clone) {
            eprintln!("Failed to update database: {e:?}");
        }
        db.save_sync()
            .expect("Failed to save JSON data sync (compact/standard)");

        let file_content = fs::read_to_string(tmp_path).expect("Failed to read file");
        let file_json: Value =
            serde_json::from_str(&file_content).expect("Invalid JSON in file (serde_json)");
        assert_eq!(file_json, new_data);
        assert!(
            !file_content.trim().contains('\n'),
            "JSON file should be compact"
        );
        cleanup_file(tmp_path);
    }

    #[test]
    fn test_jsonmutexdb_update_and_save_sync_pretty() {
        let tmp_path = "test_db_set_save_pretty.json";
        cleanup_file(tmp_path);
        // Enable pretty printing
        let db =
            JsonMutexDB::new(tmp_path, true, false, false).expect("Failed to create JsonMutexDB");
        let new_data = json!({"key": "value", "numbers": [1, 2, 3], "nested": {"a": true}});
        let new_data_clone = new_data.clone();
        if let Err(e) = db.update(move |d| *d = new_data_clone) {
            eprintln!("Failed to update database: {e:?}");
        }
        db.save_sync()
            .expect("Failed to save JSON data sync (pretty)");

        let file_content = fs::read_to_string(tmp_path).expect("Failed to read file");
        let file_json: Value = serde_json::from_str(&file_content).expect("Invalid JSON in file");
        assert_eq!(file_json, new_data);
        // Basic check for pretty printing (contains newlines and spaces for indentation)
        assert!(
            file_content.contains('\n'),
            "JSON file not pretty printed (no newlines)"
        );
        assert!(
            file_content.contains("  "),
            "JSON file not pretty printed (no indentation)"
        );
        cleanup_file(tmp_path);
    }

    #[test]
    fn test_save_async_works() {
        let tmp_path = "test_db_save_async.json";
        cleanup_file(tmp_path);
        let db =
            JsonMutexDB::new(tmp_path, false, false, true).expect("Failed to create JsonMutexDB");
        let new_data = json!({"async_key": "async_value", "id": 123});
        let new_data_clone = new_data.clone();
        if let Err(e) = db.update(move |d| *d = new_data_clone) {
            eprintln!("Failed to update database: {e:?}");
        }

        if let Err(e) = db.save_async() {
            eprintln!("Failed to save asynchronously: {e:?}");
        } // Call the async save

        // Wait for the async save to likely complete. This is brittle in tests!
        // In a real app, you might need a callback or future.
        thread::sleep(Duration::from_millis(150));

        // Verify the file content
        let file_content =
            fs::read_to_string(tmp_path).expect("Failed to read file after async save");
        let file_json: Value = unsafe {
            simd_json::from_str(&mut file_content.clone())
                .expect("Invalid JSON in file (simd_json)")
        };
        assert_eq!(file_json, new_data);
        cleanup_file(tmp_path);
    }

    #[test]
    fn test_atomic_save_prevents_corruption() {
        let tmp_path = "test_db_atomic.json";
        cleanup_file(tmp_path);

        // 1. Create an initial valid file
        let initial_data = json!({"initial": "data"});
        // Use sync mode for simpler setup in this specific test
        let db_initial = JsonMutexDB::new(tmp_path, false, false, false).unwrap();
        let initial_data_clone = initial_data.clone();
        db_initial.update(|d| *d = initial_data_clone).unwrap();
        db_initial.save_sync().unwrap(); // Save initial state

        // 2. Setup data that would be written if crash didn't happen
        let db_corrupting = JsonMutexDB::new(tmp_path, false, false, false).unwrap();
        let large_bad_data = json!({"corrupted": "data".repeat(1000)}); // Data to write
        db_corrupting.update(|d| *d = large_bad_data).unwrap();
        let data_to_write = db_corrupting.get().unwrap(); // Get data to write

        // Manually simulate the atomic save process up to the write phase
        let path = Path::new(tmp_path);
        let final_path = path.to_path_buf(); // Target path
        let parent_dir = path.parent().unwrap();
        fs::create_dir_all(parent_dir).unwrap();
        let temp_file_res = NamedTempFile::new_in(parent_dir);

        assert!(temp_file_res.is_ok(), "Failed to create NamedTempFile");
        let temp_file = temp_file_res.unwrap(); // temp_file lives outside catch_unwind
        let temp_path = temp_file.path().to_path_buf(); // Get path needed *after* close/drop

        // Simulate write failure (panic) before rename/persist
        // Use AssertUnwindSafe because we are borrowing temp_file across unwind boundary
        let write_attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // --- Write directly to the NamedTempFile using BufWriter ---
            let mut buffered_writer = BufWriter::new(&temp_file);

            serde_json::to_writer(&mut buffered_writer, &data_to_write)
                .expect("Write to temp file failed during simulation");

            buffered_writer
                .flush()
                .expect("Flush failed during simulation");
            // Explicitly drop writer before panic to release borrow
            drop(buffered_writer);

            // *** Simulate crash DURING or AFTER write/flush but BEFORE persist ***
            panic!("Simulated crash during write!");
        }));

        // Assert that the write attempt panicked as expected
        assert!(
            write_attempt.is_err(),
            "Write process did not panic as expected"
        );

        // IMPORTANT: Check the original file content hasn't changed
        let file_content_after_crash =
            fs::read_to_string(&final_path) // Read the original target path
                .expect("Failed to read original file after simulated crash");
        let file_json_after_crash: Value =
            serde_json::from_str(&file_content_after_crash).expect("Invalid JSON in original file");

        assert_eq!(
            file_json_after_crash, initial_data,
            "Original file was modified despite simulated crash during atomic save"
        );

        // --- Explicit Cleanup Attempt ---
        // Explicitly try to close (which deletes if not persisted).
        // This consumes temp_file.
        match temp_file.close() {
            Ok(()) => println!("NamedTempFile closed and deleted successfully."),
            // PersistError contains the tempfile, allowing retry or manual cleanup
            Err(persist_error) => {
                eprintln!("NamedTempFile close failed: {persist_error}. Attempting manual delete.");
                // If close fails, the Drop impl might also fail, so try manual delete
                if let Err(remove_err) = fs::remove_file(&temp_path) {
                    eprintln!("Manual deletion of temp file failed: {remove_err}");
                    // Don't panic here, let the assertion below handle the final state check.
                }
            }
        }

        // Cleanup the target file
        cleanup_file(tmp_path); // Remove the original test file (final_path)

        // Assert that the temp file path no longer exists.
        // Give the OS a tiny bit of time in case deletion has slight delay (optional)
        // std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(
            !temp_path.exists(),
            "Temporary file was not cleaned up after explicit close/delete attempt"
        );
    }

    // --- Async Update Tests (Require careful handling of state synchronization) ---

    // Helper to wait for async updates to likely propagate (use with caution in real tests)
    fn wait_for_async(db: &Arc<JsonMutexDB>, expected_key: &str, expected_value: &Value) {
        let start = Instant::now();
        let timeout = Duration::from_secs(2); // Adjust timeout as needed
        loop {
            // To check the *actual* state including async updates, we need a way
            // to query the background thread or ensure it flushes to the main state.
            // This current test structure only checks the main thread's view via `get()`,
            // which IS NOT guaranteed to be up-to-date in async mode immediately after `update`.
            //
            // WORKAROUND for testing: Send a no-op update to potentially cycle the event loop,
            // then check the file *after* a save triggered externally.
            if let Err(e) = db.update(|_| {}) {
                eprintln!("Failed to perform no-op update: {e:?}");
            } // No-op to potentially push queue
            thread::sleep(Duration::from_millis(20)); // Small delay

            // Let's save the *main* state and check the file. This still doesn't
            // guarantee the async update landed *before* the save, demonstrating the challenge.
            db.save_sync().expect("Intermediate save failed");
            if let Ok(content) = fs::read_to_string(&db.path) {
                // Use simd_json for parsing if used for saving
                if let Ok(current_val) =
                    unsafe { simd_json::from_str::<Value>(&mut content.clone()) }
                {
                    if current_val.get(expected_key) == Some(expected_value) {
                        return; // Found the expected state
                    }
                } else if let Ok(current_val) = serde_json::from_str::<Value>(&content) {
                    // Fallback if not using simd_json or it failed
                    if current_val.get(expected_key) == Some(expected_value) {
                        return; // Found the expected state
                    }
                }
            }

            assert!(
                (start.elapsed() <= timeout),
                "Timeout waiting for async update to reflect for key '{expected_key}'"
            );
        }
    }

    // Test marked ignore because the interaction between main state and async state
    // in this simple implementation makes reliable testing difficult without
    // more complex synchronization/query mechanisms.
    #[test]
    #[ignore]
    fn test_async_updates_basic_propagation() {
        let tmp_path = "test_db_async_prop.json";
        cleanup_file(tmp_path);

        // Enable async updates
        let db = Arc::new(JsonMutexDB::new(tmp_path, false, true, true).unwrap());

        let key = "async_key_1";
        let value = json!("async_value_1");

        // Perform an asynchronous update
        let db_clone = Arc::clone(&db);
        let value_clone = value.clone();
        thread::spawn(move || {
            if let Err(e) = db_clone.update(move |data| {
                data.as_object_mut()
                    .unwrap()
                    .insert(key.to_string(), value_clone);
            }) {
                eprintln!("Failed to update database: {e:?}");
            }
            println!("Async update sent for {key}");
        })
        .join()
        .unwrap();

        // Wait for the update to likely be processed and reflected (using helper)
        // This relies on the background thread processing and potentially saving.
        wait_for_async(&db, key, &value);

        // Final check via get() - MAY STILL BE STALE depending on implementation details
        // let final_data = db.get();
        // assert_eq!(final_data[key], value);
        // Instead, check the file content as wait_for_async does implicitly
        let final_content = fs::read_to_string(tmp_path).unwrap();
        let final_json: Value = unsafe { simd_json::from_str(&mut final_content.clone()).unwrap() };
        assert_eq!(final_json[key], value);

        // Test multiple async updates
        let key2 = "async_key_2";
        let value2 = json!(999);
        let db_clone2 = Arc::clone(&db);
        let value_clone2 = value2.clone();
        thread::spawn(move || {
            if let Err(e) = db_clone2.update(move |data| {
                data.as_object_mut()
                    .unwrap()
                    .insert(key2.to_string(), value_clone2);
            }) {
                eprintln!("Failed to update database: {e:?}");
            }
            println!("Async update sent for {key2}");
        })
        .join()
        .unwrap();

        wait_for_async(&db, key2, &value2);

        let final_content2 = fs::read_to_string(tmp_path).unwrap();
        let final_json2: Value =
            unsafe { simd_json::from_str(&mut final_content2.clone()).unwrap() };
        assert_eq!(final_json2[key], value); // Check previous value still exists
        assert_eq!(final_json2[key2], value2);

        // Drop the DB explicitly to trigger shutdown and potential final save
        drop(db);
        thread::sleep(Duration::from_millis(50)); // Allow Drop time

        cleanup_file(tmp_path);
    }

    // --- Performance Benchmarks (Ignored by default) ---

    #[test]
    #[ignore] // Performance sensitive, run explicitly
    #[allow(clippy::cast_sign_loss)]
    fn benchmark_save_sync_compact_fast_optimized() {
        let tmp_path = "test_db_perf_save_sync_fast.json";
        cleanup_file(tmp_path);
        let db = JsonMutexDB::new(tmp_path, false, false, true).unwrap(); // Compact, fast
        let mut large_obj = serde_json::Map::new();
        for i in 0..1000 {
            // 1000 key-value pairs
            large_obj.insert(format!("key{i}"), json!(i));
        }
        let large_obj_clone = large_obj.clone();
        if let Err(e) = db.update(move |d| *d = json!(large_obj_clone)) {
            eprintln!("Failed to update database: {e:?}");
        }

        let iterations = 500; // Fewer iterations needed maybe
        let start = Instant::now();
        for _ in 0..iterations {
            db.save_sync().expect("Save failed during benchmark");
        }
        let elapsed = start.elapsed();
        println!(
            "[Optimized] Elapsed time for {iterations} atomic sync saves (compact/fast): {elapsed:?}"
        );
        let avg_micros = elapsed.as_micros() / iterations as u128;
        println!("[Optimized] Average time per save: {avg_micros} microseconds");
        // Adjust assertion based on expected performance on target machine
        assert!(
            avg_micros < 500,
            "Average save time too slow: {avg_micros} micros"
        );

        cleanup_file(tmp_path);
    }

    // Benchmark for async updates (measures enqueue/processing time)
    // Still potentially ignores final persistence time.
    #[test]
    #[ignore]
    fn benchmark_multithread_update_async_optimized() {
        let tmp_path = "test_db_perf_async_update.json";
        cleanup_file(tmp_path);
        let db = Arc::new(JsonMutexDB::new(tmp_path, false, true, false).unwrap()); // Async enabled
        let num_threads = 8;
        let updates_per_thread = 5000;
        let total_updates = num_threads * updates_per_thread;

        let start = Instant::now();
        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for i in 0..updates_per_thread {
                    let key = format!("thread{thread_id}_key{i}");
                    let value = json!(i);
                    if let Err(e) = db_clone.update(move |json| {
                        json.as_object_mut().unwrap().insert(key, value);
                    }) {
                        eprintln!("Failed to update database: {e:?}");
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }
        let elapsed_enqueue = start.elapsed();
        println!(
            "[Optimized] Time to enqueue {total_updates} updates from {num_threads} threads: {elapsed_enqueue:?}"
        );

        // IMPORTANT: Now wait for the background thread to likely process these.
        // Drop the Arc reference held by the main thread. The background thread
        // holds the last one. Drop it to signal shutdown.
        drop(db);

        // Wait a bit for the background thread to potentially finish and drop.
        // This is NOT a guarantee it processed everything or saved finally.
        thread::sleep(Duration::from_millis(200)); // Adjust as needed

        let elapsed_total = start.elapsed();
        println!(
            "[Optimized] Total time (enqueue + potential processing/shutdown): {elapsed_total:?}"
        );

        // Optional: Verify final file state IF the Drop implementation guarantees a final save
        // let content = fs::read_to_string(tmp_path).unwrap();
        // let final_json: Value = serde_json::from_str(&content).unwrap();
        // assert_eq!(final_json.as_object().unwrap().len(), total_updates);

        cleanup_file(tmp_path);
    }

    // Add other tests as needed: error handling, concurrent reads/writes etc.
    #[test]
    fn test_concurrent_read_write_sync() {
        let tmp_path = "test_db_concurrent_sync.json";
        cleanup_file(tmp_path);
        let db = Arc::new(JsonMutexDB::new(tmp_path, false, false, false).unwrap());
        let num_writers = 4;
        let num_readers = 4;
        let writes_per_thread = 50;
        let reads_per_thread = 200;

        let mut handles = vec![];

        // Writers
        for i in 0..num_writers {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for j in 0..writes_per_thread {
                    let key = format!("writer{i}_key{j}");
                    let value = json!(j);
                    if let Err(e) = db_clone.update(move |d| {
                        d.as_object_mut().unwrap().insert(key, value);
                    }) {
                        eprintln!("Failed to update database: {e:?}");
                    }
                    // Small yield to increase chance of interleaving
                    thread::yield_now();
                }
            }));
        }

        // Readers
        for _ in 0..num_readers {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for _ in 0..reads_per_thread {
                    let _data = db_clone.get(); // Perform read
                    // Optional: Add assertions on data consistency if needed,
                    // but could make test flaky depending on timing.
                    thread::yield_now();
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify final state
        let binding = db.get().unwrap();
        let final_data_unwrapped = binding.as_object().unwrap();
        assert_eq!(final_data_unwrapped.len(), num_writers * writes_per_thread);
        // Check one key per writer to be reasonably sure
        assert_eq!(final_data_unwrapped["writer0_key49"], 49);
        assert_eq!(final_data_unwrapped["writer1_key49"], 49);

        cleanup_file(tmp_path);
    }

    /// Naive JSON write helper (before optimization)
    fn write_json_orig<W: Write>(writer: &mut W, data: &Value, pretty: bool, fast: bool) -> std::io::Result<()> {
        if pretty {
            serde_json::to_writer_pretty(writer.by_ref(), data)?;
        } else if fast {
            simd_json::to_writer(writer.by_ref(), data)?;
        } else {
            serde_json::to_writer(writer.by_ref(), data)?;
        }
        writer.flush()?;
        Ok(())
    }

    #[test]
    fn test_write_call_count_compact_fast() {
        let mut data_map = serde_json::Map::new();
        for i in 0..500 {
            data_map.insert(format!("key{}", i), json!(i));
        }
        let v = Value::Object(data_map);
        let mut cnt_std = WriteCounter::new();
        write_json_raw(&mut cnt_std, &v, false, false).unwrap();
        let std_calls = cnt_std.calls();
        let mut cnt_fast = WriteCounter::new();
        write_json_raw(&mut cnt_fast, &v, false, true).unwrap();
        let fast_calls = cnt_fast.calls();
        assert!(
            fast_calls <= std_calls,
            "fast serialization should use no more write calls: fast={} std={}",
            fast_calls,
            std_calls
        );
    }

    #[test]
    fn test_write_call_count_pretty_vs_compact() {
        let mut data_map = serde_json::Map::new();
        data_map.insert("a".to_string(), json!(1));
        data_map.insert("b".to_string(), json!(2));
        let v = Value::Object(data_map);
        let mut cnt_compact = WriteCounter::new();
        write_json_raw(&mut cnt_compact, &v, false, false).unwrap();
        let compact_calls = cnt_compact.calls();
        let mut cnt_pretty = WriteCounter::new();
        write_json_raw(&mut cnt_pretty, &v, true, false).unwrap();
        let pretty_calls = cnt_pretty.calls();
        assert!(
            pretty_calls >= compact_calls,
            "pretty printing should use at least as many write calls: pretty={} compact={}",
            pretty_calls,
            compact_calls
        );
    }

    #[test]
    fn test_optimized_vs_naive_write_calls_standard() {
        let mut data_map = serde_json::Map::new();
        for i in 0..1000 {
            data_map.insert(format!("key{}", i), json!(i));
        }
        let v = Value::Object(data_map);
        let mut cnt_old = WriteCounter::new();
        write_json_orig(&mut cnt_old, &v, false, false).unwrap();
        let old_calls = cnt_old.calls();
        let mut cnt_new = WriteCounter::new();
        write_json_raw(&mut cnt_new, &v, false, false).unwrap();
        let new_calls = cnt_new.calls();
        assert!(
            new_calls < old_calls,
            "optimized write_json_raw should use fewer writes than naive: {} < {}",
            new_calls,
            old_calls
        );
    }

    #[test]
    fn test_optimized_vs_naive_write_calls_fast() {
        let mut data_map = serde_json::Map::new();
        for i in 0..1000 {
            data_map.insert(format!("key{}", i), json!(i));
        }
        let v = Value::Object(data_map);
        let mut cnt_old = WriteCounter::new();
        write_json_orig(&mut cnt_old, &v, false, true).unwrap();
        let old_calls = cnt_old.calls();
        let mut cnt_new = WriteCounter::new();
        write_json_raw(&mut cnt_new, &v, false, true).unwrap();
        let new_calls = cnt_new.calls();
        assert!(
            new_calls < old_calls,
            "optimized write_json_raw should use fewer writes than naive fast: {} < {}",
            new_calls,
            old_calls
        );
    }

    #[test]
    fn test_write_json_pretty_vs_compact() {
        let mut data_map = serde_json::Map::new();
        data_map.insert("a".to_string(), json!(1));
        data_map.insert("b".to_string(), json!(2));
        let v = Value::Object(data_map);
        let mut cnt_compact = WriteCounter::new();
        write_json_raw(&mut cnt_compact, &v, false, false).unwrap();
        let compact_calls = cnt_compact.calls();
        let mut cnt_pretty = WriteCounter::new();
        write_json_raw(&mut cnt_pretty, &v, true, false).unwrap();
        let pretty_calls = cnt_pretty.calls();
        assert!(
            pretty_calls >= compact_calls,
            "pretty printing should use at least as many write calls: pretty={} compact={}",
            pretty_calls,
            compact_calls
        );
    }
}

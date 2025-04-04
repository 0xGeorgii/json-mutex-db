#![warn(clippy::pedantic)]
use serde_json::Value;
use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Error as IoError, ErrorKind, Write};
use std::path::Path;
// Use std::sync::Mutex for RefUnwindSafe compatibility if needed, or stick to parking_lot otherwise
use std::sync::Mutex; // Switched back in previous user code
use std::thread;
use tempfile::NamedTempFile;

// Use crossbeam_channel for select! and unbounded/bounded channels
use crossbeam_channel::{Receiver, RecvError, SendError, Sender, bounded, select, unbounded};

// --- Communication types for async mode ---

// Command sent from main thread to background thread
enum BackgroundCommand {
    // Request the current state, providing a channel to send the response back
    GetState(Sender<Value>),
    // Shut down the background thread gracefully
    Shutdown,
}

// An update closure sent via a separate channel
type UpdateTask = Box<dyn FnOnce(&mut Value) + Send>;

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

// Helper for channel send errors
impl<T> From<SendError<T>> for DbError {
    fn from(e: SendError<T>) -> Self {
        DbError::Sync(format!("Failed to send command to background thread: {e}"))
    }
}

// Helper for channel receive errors
impl From<RecvError> for DbError {
    fn from(e: RecvError) -> Self {
        DbError::Sync(format!(
            "Failed to receive response from background thread: {e}"
        ))
    }
}

// Thread-local buffer (optional, kept from previous version)
thread_local! {
    static SERIALIZE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1024 * 64));
}

pub struct JsonMutexDB {
    // Shared state for both sync and async modes
    path: String,
    pretty: bool,
    fast_serialization: bool,

    // Mode-specific state
    sync_data: Option<Mutex<Value>>, // Only used when async_updates is false
    async_comm: Option<AsyncCommunicator>, // Only used when async_updates is true
}

// Holds channels and thread handle for async mode
struct AsyncCommunicator {
    command_tx: Sender<BackgroundCommand>,
    update_tx: Sender<UpdateTask>,
    update_handle: Option<thread::JoinHandle<()>>, // Option to allow taking during drop
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

        let mut sync_data = None;
        let mut async_comm = None;

        if async_updates {
            // --- Async Mode Setup ---
            let (command_tx, command_rx) = unbounded::<BackgroundCommand>();
            let (update_tx, update_rx) = unbounded::<UpdateTask>();

            // Clone necessary info for the background thread
            let bg_initial_data = initial_json; // No Mutex needed here
            let bg_path = path.to_string();
            let bg_pretty = pretty;
            let bg_fast_serialization = fast_serialization;

            let update_handle = thread::spawn(move || {
                background_thread_loop(
                    bg_initial_data,
                    &bg_path,
                    bg_pretty,
                    bg_fast_serialization,
                    &command_rx,
                    &update_rx,
                );
            });

            async_comm = Some(AsyncCommunicator {
                command_tx,
                update_tx,
                update_handle: Some(update_handle),
            });
        } else {
            // --- Sync Mode Setup ---
            sync_data = Some(Mutex::new(initial_json));
        }

        Ok(JsonMutexDB {
            path: path.to_string(),
            pretty,
            fast_serialization,
            sync_data,
            async_comm,
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
        if let Some(comm) = &self.async_comm {
            // --- Async Mode: Request state from background ---
            // Create a temporary channel for the response
            let (response_tx, response_rx) = bounded(1); // Size 1 for one-shot behavior
            comm.command_tx
                .send(BackgroundCommand::GetState(response_tx))?;
            // Block waiting for the response
            let value = response_rx.recv()?;
            Ok(value)
        } else if let Some(mutex) = &self.sync_data {
            // --- Sync Mode: Lock local mutex ---
            let guard = mutex
                .lock()
                .map_err(|_| DbError::Sync("Mutex poisoned".to_string()))?;
            Ok(guard.clone())
            // Handle potential poisoning if using std::sync::Mutex
            // match mutex.lock() {
            //     Ok(guard) => Ok(guard.clone()),
            //     Err(poisoned) => Ok(poisoned.into_inner().clone()), // Or return DbError::Sync
            // }
        } else {
            Err(DbError::Sync(
                "DB is in an invalid state (neither sync nor async)".to_string(),
            ))
        }
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
        if let Some(comm) = &self.async_comm {
            // --- Async Mode: Send update task ---
            comm.update_tx.send(Box::new(update_fn))?;
            Ok(())
        } else if let Some(mutex) = &self.sync_data {
            // --- Sync Mode: Lock and apply ---
            let mut guard = mutex
                .lock()
                .map_err(|_| DbError::Sync("Mutex poisoned".to_string()))?;
            update_fn(&mut guard);
            Ok(())
            // Handle potential poisoning
            // match mutex.lock() {
            //     Ok(mut guard) => {
            //         update_fn(&mut guard);
            //         Ok(())
            //     }
            //     Err(_) => Err(DbError::Sync("Mutex poisoned during update".to_string())),
            // }
        } else {
            Err(DbError::Sync(
                "DB is in an invalid state (neither sync nor async)".to_string(),
            ))
        }
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
        // 1. Get the current state (fetches from background if async)
        let data_to_save = self.get()?;

        // 2. Use the optimized internal save function with atomic=true
        Self::save_data_to_disk(
            &self.path,
            &data_to_save,
            self.pretty,
            self.fast_serialization,
            true, // Atomic save
        )
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
        // 1. Get the current state (fetches from background if async)
        // This block ensures we get the state *before* spawning the thread.
        let data_clone = self.get()?;

        // Clone necessary data for the background thread
        let path_clone = self.path.clone();
        let pretty_clone = self.pretty;
        let fast_serial_clone = self.fast_serialization;

        // 2. Spawn a thread to perform the save using the optimized helper
        thread::spawn(move || {
            if let Err(e) = Self::save_data_to_disk(
                &path_clone,
                &data_clone,
                pretty_clone,
                fast_serial_clone,
                true, // Perform atomic save in background thread
            ) {
                // Consider more robust error reporting than just stderr
                eprintln!("Async save failed: {e:?}");
            }
        });
        Ok(())
    }

    // --- Internal helper remains largely the same ---
    fn save_data_to_disk(
        path_str: &str,
        data_to_save: &Value,
        pretty: bool,
        fast_serialization: bool,
        atomic: bool,
    ) -> Result<(), DbError> {
        // Changed return type to DbError
        let path = Path::new(path_str);
        let final_path = path.to_path_buf();

        let write_logic = |writer: Box<dyn Write>| -> Result<(), DbError> {
            let mut buffered_writer = BufWriter::new(writer);
            if pretty {
                serde_json::to_writer_pretty(&mut buffered_writer, data_to_save)
                    .map_err(|e| DbError::Io(IoError::other(e.to_string())))?;
            } else if fast_serialization {
                simd_json::to_writer(&mut buffered_writer, data_to_save)
                    .map_err(|e| DbError::Io(IoError::other(format!("{e:?}"))))?;
            } else {
                serde_json::to_writer(&mut buffered_writer, data_to_save)
                    .map_err(|e| DbError::Io(IoError::other(e.to_string())))?;
            }
            buffered_writer.flush()?; // Ensure buffer is flushed
            Ok(())
        };

        if atomic {
            let parent_dir = path.parent().ok_or_else(|| {
                DbError::Io(IoError::new(
                    ErrorKind::InvalidInput,
                    "Invalid path: cannot determine parent directory",
                ))
            })?;
            fs::create_dir_all(parent_dir)?; // Ensure parent dir exists

            // Create temp file
            let temp_file = NamedTempFile::new_in(parent_dir)?;
            let temp_path = temp_file.path().to_path_buf(); // Keep path

            // Write to temp file (using explicit file handle for Box<dyn Write>)
            let file = OpenOptions::new().write(true).open(&temp_path)?;
            write_logic(Box::new(file))?; // Write happens here

            // Persist atomically (consumes temp_file)
            temp_file.persist(&final_path).map_err(|e| {
                DbError::Io(IoError::other(format!(
                    "Failed to atomically rename temp file: {}",
                    e.error
                )))
            })?;
        } else {
            // Non-atomic: Create/truncate target file directly
            let file = File::create(path)?;
            write_logic(Box::new(file))?;
        }

        Ok(())
    }
}

// --- Background Thread Logic ---
fn background_thread_loop(
    mut local_data: Value,
    path: &str,
    pretty: bool,
    fast_serialization: bool,
    command_rx: &Receiver<BackgroundCommand>,
    update_rx: &Receiver<UpdateTask>,
) {
    println!("Background thread started.");
    loop {
        select! {
            // Received an update task
            recv(update_rx) -> msg => {
                if let Ok(update_fn) = msg {
                    // Apply the update to the local state
                    update_fn(&mut local_data);
                    // Optional: log update application
                } else {
                    // Update channel closed, probably shutting down.
                    println!("Update channel closed.");
                    break; // Exit loop
                }
            },

            // Received a command (GetState or Shutdown)
            recv(command_rx) -> msg => {
                match msg {
                    Ok(BackgroundCommand::GetState(response_tx)) => {
                        // Clone current state and send it back
                        let _ = response_tx.send(local_data.clone()); // Ignore error if main thread hung up
                    }
                     Ok(BackgroundCommand::Shutdown) => {
                         println!("Received Shutdown command.");
                         break; // Exit loop
                     }
                    Err(_) => {
                        // Command channel closed, main thread likely dropped.
                        println!("Command channel closed.");
                        break; // Exit loop
                    }
                }
            }
        }
    }

    // --- Shutdown sequence ---
    println!("Background thread shutting down. Performing final save...");
    // Perform a final non-atomic save of the last known state
    if let Err(e) = JsonMutexDB::save_data_to_disk(
        path,
        &local_data,
        pretty,
        fast_serialization,
        false, // Non-atomic during this final shutdown save
    ) {
        eprintln!("Error during final background save: {e:?}");
    }
    println!("Background thread finished.");
}

impl Drop for JsonMutexDB {
    fn drop(&mut self) {
        if let Some(mut comm) = self.async_comm.take() {
            println!("Dropping JsonMutexDB (async)...");
            // 1. Signal background thread to shut down (optional, closing channels might suffice)
            let _ = comm.command_tx.send(BackgroundCommand::Shutdown);

            // 2. Drop senders - this will cause receivers in background to error/terminate select! loop
            drop(comm.command_tx);
            drop(comm.update_tx);

            // 3. Wait for the background thread to finish
            if let Some(handle) = comm.update_handle.take() {
                match handle.join() {
                    Ok(()) => println!("Background thread joined cleanly."),
                    Err(e) => eprintln!("Background thread panicked: {e:?}"),
                }
            }
        } else {
            println!("Dropping JsonMutexDB (sync)...");
            // Optional: Save synchronously if in sync mode and desired
            // if let Some(mutex) = &self.sync_data {
            //     match mutex.lock() {
            //         Ok(guard) => {
            //             if let Err(e) = Self::save_data_to_disk(&self.path, &guard, self.pretty, self.fast_serialization, true) {
            //                  eprintln!("Error during final sync save on drop: {:?}", e);
            //             }
            //         },
            //         Err(_) => eprintln!("Mutex poisoned during drop, could not save."),
            //     }
            // }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant}; // Added Duration

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
}

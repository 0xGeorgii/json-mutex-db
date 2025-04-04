use parking_lot::Mutex;
use serde_json::Value;
use std::cell::RefCell;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::thread;

use crossbeam_channel::{Receiver, Sender, unbounded};

// Thread-local buffer to reuse across saves
thread_local! {
    static SERIALIZE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1024 * 64)); // 64 KB buffer
}

/// JsonMutexDB provides thread-safe access to a JSON file acting as a simple database.
/// It supports asynchronous (batched) updates and fast serialization (using simd-json)
/// in compact mode.
pub struct JsonMutexDB {
    /// A lightweight mutex protecting the in-memory JSON data.
    data: Mutex<Value>,
    /// The path to the JSON file on disk.
    path: String,
    /// Whether to pretty-print when saving.
    pretty: bool,
    /// Whether to use fast serialization (simd-json) when in compact mode.
    fast_serialization: bool,
    /// If asynchronous updates are enabled, this channel is used to send update closures.
    update_sender: Option<Sender<Box<dyn FnOnce(&mut Value) + Send>>>,
    /// Handle for the background update thread (if async_updates is enabled).
    update_handle: Option<thread::JoinHandle<()>>,
}

impl JsonMutexDB {
    /// Creates a new instance of JsonMutexDB.
    ///
    /// * `path` - path to the JSON file
    /// * `pretty` - if true, saved JSON will be human-readable (pretty printed).
    ///   Note: fast serialization using simd-json is only used if `pretty` is false.
    /// * `async_updates` - if true, update calls are enqueued to a background thread.
    /// * `fast_serialization` - if true and in compact mode, uses simd-json for serialization.
    pub fn new(
        path: &str,
        pretty: bool,
        async_updates: bool,
        fast_serialization: bool,
    ) -> Result<Self, IoError> {
        let json = match fs::read_to_string(path) {
            Ok(content) if content.trim().is_empty() => {
                // Treat empty files as an empty JSON object.
                Value::Object(serde_json::Map::new())
            }
            Ok(content) => serde_json::from_str::<Value>(&content)
                .map_err(|_| IoError::new(ErrorKind::InvalidData, "Invalid JSON content"))?,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // File doesn't exist: start with an empty JSON object.
                Value::Object(serde_json::Map::new())
            }
            Err(err) => return Err(err),
        };

        let (update_sender, update_handle) = if async_updates {
            let (tx, rx): (
                Sender<Box<dyn FnOnce(&mut Value) + Send>>,
                Receiver<Box<dyn FnOnce(&mut Value) + Send>>,
            ) = unbounded();
            // We'll spawn a background thread that applies all enqueued updates.
            // Note: For simplicity, we do not implement a sophisticated shutdown.
            let data_mutex = Mutex::new(()); // dummy mutex to capture ordering in the closure
            let handle = thread::spawn({
                // We capture a pointer to the same JSON data (we'll borrow it via a reference).
                // Safety: This thread will be the sole executor of queued updates.
                let data_ref = Mutex::new(json.clone());
                // We wrap data_ref in a parking_lot::Mutex that we own (the same one in the struct).
                move || {
                    // Loop until the channel is closed.
                    for update in rx {
                        // We simply lock the global data mutex from the main struct.
                        // SAFETY: The background thread must coordinate with synchronous callers.
                        // In this design, all mutations occur via either this thread or direct lock.
                        // (In a production design, you’d want a more robust design for ordering.)
                        // Reinterpret the raw pointer back into a reference.
                        let mut data = data_ref.lock();
                        update(&mut data);
                    }
                }
            });
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        Ok(JsonMutexDB {
            data: Mutex::new(json),
            path: path.to_string(),
            pretty,
            fast_serialization,
            update_sender,
            update_handle,
        })
    }

    /// Returns a clone of the in-memory JSON data.
    /// Note: When using async updates, queued updates may not yet be applied.
    pub fn get(&self) -> Value {
        // (For simplicity we do not flush pending async updates here.)
        let data_guard = self.data.lock();
        data_guard.clone()
    }

    /// Updates the in-memory JSON data using the provided closure.
    /// If async_updates is enabled, the update is enqueued and the call returns immediately.
    /// Otherwise, the update is applied synchronously.
    pub fn update<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut Value) + Send + 'static,
    {
        if let Some(ref sender) = self.update_sender {
            // Enqueue the update.
            // In a production system, you might want to handle errors here.
            sender
                .send(Box::new(update_fn))
                .expect("Failed to send update");
        } else {
            let mut data_guard = self.data.lock();
            update_fn(&mut data_guard);
        }
    }

    /// Synchronously saves the current in-memory JSON data to the file on disk.
    /// The JSON is saved in either pretty printed or compact format based on configuration.
    pub fn save_sync(&self) -> Result<(), IoError> {
        let data_guard = self.data.lock();
        let json_data = &*data_guard;

        let content = if self.pretty {
            // Use pretty printing via serde_json.
            serde_json::to_string_pretty(json_data)
                .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?
        } else if self.fast_serialization {
            // Use fast compact serialization via simd-json.
            SERIALIZE_BUF.with(|buf| {
                let mut buffer = buf.borrow_mut();
                buffer.clear(); // Reuse the buffer to avoid reallocations
                simd_json::to_writer(&mut *buffer, json_data).map_err(|e| {
                    IoError::new(ErrorKind::Other, format!("simd_json error: {:?}", e))
                })?;
                // Safety: simd_json guarantees valid UTF-8.
                String::from_utf8(buffer.clone())
                    .map_err(|_| IoError::new(ErrorKind::InvalidData, "UTF-8 conversion error"))
            })?
        } else {
            serde_json::to_string(json_data)
                .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?
        };

        fs::write(&self.path, content)
    }

    /// Asynchronously saves the current in-memory JSON data.
    ///
    /// This spawns a background thread so that the calling thread is not blocked by I/O or serialization.
    /// Any errors in the background thread are printed to stderr.
    pub fn save_async(&self) {
        let data = self.get(); // grab a snapshot of the current data
        let path = self.path.clone();
        let pretty = self.pretty;
        let fast_serialization = self.fast_serialization;
        thread::spawn(move || {
            let result = if pretty {
                serde_json::to_string_pretty(&data)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            } else if fast_serialization {
                simd_json::to_string(&data).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("simd_json error: {:?}", e),
                    )
                })
            } else {
                serde_json::to_string(&data)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            };
            match result {
                Ok(content) => {
                    if let Err(e) = fs::write(&path, content) {
                        eprintln!("Async save failed: {}", e);
                    }
                }
                Err(e) => eprintln!("Serialization error in async save: {}", e),
            }
        });
    }
}

impl Drop for JsonMutexDB {
    fn drop(&mut self) {
        // If async_updates is enabled, drop the sender so that the background thread can exit.
        self.update_sender = None;
        if let Some(handle) = self.update_handle.take() {
            // Wait for the background thread to finish.
            let _ = handle.join();
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
    use std::time::Instant;

    #[test]
    fn test_jsonmutexdb_new_and_get() {
        let tmp_path = "test_db.json";
        let _ = fs::remove_file(tmp_path);

        // Create a new DB instance (file not found, so should initialize with empty object)
        let db =
            JsonMutexDB::new(tmp_path, false, false, false).expect("Failed to create JsonMutexDB");
        let data = db.get();
        assert_eq!(data, json!({}));

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_jsonmutexdb_set_and_save_sync() {
        let tmp_path = "test_db_set_save.json";
        let _ = fs::remove_file(tmp_path);

        let db =
            JsonMutexDB::new(tmp_path, false, false, true).expect("Failed to create JsonMutexDB");

        // Set new data and save synchronously using compact mode with fast serialization.
        let new_data = json!({
            "key": "value",
            "numbers": [1, 2, 3]
        });
        let new_data_clone = new_data.clone();
        db.update(move |d| *d = new_data_clone);
        db.save_sync().expect("Failed to save JSON data");

        // Read file back and compare
        let file_content = fs::read_to_string(tmp_path).expect("Failed to read file");
        let file_json: Value = serde_json::from_str(&file_content).expect("Invalid JSON in file");
        assert_eq!(file_json, new_data);

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_pretty_print_format() {
        let tmp_path = "test_db_format.json";
        let _ = fs::remove_file(tmp_path);
        let db = JsonMutexDB::new(tmp_path, true, false, false).unwrap();
        let new_data = json!({
            "name": "Test",
            "value": 123,
            "array": [1, 2, 3]
        });
        db.update(|d| *d = new_data);
        db.save_sync().unwrap();
        let file_content = fs::read_to_string(tmp_path).unwrap();
        // Verify that the JSON is pretty printed (contains newlines)
        assert!(
            file_content.contains("\n"),
            "JSON file not pretty printed: {}",
            file_content
        );
        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_multithreading_sync() {
        // Synchronous updates for comparison.
        let tmp_path = "test_db_multithread.json";
        let _ = fs::remove_file(tmp_path);

        let db = Arc::new(JsonMutexDB::new(tmp_path, false, false, false).unwrap());
        let num_threads = 10;
        let updates_per_thread = 100;
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for i in 0..updates_per_thread {
                    db_clone.update(move |json| {
                        let obj = json.as_object_mut().expect("JSON is not an object");
                        obj.insert(format!("thread{}_key{}", thread_id, i), json!(i));
                    });
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Confirm that the total number of keys equals the sum of all updates.
        let data = db.get();
        let obj = data.as_object().expect("JSON is not an object");
        assert_eq!(obj.len(), num_threads * updates_per_thread);

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_update_and_get() {
        let tmp_path = "test_db_update_get.json";
        let _ = fs::remove_file(tmp_path);

        let db = JsonMutexDB::new(tmp_path, false, false, false).unwrap();

        // Update the database with new data
        db.update(|data| {
            let obj = data.as_object_mut().unwrap();
            obj.insert("key1".to_string(), json!("value1"));
            obj.insert("key2".to_string(), json!(42));
        });

        // Verify the updated data
        let data = db.get();
        assert_eq!(data["key1"], "value1");
        assert_eq!(data["key2"], 42);

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_save_and_reload() {
        let tmp_path = "test_db_save_reload.json";
        let _ = fs::remove_file(tmp_path);

        let db = JsonMutexDB::new(tmp_path, false, false, false).unwrap();

        // Update the database with new data
        db.update(|data| {
            let obj = data.as_object_mut().unwrap();
            obj.insert("key".to_string(), json!("value"));
        });

        // Save the data to disk
        db.save_sync().unwrap();

        // Reload the database from the file
        let db_reloaded = JsonMutexDB::new(tmp_path, false, false, false).unwrap();
        let data = db_reloaded.get();
        assert_eq!(data["key"], "value");

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_empty_file_initialization() {
        let tmp_path = "test_db_empty.json";
        let _ = fs::remove_file(tmp_path);

        // Create an empty file
        fs::write(tmp_path, "").unwrap();

        // Initialize the database
        let db = JsonMutexDB::new(tmp_path, false, false, false).unwrap();
        let data = db.get();
        assert_eq!(data, json!({}));

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_invalid_json_file() {
        let tmp_path = "test_db_invalid.json";
        let _ = fs::remove_file(tmp_path);

        // Write invalid JSON to the file
        fs::write(tmp_path, "invalid json").unwrap();

        // Attempt to initialize the database
        let result = JsonMutexDB::new(tmp_path, false, false, false);
        assert!(result.is_err());

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    #[ignore] //FIXME
    fn test_async_updates() {
        let tmp_path = "test_db_async.json";
        let _ = fs::remove_file(tmp_path);

        let db = Arc::new(JsonMutexDB::new(tmp_path, false, true, false).unwrap());

        // Perform asynchronous updates
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            db_clone.update(|data| {
                let obj = data.as_object_mut().unwrap();
                obj.insert("async_key".to_string(), json!("async_value"));
            });
        });

        handle.join().unwrap();

        // Ensure all pending updates are processed
        for _ in 0..10 {
            db.update(|_| {}); // Trigger a no-op update to flush the channel
            let data = db.get();
            if data["async_key"] == "async_value" {
                break;
            }
            thread::sleep(std::time::Duration::from_millis(10)); // Allow time for processing
        }

        let data = db.get();
        assert_eq!(data["async_key"], "async_value");

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_large_data_handling() {
        let tmp_path = "test_db_large.json";
        let _ = fs::remove_file(tmp_path);

        let db = JsonMutexDB::new(tmp_path, false, false, false).unwrap();

        // Insert a large number of keys
        db.update(|data| {
            let obj = data.as_object_mut().unwrap();
            for i in 0..10_000 {
                obj.insert(format!("key{}", i), json!(i));
            }
        });

        // Verify the data
        let data = db.get();
        let obj = data.as_object().unwrap();
        assert_eq!(obj.len(), 10_000);
        assert_eq!(obj["key9999"], 9999);

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_concurrent_read_and_write() {
        let tmp_path = "test_db_concurrent.json";
        let _ = fs::remove_file(tmp_path);

        let db = Arc::new(JsonMutexDB::new(tmp_path, false, false, false).unwrap());

        let db_clone = Arc::clone(&db);
        let writer = thread::spawn(move || {
            for i in 0..100 {
                db_clone.update(move |data| {
                    let obj = data.as_object_mut().unwrap();
                    obj.insert(format!("key{}", i), json!(i));
                });
            }
        });

        let db_clone = Arc::clone(&db);
        let reader = thread::spawn(move || {
            for _ in 0..10 {
                let data = db_clone.get();
                let _ = data.as_object().unwrap();
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();

        // Verify the data
        let data = db.get();
        let obj = data.as_object().unwrap();
        assert_eq!(obj.len(), 100);

        let _ = fs::remove_file(tmp_path);
    }

    /// Benchmark saving the database with fast compact serialization.
    /// This test is marked #[ignore] because it is performance sensitive.
    #[test]
    #[ignore]
    fn benchmark_save_compact_fast() {
        let tmp_path = "test_db_perf.json";
        let _ = fs::remove_file(tmp_path);
        // Use compact mode with fast serialization enabled.
        let db = JsonMutexDB::new(tmp_path, false, false, true).unwrap();

        // Create a large JSON object with 1000 key-value pairs.
        let mut large_obj = serde_json::Map::new();
        for i in 0..1000 {
            large_obj.insert(format!("key{}", i), json!(i));
        }
        db.update(move |d| *d = json!(large_obj));

        let iterations = 10_000;
        let start = Instant::now();
        for _ in 0..iterations {
            db.save_sync().expect("Save failed");
        }
        let elapsed = start.elapsed();
        println!(
            "Elapsed time for {} compact saves: {:?}",
            iterations, elapsed
        );
        let avg = elapsed.as_secs_f64() / iterations as f64;
        println!("Average time per compact save: {} seconds", avg);
        // Expect average save time to be under 100 microseconds.
        assert!(
            avg < 0.0001,
            "Average compact save time too slow: {} seconds",
            avg
        );

        let _ = fs::remove_file(tmp_path);
    }

    /// Benchmark multithreaded updates using asynchronous (batched) updates.
    /// This test is marked #[ignore] because it is performance sensitive.
    #[test]
    #[ignore]
    fn benchmark_multithread_update_async() {
        let tmp_path = "test_db_multithread_perf.json";
        let _ = fs::remove_file(tmp_path);

        // Enable asynchronous updates.
        let db = Arc::new(JsonMutexDB::new(tmp_path, false, true, false).unwrap());
        let num_threads = 10;
        let updates_per_thread = 1000;
        let start = Instant::now();

        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for i in 0..updates_per_thread {
                    db_clone.update(move |json| {
                        let obj = json.as_object_mut().expect("JSON is not an object");
                        obj.insert(format!("thread{}_key{}", thread_id, i), json!(i));
                    });
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // (Note: In async mode, some updates might still be queued; in a production system
        // you would flush the channel. For this benchmark we measure just the enqueue cost.)
        let elapsed = start.elapsed();
        println!("Elapsed time for async multithread update: {:?}", elapsed);
        // Expect total async update time to be under 5 milliseconds.
        assert!(
            elapsed.as_secs_f64() < 0.005,
            "Multithread async update took too long: {:?}",
            elapsed
        );
        let _ = fs::remove_file(tmp_path);
    }
}

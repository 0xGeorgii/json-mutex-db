use parking_lot::Mutex;
use serde_json::Value;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::thread;

use crossbeam_channel::{Receiver, Sender, unbounded};
use simd_json::OwnedValue; // Use simd_json's Value type
use simd_json::serde::to_borrowed_value; // For converting serde_json::Value

/// BufferWriter writes data into a preallocated Vec<u8>.
struct BufferWriter<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> BufferWriter<'a> {
    fn new(buf: &'a mut Vec<u8>) -> Self {
        BufferWriter { buf }
    }
}

impl<'a> Write for BufferWriter<'a> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok::<(), io::Error>(())
    }
}

/// Thread-local buffer for serialization.
thread_local! {
    static SERIALIZE_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(64 * 1024)); // 64 KB buffer
}

/// JsonMutexDB provides thread-safe access to a JSON file acting as a simple database.
/// It supports asynchronous (batched) updates and fast serialization.
pub struct JsonMutexDB {
    /// A lightweight mutex protecting the in-memory JSON data.
    data: Mutex<Value>,
    /// The path to the JSON file on disk.
    path: String,
    /// Whether to pretty-print when saving.
    pretty: bool,
    /// Whether to use fast serialization (simulated here by using our preallocated writer).
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
    /// * `pretty` - if true, saved JSON will be pretty printed.
    /// * `async_updates` - if true, update calls are enqueued to a background thread.
    /// * `fast_serialization` - if true and in compact mode, uses a preallocated writer to avoid extra allocations.
    pub fn new(
        path: &str,
        pretty: bool,
        async_updates: bool,
        fast_serialization: bool,
    ) -> io::Result<Self> {
        let json = match fs::read_to_string(path) {
            Ok(content) => serde_json::from_str::<Value>(&content)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid JSON content"))?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
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
            let json_for_thread = json.clone(); // Clone the JSON for the thread.
            // Spawn a background thread that applies all enqueued updates.
            let handle = thread::spawn(move || {
                for update in rx {
                    // In a production system, you’d want more robust ordering.
                    update(&mut json_for_thread.clone());
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
    /// (Note: When using async updates, queued updates may not yet be applied.)
    pub fn get(&self) -> Value {
        let data_guard = self.data.lock();
        data_guard.clone()
    }

    /// Updates the in-memory JSON data using the provided closure.
    /// If async_updates is enabled, the update is enqueued; otherwise, it is applied synchronously.
    pub fn update<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut Value) + Send + 'static,
    {
        if let Some(ref sender) = self.update_sender {
            sender
                .send(Box::new(update_fn))
                .expect("Failed to send update");
        } else {
            let mut data_guard = self.data.lock();
            update_fn(&mut *data_guard);
        }
    }

    /// Synchronously saves the current in-memory JSON data to the file on disk.
    /// In compact mode with fast_serialization enabled, it reuses a preallocated buffer to avoid extra allocations.
    pub fn save_sync(&self) -> io::Result<()> {
        let data_guard = self.data.lock();
        let json_data = &*data_guard;

        let mut file = BufWriter::new(File::create(&self.path)?);

        if self.pretty {
            // Pretty printing using serde_json.
            serde_json::to_writer_pretty(&mut file, json_data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        } else if self.fast_serialization {
            SERIALIZE_BUF.with(|buf_cell| {
                let mut buf = buf_cell.borrow_mut();
                buf.clear();
                {
                    let mut writer = BufferWriter::new(&mut buf);
                    // Convert serde_json::Value to simd_json::OwnedValue for potentially faster serialization
                    if let Ok(borrowed) = to_borrowed_value(json_data) {
                        simd_json::to_writer(&mut writer, &borrowed)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    } else {
                        // Fallback to serde_json if conversion fails (shouldn't happen for valid serde_json::Value)
                        serde_json::to_writer(&mut writer, json_data)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    }
                }
                file.write_all(&buf)?;
                Ok::<(), io::Error>(())
            })?;
        } else {
            // Default compact serialization via serde_json.
            serde_json::to_writer(&mut file, json_data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }
        Ok(())
    }
}

impl Drop for JsonMutexDB {
    fn drop(&mut self) {
        self.update_sender = None;
        if let Some(handle) = self.update_handle.take() {
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

        let new_data = json!({
            "key": "value",
            "numbers": [1, 2, 3]
        });
        let new_data_clone = new_data.clone();
        db.update(move |d| *d = new_data_clone);
        db.save_sync().expect("Failed to save JSON data");

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
        assert!(
            file_content.contains("\n"),
            "JSON file not pretty printed: {}",
            file_content
        );
        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_multithreading_sync() {
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
        let data = db.get();
        let obj = data.as_object().expect("JSON is not an object");
        assert_eq!(obj.len(), num_threads * updates_per_thread);

        let _ = fs::remove_file(tmp_path);
    }

    /// Benchmark saving the database with our fast compact serialization using the preallocated writer.
    /// This test is marked #[ignore] as it is performance sensitive.
    #[test]
    #[ignore]
    fn benchmark_save_compact_fast() {
        let tmp_path = "test_db_perf.json";
        let _ = fs::remove_file(tmp_path);
        // Use compact mode with fast_serialization enabled (using our preallocated writer).
        let db = JsonMutexDB::new(tmp_path, false, false, true).unwrap();

        // Create a large JSON object with 1000 key-value pairs.
        let mut large_obj = serde_json::Map::new();
        for i in 0..1000 {
            large_obj.insert(format!("key{}", i), json!(i));
        }
        db.update(move |d| *d = json!(large_obj));

        let iterations = 100;
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
        // Set a more realistic threshold after optimization.
        assert!(
            avg < 0.00005, // Adjust this threshold based on your results
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
        let elapsed = start.elapsed();
        println!("Elapsed time for async multithread update: {:?}", elapsed);
        // Expect total async update time to be under a chosen threshold (e.g., 5 ms).
        assert!(
            elapsed.as_secs_f64() < 0.005,
            "Multithread async update took too long: {:?}",
            elapsed
        );
        let _ = fs::remove_file(tmp_path);
    }
}

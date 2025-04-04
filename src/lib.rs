use parking_lot::Mutex;
use serde_json::Value;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::thread;

/// JsonMutexDB provides thread-safe access to a JSON file acting as a simple database.
/// It can operate in either “pretty” mode (human-readable) or “compact” mode for speed.
pub struct JsonMutexDB {
    /// A lightweight Mutex protecting the in-memory JSON data.
    data: Mutex<Value>,
    /// The path to the JSON file on disk.
    path: String,
    /// Whether to pretty-print when saving.
    pretty: bool,
}

impl JsonMutexDB {
    /// Creates a new instance of JsonMutexDB.
    ///
    /// If the file at `path` exists and contains valid JSON, it will be loaded.
    /// Otherwise, an empty JSON object is created and used.
    /// The `pretty` flag indicates whether to pretty print when saving (true) or use a compact format (false).
    pub fn new(path: &str, pretty: bool) -> Result<Self, IoError> {
        let json = match fs::read_to_string(path) {
            Ok(content) => serde_json::from_str::<Value>(&content)
                .map_err(|_| IoError::new(ErrorKind::InvalidData, "Invalid JSON content"))?,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // File doesn't exist: start with an empty JSON object.
                Value::Object(serde_json::Map::new())
            }
            Err(err) => return Err(err),
        };

        Ok(JsonMutexDB {
            data: Mutex::new(json),
            path: path.to_string(),
            pretty,
        })
    }

    /// Returns a clone of the in-memory JSON data.
    pub fn get(&self) -> Value {
        let data_guard = self.data.lock();
        data_guard.clone()
    }

    /// Replaces the in-memory JSON data with `new_data`.
    pub fn set(&self, new_data: Value) {
        let mut data_guard = self.data.lock();
        *data_guard = new_data;
    }

    /// Updates the in-memory JSON data using the provided closure.
    ///
    /// This allows for modifying the data without replacing it completely.
    pub fn update<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut Value),
    {
        let mut data_guard = self.data.lock();
        update_fn(&mut data_guard);
    }

    /// Synchronously saves the current in-memory JSON data to the file on disk.
    ///
    /// The JSON is saved in either pretty printed or compact format based on configuration.
    pub fn save_sync(&self) -> Result<(), IoError> {
        let data_guard = self.data.lock();
        let content = if self.pretty {
            serde_json::to_string_pretty(&*data_guard)
        } else {
            serde_json::to_string(&*data_guard)
        }
        .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?;
        fs::write(&self.path, content)
    }

    /// Asynchronously saves the current in-memory JSON data.
    ///
    /// This spawns a background thread so that the calling thread is not blocked by I/O or serialization.
    /// Note: Any errors in the background thread are printed to stderr.
    pub fn save_async(&self) {
        let data = self.get(); // grab a snapshot of the current data
        let path = self.path.clone();
        let pretty = self.pretty;
        thread::spawn(move || {
            let result = if pretty {
                serde_json::to_string_pretty(&data)
            } else {
                serde_json::to_string(&data)
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
        let db = JsonMutexDB::new(tmp_path, false).expect("Failed to create JsonMutexDB");
        let data = db.get();
        assert_eq!(data, json!({}));

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn test_jsonmutexdb_set_and_save_sync() {
        let tmp_path = "test_db_set_save.json";
        let _ = fs::remove_file(tmp_path);

        let db = JsonMutexDB::new(tmp_path, false).expect("Failed to create JsonMutexDB");

        // Set new data and save synchronously using the compact mode.
        let new_data = json!({
            "key": "value",
            "numbers": [1, 2, 3]
        });
        db.set(new_data.clone());
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
        let db = JsonMutexDB::new(tmp_path, true).unwrap();
        let new_data = json!({
            "name": "Test",
            "value": 123,
            "array": [1, 2, 3]
        });
        db.set(new_data);
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
    fn test_multithreading() {
        let tmp_path = "test_db_multithread.json";
        let _ = fs::remove_file(tmp_path);

        let db = Arc::new(JsonMutexDB::new(tmp_path, false).unwrap());
        let num_threads = 10;
        let updates_per_thread = 100;
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for i in 0..updates_per_thread {
                    db_clone.update(|json| {
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
    fn benchmark_save_compact() {
        let tmp_path = "test_db_perf.json";
        let _ = fs::remove_file(tmp_path);
        // Use compact mode (fast mode)
        let db = JsonMutexDB::new(tmp_path, false).unwrap();

        // Create a large JSON object with 1000 key-value pairs.
        let mut large_obj = serde_json::Map::new();
        for i in 0..1000 {
            large_obj.insert(format!("key{}", i), json!(i));
        }
        db.set(json!(large_obj));

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
        // Expect a dramatic speed-up relative to pretty printing.
        assert!(
            avg < 0.00001,
            "Average compact save time too slow: {} seconds",
            avg
        );

        let _ = fs::remove_file(tmp_path);
    }

    #[test]
    fn benchmark_multithread_update_fast() {
        let tmp_path = "test_db_multithread_perf.json";
        let _ = fs::remove_file(tmp_path);

        let db = Arc::new(JsonMutexDB::new(tmp_path, false).unwrap());
        let num_threads = 10;
        let updates_per_thread = 1000;
        let start = Instant::now();

        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for i in 0..updates_per_thread {
                    db_clone.update(|json| {
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
        println!(
            "Elapsed time for multithread update (fast mode): {:?}",
            elapsed
        );
        // In compact mode with parking_lot, we expect very fast updates.
        assert!(
            elapsed.as_secs_f64() < 0.001,
            "Multithread update took too long: {:?}",
            elapsed
        );
        let _ = fs::remove_file(tmp_path);
    }
}

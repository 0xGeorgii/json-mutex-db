[![Build](https://github.com/Inferara/inf-wasm-tools/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/Inferara/inf-wasm-tools/actions/workflows/build.yml)
![Crates.io Version](https://img.shields.io/crates/v/json-mutex-db?label=json-mutex-db)
![Crates.io Total Downloads](https://img.shields.io/crates/d/json-mutex-db)

# JsonMutexDB 💾

Ridiculously simple, fast and thread safe JSON file database

Ever found yourself needing a *really* simple way to persist some state in a Rust application? Maybe a configuration file, some user preferences, or the results of that *one* calculation you don't want to run again? And did you also need multiple threads to poke at that state without setting your data on fire? 🔥

`JsonMutexDB` was born out of a desire for a straightforward, thread-safe mechanism to manage data stored in a single JSON file. It doesn't try to be a full-fledged database, but it's pretty handy for those "I just need to save this `struct` somewhere" moments.

## What's Inside? ✨

* **Thread-Safe Access:** Uses `std::sync::Mutex` (or potentially `parking_lot::Mutex` depending on historical versions) under the hood, allowing multiple threads to safely read (`get`) and write (`update`) data.
* **JSON Persistence:** Reads from and saves data to a JSON file you specify. Handles empty or non-existent files gracefully on startup.
* **Atomic Saves:** Writes are performed atomically by default (using `tempfile` and rename) to prevent data corruption if your application crashes mid-save. Safety first!
* **Serialization Options:**
    * Save JSON in a compact format (default) or human-readable "pretty" format.
    * Optionally use `simd-json` for potentially faster serialization when saving in compact mode. Speed boost! 🚀
* **Optional Asynchronous Updates:** For scenarios where you don't want your main threads blocked by updates, you can enable `async_updates`. Updates are sent to a dedicated background thread for processing.
    * **State Synchronization:** When async mode is enabled, `get()` and `save_sync()` intelligently query the background thread to ensure they operate on the *absolute latest* state. (This involves some channel communication overhead).
* **Asynchronous Saving:** Offload the potentially slow file I/O of saving to a background thread with `save_async()`.

## Quick Start 🚀

```bash
cargo add json-mutex-db
```

or add the following line to your `Cargo.toml`:

```toml
json-mutex-db = "0.0.2"
```

And get going in your code:

```rust
use json_mutex_db::{JsonMutexDB, DbError};
use serde::{Serialize, Deserialize};
use serde_json::json;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Config {
    api_key: Option<String>,
    retries: u32,
}

fn main() -> Result<(), DbError> {
    let db_path = "my_app_state.json";
    // Create DB (sync mode, compact, standard serialization)
    let db = JsonMutexDB::new(db_path, false, false, false)?;

    // Get initial data (starts empty if file doesn't exist)
    let initial_val = db.get()?;
    println!("Initial value: {}", initial_val);

    // Update the data - replace the whole value
    let initial_config = Config { api_key: None, retries: 3 };
    db.update(move |data| {
        *data = serde_json::to_value(&initial_config).unwrap();
    })?;

    // Update part of the data (if it's an object)
    db.update(move |data| {
        if let Some(obj) = data.as_object_mut() {
            obj.insert("retries".to_string(), json!(5));
            obj.insert("new_feature_enabled".to_string(), json!(true));
        }
    })?;

    // Get the current state
    let current_val = db.get()?;
    println!("Current value: {}", current_val);

    // Try to deserialize it back
    let current_config: Config = serde_json::from_value(current_val.clone())
        .expect("Failed to deserialize");
    assert_eq!(current_config.retries, 5);

    // Save it synchronously (atomic by default)
    db.save_sync()?;

    // Cleanup the file for the example
    std::fs::remove_file(db_path).ok();

    Ok(())
}
```

## Configuration Options (new) ⚙️

When creating a JsonMutexDB, you have a few choices:

```rust,ignore
pub fn new(
    path: &str,            // Path to the JSON file
    pretty: bool,          // `true` for pretty-printed JSON, `false` for compact
    async_updates: bool,   // `true` to enable background thread for updates
    fast_serialization: bool, // `true` to use simd-json for compact serialization (if `pretty` is false)
) -> Result<Self, DbError>
```
* `path`: The path to the JSON file. If it doesn't exist, it will be created.
* `pretty`: If true, the JSON will be saved in a human-readable format. If false, it will be compact. This affects both `save_sync()` and `save_async()`.
* `async_updates`: If true, updates are sent to a background thread. This allows the main thread to continue without waiting for the update to complete. If false, updates are synchronous and block until completed.
* `fast_serialization`: If true and pretty is false, uses the `simd-json` crate for faster serialization. This is only effective when saving in compact mode.

## Examples 🧐

### Async Updates

```rust
use json_mutex_db::{JsonMutexDB, DbError};
use serde_json::json;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), DbError> {
    let db_path = "async_example.json";
    // Enable async updates, use fast compact saving
    let db = Arc::new(JsonMutexDB::new(db_path, false, true, true)?);

    let db_clone = Arc::clone(&db);
    thread::spawn(move || {
        println!("Background thread updating...");
        db_clone.update(|data| {
            let obj = data.as_object_mut().unwrap();
            obj.insert("worker_id".to_string(), json!(123));
            obj.insert("status".to_string(), json!("running"));
        }).expect("Failed to send update");
        println!("Background thread update sent.");
    });

    // Give the background thread a moment to process
    thread::sleep(Duration::from_millis(50));

    // Get the latest state (will block briefly to query background thread)
    let current_state = db.get()?;
    println!("State after async update: {}", current_state);
    assert_eq!(current_state["status"], "running");

    db.save_sync()?; // Save the state fetched from background
    println!("Async state saved.");

    // Required: Drop the Arc to signal background thread shutdown before cleanup
    drop(db);
    thread::sleep(Duration::from_millis(50)); // Allow time for shutdown/final save

    std::fs::remove_file(db_path).ok();

    Ok(())
}
```

### Async Saving

```rust
use json_mutex_db::{JsonMutexDB, DbError};
use serde_json::json;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), DbError> {
    let db_path = "async_save_example.json";
    // Sync updates, pretty printing
    let db = JsonMutexDB::new(db_path, true, false, false)?;

    db.update(|d| *d = json!({"message": "Hello from async save!"}))?;

    println!("Triggering async save...");
    db.save_async()?; // Returns immediately

    println!("Main thread doing other work...");
    thread::sleep(Duration::from_millis(100));

    println!("Checking file...");
    let content = std::fs::read_to_string(db_path)?;
    println!("File content:\n{}", content);
    assert!(content.contains("  \"message\":")); // Check for pretty printing

    std::fs::remove_file(db_path).ok();

    Ok(())
}
```

## Performance Notes ⚡️

* Atomic Sync Saves (`save_sync`): No longer deep-clones the JSON data to avoid extra allocations and copying. Instead, holds a read lock during serialization into a thread-local buffer, reducing memory operations at the cost of blocking concurrent updates during the save.
* Asynchronous Saves (`save_async`): No longer deep-clones the JSON data; holds a read lock during serialization into a thread-local buffer in the background thread, reducing memory operations at the cost of blocking concurrent updates until the save completes.
* Serialization Buffer: The thread-local buffer is pre-allocated based on initial file size to minimize reallocations.
* I/O: Saves serialize into a thread-local in-memory buffer and issue a single `write_all` + `flush`, drastically reducing the number of write syscalls. Atomic saves still involve writing to a temporary file and renaming.
* Async Updates: Updates are non-blocking and queued to a background thread. Multiple rapid updates are coalesced into a single disk write, reducing redundant I/O.

## Error Handling ⚠️

Most operations return `Result<_, DbError>`. This enum covers:
* `DbError::Io(std::io::Error)`: Filesystem errors, invalid JSON loading, serialization errors.
* `DbError::Sync(String)`: Errors related to the async background thread communication (channel errors, poisoned mutexes in sync mode).
Match on the result or use ? to propagate errors.

## Limitations & Considerations 🤔

Single File: This manages one JSON file. It's not designed for complex relational data or large datasets where a real database would be more appropriate.
Memory Usage: The entire JSON structure is loaded into memory. Very large JSON files might consume significant RAM.
Async Mode Latency: While async_updates: true makes update() non-blocking, get() and save_sync() do block while communicating with the background thread to retrieve the latest state.
unsafe: Uses unsafe internally for simd-json's from_str for performance. While believed to be safe in this context, be aware if auditing for unsafe.

## Contributing 🤝

Welcome!

## Happy JSON juggling!

🎉

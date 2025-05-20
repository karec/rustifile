use std::{
    fs::File,
    io::BufReader,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Value};

use super::{FileReader, ReaderError};

/// A struct representing a JSON Stream reader.
///
/// This reader will expect json objects split by new lines.
#[derive(Serialize, Deserialize)]
pub struct JsonStreamReader {
    /// Path for the file to read
    file_path: String,

    /// Stream reader
    #[serde(skip)]
    _iterator: Option<Arc<Mutex<dyn Iterator<Item = Result<Value, serde_json::Error>>>>>,

    /// Indicate if the reader has already been initialized
    #[serde(default)]
    _initialized: bool,
}

impl JsonStreamReader {
    /// Initializes the `JsonStreamReader` by opening the file and creating a stream iterator
    ///
    /// # Returns
    ///
    /// * `Result<(), ReaderError>` - Returns `Ok(())` if initialization is successful, otherwise returns a `ReaderError`.
    fn init(&mut self) -> Result<(), ReaderError> {
        // Open the file and create a buffered reader.
        let file = File::open(&self.file_path)?;
        let buf_reader = BufReader::new(file);

        let stream_iterator = Deserializer::from_reader(buf_reader).into_iter::<Value>();

        self._iterator = Some(Arc::new(Mutex::new(stream_iterator)));

        Ok(())
    }
}

/// Implementing the Iterator trait for `JsonStreamReader` to allow iteration over the records.
#[typetag::serde(name = "jsonstream")]
impl FileReader for JsonStreamReader {
    /// Reads an item from the JSON file.
    ///
    /// This method is called iteratively to return a `serde_json::Value` for each item present in file.
    ///
    /// # Returns
    ///
    /// * `Option<Result<Value, ReaderError>>` - Returns `Some(Ok(Value))` if an item is found, `Some(Err(ReaderError))` if an error is encountered, or `None` if the file is exhausted.
    /// # Type Conversion
    ///
    /// The JSON reader will not convert any type as it already read json from file
    fn read_item(&mut self) -> Option<Result<Value, ReaderError>> {
        if self._iterator.is_none() {
            if let Err(e) = self.init() {
                self._initialized = true;
                tracing::error!(
                    "JsonStreamReader initialization error : {:?} - file path : {}",
                    e,
                    self.file_path
                );
                return Some(Err(e));
            }
        }

        let Some(iterator) = &self._iterator else {
            return Some(Err(ReaderError::InitializationError(
                "JsonStreamReader not initialized",
            )));
        };

        match iterator.lock() {
            Ok(mut guard) => guard.next().map(|result| result.map_err(|e| e.into())),
            Err(_) => Some(Err(ReaderError::InitializationError("Mutex lock poisoned"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_file() -> String {
        format!(
            "{}/examples/products_stream.json",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    fn get_invalid_file() -> String {
        format!("{}/examples/products.json", env!("CARGO_MANIFEST_DIR"))
    }

    #[test]
    fn test_json_stream_reader_init() {
        // Create an instance of JsonStreamReader with the test file path
        let mut reader = JsonStreamReader {
            file_path: get_file(),
            _iterator: None,
            _initialized: false,
        };

        let result = reader.init();

        // Check if initialization was successful
        assert!(result.is_ok(), "Initialization failed: {:?}", result.err());
        assert!(reader._iterator.is_some(), "Iterator was not initialized");
    }

    #[test]
    fn test_json_stream_reader_iteration() {
        // Create an instance of JsonStreamReader with the test file path
        let mut reader = JsonStreamReader {
            file_path: get_file(),
            _iterator: None,
            _initialized: false,
        };

        // Initialize the reader
        reader.init().unwrap();

        // Iterate over the items and check their values
        let mut results: Vec<Result<Value, ReaderError>> = vec![];
        while let Some(item) = reader.read_item() {
            results.push(item);
        }

        let results: Vec<Value> = results.into_iter().flatten().collect();
        assert_eq!(results.len(), 2, "Expected 2 results");

        assert_eq!(results[0]["name"].as_str().unwrap(), "My super product");
        assert_eq!(results[0]["price"].as_f64().unwrap(), 10.5);
        assert_eq!(results[0]["inStock"].as_bool().unwrap(), true);

        assert_eq!(results[1]["name"].as_str().unwrap(), "My other product");
        assert_eq!(results[1]["price"].as_f64().unwrap(), 20.0);
        assert_eq!(results[1]["inStock"].as_bool().unwrap(), false);
    }

    #[test]
    fn test_json_invalid_file_stream_reader_iteration() {
        // Create an instance of JsonStreamReader with the test file path
        let mut reader = JsonStreamReader {
            file_path: get_invalid_file(),
            _iterator: None,
            _initialized: false,
        };

        // Initialize the reader
        reader.init().unwrap();

        // Iterate over the records and check their values
        let item: Result<Value, ReaderError> =
            reader.read_item().expect("should have one invalid record");

        assert!(item.is_err())
    }

    #[test]
    fn test_json_file_does_not_exists() {
        // Create an instance of JsonStreamReader with the test file path
        let mut reader = JsonStreamReader {
            file_path: String::from("/invalid/file/path"),
            _iterator: None,
            _initialized: false,
        };

        // Initialize the reader
        assert!(reader.init().is_err(), "init error expected");
    }
}

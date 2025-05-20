use std::{fs::File, io::BufReader};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{FileReader, ReaderError};

/// Default delimiter function for the CSV reader.
///
/// Returns a comma (`,`) as the default delimiter.
fn default_delimiter() -> String {
    ",".to_string()
}

/// Struct representing a CSV reader.
///
/// This struct is used to read CSV files and deserialize them into JSON values.
#[derive(Debug, Serialize, Deserialize)]
pub struct CsvReader {
    /// The delimiter used in the CSV file. Defaults to a comma (`,`).
    #[serde(default = "default_delimiter")]
    delimiter: String,

    /// Whether the CSV reader should be flexible in parsing the file.
    /// If true, the reader will attempt to correct common issues in the CSV format.
    #[serde(default)]
    flexible: bool,

    /// Path for the file to read
    file_path: String,

    /// The internal CSV reader instance. This field is skipped during serialization and deserialization.
    #[serde(skip)]
    _reader: Option<csv::Reader<BufReader<File>>>,

    /// Indicate if the reader has already been initialized
    #[serde(default)]
    _initialized: bool,
}

impl CsvReader {
    /// Initializes the CSV reader.
    ///
    /// This method opens the file specified by `file_path` and initializes the CSV reader with the given configuration.
    ///
    /// # Returns
    ///
    /// * `Result<(), ReaderError>` - Returns `Ok(())` if the reader is successfully initialized, or an error if the file cannot be opened.
    fn init_reader(&mut self) -> Result<(), ReaderError> {
        let buf_reader = BufReader::new(File::open(&self.file_path)?);

        let reader = csv::ReaderBuilder::new()
            .flexible(self.flexible)
            .delimiter(if self.delimiter.is_empty() {
                b',' // Default to comma if empty
            } else {
                self.delimiter.as_bytes()[0] // Only use first byte
            })
            .from_reader(buf_reader);

        tracing::debug!("Initialized csv reader with config : {:?}", self);

        self._reader = Some(reader);

        Ok(())
    }
}

/// Implementation of the `FileReader` trait for `CsvReader`.
///
/// This implementation allows `CsvReader` to be used as a file reader that iterates over items.
#[typetag::serde(name = "csv")]
impl FileReader for CsvReader {
    /// Reads an item from the CSV file.
    ///
    /// This method is called iteratively to return a `serde_json::Value` for each item present in the CSV file.
    ///
    /// # Returns
    ///
    /// * `Option<Result<Value, ReaderError>>` - Returns `Some(Ok(Value))` if an item is found, `Some(Err(ReaderError))` if an error is encountered, or `None` if the file is exhausted.
    /// # Type Conversion
    ///
    /// The CSV reader will attempt to convert values to appropriate JSON types:
    /// - Numeric values will be converted to JSON Numbers
    /// - "true" and "false" will be converted to JSON Booleans
    /// - All other values will remain as JSON Strings
    fn read_item(&mut self) -> Option<Result<Value, ReaderError>> {
        if self._reader.is_none() {
            if let Err(e) = self.init_reader() {
                if self._initialized {
                    return None;
                } else {
                    self._initialized = true;
                    tracing::error!(
                        "CsvReader initialization error : {:?} - Config : {:?}",
                        e,
                        self
                    );
                    return Some(Err(e));
                }
            }
        }

        match &mut self._reader {
            Some(reader) => reader.deserialize().next().map(|result| {
                let record: Map<String, Value> = result?;
                Ok(Value::Object(record))
            }),
            None => {
                tracing::error!("Cannot initialize reader");
                Some(Err(ReaderError::InitializationError(
                    "Failed to initialize reader",
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Number;
    use std::io::Write;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_reading_file() {
        let mut reader = CsvReader {
            delimiter: ",".to_string(),
            flexible: false,
            file_path: format!("{}/examples/uspop.csv", env!("CARGO_MANIFEST_DIR")),
            _reader: None,
            _initialized: false,
        };

        let mut results: Vec<Result<Value, ReaderError>> = vec![];
        while let Some(item) = reader.read_item() {
            results.push(item);
        }
        println!("{:?}", results);

        assert_eq!(results.len(), 100);

        let valid_results: Vec<Value> = results.into_iter().flatten().collect();
        assert_eq!(valid_results.len(), 100);

        let first_record = valid_results[0].clone();
        assert_eq!(
            first_record["City"],
            Value::String("Davidsons Landing".to_string())
        );
        assert_eq!(first_record["State"], Value::String("AK".to_string()));
        assert_eq!(first_record["Population"], Value::String("".to_string()));
        assert_eq!(
            first_record["Latitude"],
            Value::Number(Number::from_f64(65.2419444).unwrap())
        );
        assert_eq!(
            first_record["Longitude"],
            Value::Number(Number::from_f64(-165.2716667).unwrap())
        );
        assert_eq!(first_record["IsActive"], Value::Bool(true));
    }

    #[test]
    fn test_flexible_reader() {
        // Create a malformed CSV file with inconsistent field counts
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "Name,Age,City").unwrap();
        writeln!(file, "John,30,New York").unwrap();
        writeln!(file, "Alice,25").unwrap(); // Missing field
        writeln!(file, "Bob,40,Chicago,IL").unwrap(); // Extra field
        let path = file.path().to_str().unwrap().to_string();

        let mut reader = CsvReader {
            delimiter: ",".to_string(),
            flexible: true,
            file_path: path,
            _reader: None,
            _initialized: false,
        };

        let mut results: Vec<Result<Value, ReaderError>> = vec![];
        while let Some(item) = reader.read_item() {
            results.push(item);
        }

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_custom_delimiter() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "City\tState\tPopulation").unwrap();
        writeln!(file, "New York\tNY\t8419000").unwrap();
        writeln!(file, "Los Angeles\tCA\t3971000").unwrap();

        let path = file.path().to_str().unwrap().to_string();
        let mut reader = CsvReader {
            delimiter: "\t".to_string(),
            flexible: false,
            file_path: path,
            _reader: None,
            _initialized: false,
        };

        let mut results: Vec<Result<Value, ReaderError>> = vec![];
        while let Some(item) = reader.read_item() {
            results.push(item);
        }

        assert_eq!(results.len(), 2);

        let valid_results: Vec<Value> = results.into_iter().flatten().collect();
        assert_eq!(valid_results.len(), 2);

        let first_record = valid_results[0].clone();
        assert_eq!(first_record["City"], Value::String("New York".to_string()));
        assert_eq!(first_record["State"], Value::String("NY".to_string()));
        assert_eq!(
            first_record["Population"],
            Value::Number(Number::from_u128(8419000).unwrap())
        );
    }

    #[test]
    fn test_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap().to_string();
        let mut reader = CsvReader {
            delimiter: ",".to_string(),
            flexible: false,
            file_path: path,
            _reader: None,
            _initialized: false,
        };

        let mut results: Vec<Result<Value, ReaderError>> = vec![];
        while let Some(item) = reader.read_item() {
            results.push(item);
        }

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_nonexistent_file() {
        let mut reader = CsvReader {
            delimiter: ",".to_string(),
            flexible: false,
            file_path: "nonexistent_file.csv".to_string(),
            _reader: None,
            _initialized: false,
        };

        let first_result = reader.read_item();
        assert!(first_result.is_some(), "Expected Some(Err), got None");

        if let Some(res) = first_result {
            assert!(res.is_err(), "Expected an error for nonexistent file");
            // Optionally verify the error type if ReaderError has variants
            // assert!(matches!(res.unwrap_err(), ReaderError::Io(_)));
        }

        // Subsequent reads should return None
        assert!(reader.read_item().is_none(), "Expected None after error");
    }
}

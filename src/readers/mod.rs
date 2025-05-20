mod csv;
mod errors;

use serde_json::Value;

pub use errors::ReaderError;

/// Trait defining the functionalities of a file reader.
///
/// This trait uses the `typetag::serde` macro to enable polymorphic deserialization.
#[typetag::serde(tag = "type")]
pub trait FileReader {
    /// Reads an item from the file.
    ///
    /// This method is called iteratively to return a `serde_json::Value` for each
    /// item present in the file. In the context of this trait, an item can be one or multiple lines within the file.
    ///
    /// # Returns
    ///
    /// * `Option<Result<Value, ReaderError>>` - Returns `Some(Ok(Value))` if an item is found,
    ///   `None` if the file is exhausted, and `Some(Err(ReaderError))` if an error is encountered while reading the item.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Deserialize, Serialize};
    /// use serde_json::Value;
    /// use rustifile::readers::FileReader;
    /// use rustifile::readers::ReaderError;
    ///
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct MyFileReader;
    ///
    /// #[typetag::serde(name = "my-file-reader")]
    /// impl FileReader for MyFileReader {
    ///     fn read_item(&mut self) -> Option<Result<Value, ReaderError>> {
    ///         // Implementation of reading an item
    ///         Some(Ok(Value::String("example".to_string())))
    ///     }
    /// }
    /// ```
    fn read_item(&mut self) -> Option<Result<Value, ReaderError>>;
}

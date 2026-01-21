#![doc(
    html_logo_url = "https://raw.githubusercontent.com/dariocurr/dynamodb-crud/main/assets/logo.png",
    html_favicon_url = "https://raw.githubusercontent.com/dariocurr/dynamodb-crud/main/assets/logo.png"
)]
#![deny(missing_docs)]
#![deny(warnings)]

//! # DynamoDB CRUD
//!
//! A type-safe, ergonomic interface for performing CRUD operations on Amazon DynamoDB tables.
//!
//! ## Overview
//!
//! This library provides a high-level, type-safe API for interacting with DynamoDB that:
//! - Prevents common errors at compile time through Rust's type system
//! - Offers an intuitive builder pattern for constructing operations
//! - Supports all major DynamoDB operations (Get, Put, Update, Delete, Query, Scan, Batch)
//! - Handles expression building, pagination, and error handling automatically
//!
//! ## Quick Example
//!
//! Instead of manually building DynamoDB expression strings and managing placeholders,
//! use structured types that the compiler validates:
//!
//! ```no_run
//! use aws_sdk_dynamodb::Client;
//! use dynamodb_crud::{common, write};
//! use serde_json::Value;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = Client::from_conf(aws_sdk_dynamodb::config::Config::builder().build());
//! // Complex update with multiple operations - no expression strings needed!
//! let update_item = write::update_item::UpdateItem {
//!     keys: common::key::Keys {
//!         partition_key: common::key::Key {
//!             name: "id".to_string(),
//!             value: Value::String("1".to_string()),
//!         },
//!         ..Default::default()
//!     },
//!     update_expression: write::update_item::UpdateExpressionMap::Combined(vec![
//!         // SET: Update name and increment age atomically
//!         write::update_item::UpdateExpressionMap::Set(
//!             write::update_item::SetInputsMap::Leaves(vec![
//!                 ("name".to_string(), write::update_item::SetInput::Assign(Value::String("Jane".to_string()))),
//!                 ("age".to_string(), write::update_item::SetInput::Increment(Value::Number(1.into()))),
//!             ]),
//!         ),
//!         // ADD: Add items to a set
//!         write::update_item::UpdateExpressionMap::Add(
//!             write::update_item::AddOrDeleteInputsMap::Leaves(vec![
//!                 ("tags".to_string(), Value::Array(vec![
//!                     Value::String("new".to_string()),
//!                     Value::String("feature".to_string()),
//!                 ])),
//!             ]),
//!         ),
//!     ]),
//!     write_args: write::common::WriteArgs {
//!         table_name: "users".to_string(),
//!         ..Default::default()
//!     },
//! };
//! // The crate automatically builds: "SET #name = :set0, #age = #age + :set1 ADD #tags :add_or_delete2"
//! update_item.send(&client).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Modules
//!
//! - [`mod@common`] - Shared utilities for keys, conditions, and selections
//! - [`mod@read`] - Read operations (GetItem, Query, Scan, BatchGetItem)
//! - [`mod@write`] - Write operations (PutItem, UpdateItem, DeleteItem, BatchWriteItem)

/// Common utilities for keys, conditions, and attribute selection.
pub mod common;

/// Read operations for retrieving data from DynamoDB tables.
///
/// This module provides operations for:
/// - Getting individual items by key
/// - Querying items with key conditions
/// - Scanning entire tables
/// - Batch retrieving multiple items
pub mod read;

/// Write operations for modifying data in DynamoDB tables.
///
/// This module provides operations for:
/// - Putting new items or replacing existing ones
/// - Updating items with various operations (set, add, remove)
/// - Deleting items by key
/// - Batch writing multiple items
pub mod write;

//! Read operations for retrieving data from DynamoDB tables.
//!
//! This module provides operations for reading data from DynamoDB:
//! - Getting individual items by primary key
//! - Querying items with key conditions
//! - Scanning entire tables
//! - Batch retrieving multiple items

/// Batch get item operation for retrieving multiple items efficiently.
pub mod batch_get_item;

/// Common utilities and types for read operations.
pub mod common;

/// Get item operation for retrieving a single item by primary key.
pub mod get_item;

/// Query operation for retrieving items with key conditions.
pub mod query;

/// Scan operation for retrieving all items from a table.
pub mod scan;

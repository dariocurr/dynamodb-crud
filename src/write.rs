//! Write operations for modifying data in DynamoDB tables.
//!
//! This module provides operations for writing data to DynamoDB:
//! - Putting new items or replacing existing ones
//! - Updating items with various operations
//! - Deleting items by primary key
//! - Batch writing multiple items

/// Batch write item operation for efficiently writing multiple items.
pub mod batch_write_item;

/// Common utilities and types for write operations.
pub mod common;

/// Delete item operation for removing items from tables.
pub mod delete_item;

/// Put item operation for creating or replacing items.
pub mod put_item;

/// Update item operation for modifying existing items.
pub mod update_item;

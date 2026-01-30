//! Common utilities for DynamoDB operations.
//!
//! This module provides shared types and utilities used across read and write operations,
//! including key handling, condition expressions, and attribute selection.

/// Condition expression building for filters and conditional writes.
pub mod condition;

/// Key types for identifying items in DynamoDB tables.
pub mod key;

/// Attribute selection for projection expressions.
pub mod selection;

use aws_sdk_dynamodb::types;
use std::collections;

pub(crate) fn add_placeholder(keys: &[String], identifier: &str) -> (String, Vec<String>) {
    let placeholder = format!("#{identifier}");
    let mut new_keys = Vec::with_capacity(keys.len() + 1);
    new_keys.extend_from_slice(keys);
    new_keys.push(placeholder.clone());
    (placeholder, new_keys)
}

fn get_expression(left: String, operator: &str, right: String) -> String {
    if left.is_empty() {
        right
    } else if right.is_empty() {
        left
    } else {
        format!("{left}{operator}{right}")
    }
}

/// expression operation
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ExpressionInput {
    pub(crate) expression: String,
    pub(crate) expression_attribute_names: collections::HashMap<String, String>,
    pub(crate) expression_attribute_values: collections::HashMap<String, types::AttributeValue>,
}

impl ExpressionInput {
    pub(crate) fn merge(operator: &str, items: Vec<Self>) -> Self {
        let mut operation = Self::default();
        for item in items {
            operation
                .expression_attribute_names
                .extend(item.expression_attribute_names);
            operation
                .expression_attribute_values
                .extend(item.expression_attribute_values);
            operation.expression = get_expression(operation.expression, operator, item.expression);
        }
        operation
    }

    pub(crate) fn merge_into(
        self,
        names: &mut Option<collections::HashMap<String, String>>,
        values: &mut Option<collections::HashMap<String, types::AttributeValue>>,
    ) -> String {
        match names {
            Some(existing) => existing.extend(self.expression_attribute_names),
            None => *names = Some(self.expression_attribute_names),
        }
        match values {
            Some(existing) => existing.extend(self.expression_attribute_values),
            None => *values = Some(self.expression_attribute_values),
        }
        self.expression
    }
}

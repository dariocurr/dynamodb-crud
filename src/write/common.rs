use crate::common;

use aws_sdk_dynamodb::types;
use serde::Serialize;
use serde_dynamo::{Error, Result};
use std::collections;

/// Internal representation of write operation parameters.
///
/// This is an internal type that holds the processed write operation parameters
/// after conversion from the public `WriteArgs` type. It contains the fully
/// resolved expression strings and attribute mappings ready for DynamoDB API calls.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WriteInput {
    pub(crate) condition_expression: Option<String>,
    pub(crate) expression_attribute_names: Option<collections::HashMap<String, String>>,
    pub(crate) expression_attribute_values:
        Option<collections::HashMap<String, types::AttributeValue>>,
    pub(crate) return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    pub(crate) return_item_collection_metrics: Option<types::ReturnItemCollectionMetrics>,
    pub(crate) return_values: Option<types::ReturnValue>,
    pub(crate) return_values_on_condition_check_failure:
        Option<types::ReturnValuesOnConditionCheckFailure>,
    pub(crate) table_name: String,
}

impl WriteInput {
    /// Merge an expression operation into this write operation.
    pub(crate) fn merge_expression(&mut self, operation: common::ExpressionInput) -> String {
        operation.merge_into(
            &mut self.expression_attribute_names,
            &mut self.expression_attribute_values,
        )
    }
}

/// Arguments common to all write operations (Put, Update, Delete).
///
/// These arguments apply to operations that modify data in DynamoDB tables.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteArgs<T> {
    /// Condition expression that must be true for the operation to succeed.
    ///
    /// If specified, the operation will only proceed if the condition evaluates to true.
    /// If the condition is false, the operation will fail with a conditional check error.
    pub condition: Option<common::condition::ConditionMap<T>>,
    /// Whether to return the consumed capacity information.
    ///
    /// Useful for monitoring and capacity planning.
    pub return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    /// Whether to return item collection metrics.
    ///
    /// Item collection metrics provide information about collections (local secondary indexes)
    /// affected by the operation.
    pub return_item_collection_metrics: Option<types::ReturnItemCollectionMetrics>,
    /// Which item attributes to return in the response.
    ///
    /// Options: `AllOld`, `AllNew`, `UpdatedOld`, `UpdatedNew`, or `None`.
    pub return_values: Option<types::ReturnValue>,
    /// Which item attributes to return if a condition check fails.
    ///
    /// Allows you to see the item that caused the condition check to fail.
    pub return_values_on_condition_check_failure:
        Option<types::ReturnValuesOnConditionCheckFailure>,
    /// The name of the table to write to.
    pub table_name: String,
}

impl<T: Serialize> TryFrom<WriteArgs<T>> for WriteInput {
    type Error = Error;

    fn try_from(write_args: WriteArgs<T>) -> Result<Self> {
        let (condition_expression, expression_attribute_names, expression_attribute_values) =
            match write_args.condition {
                Some(condition) => {
                    let condition_operation: common::ExpressionInput = condition.try_into()?;
                    (
                        Some(condition_operation.expression),
                        Some(condition_operation.expression_attribute_names),
                        Some(condition_operation.expression_attribute_values),
                    )
                }
                None => (None, None, None),
            };
        let operation = Self {
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_consumed_capacity: write_args.return_consumed_capacity,
            return_item_collection_metrics: write_args.return_item_collection_metrics,
            return_values: write_args.return_values,
            return_values_on_condition_check_failure: write_args
                .return_values_on_condition_check_failure,
            table_name: write_args.table_name,
        };
        Ok(operation)
    }
}

/// apply common write operation settings to a builder
#[macro_export]
macro_rules! apply_write_operation {
    ($builder:expr, $write_operation:expr) => {
        $builder
            .set_condition_expression($write_operation.condition_expression)
            .set_expression_attribute_names($write_operation.expression_attribute_names)
            .set_expression_attribute_values($write_operation.expression_attribute_values)
            .set_return_consumed_capacity($write_operation.return_consumed_capacity)
            .set_return_item_collection_metrics($write_operation.return_item_collection_metrics)
            .set_return_values($write_operation.return_values)
            .set_return_values_on_condition_check_failure(
                $write_operation.return_values_on_condition_check_failure,
            )
            .table_name($write_operation.table_name)
    };
}

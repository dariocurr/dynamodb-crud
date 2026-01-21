use crate::common;

use aws_sdk_dynamodb::types;
use serde::Serialize;
use serde_dynamo::{Error, Result, to_attribute_value};
use std::collections;

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SingleReadInput {
    pub(crate) consistent_read: Option<bool>,
    pub(crate) expression_attribute_names: Option<collections::HashMap<String, String>>,
    pub(crate) projection_expression: Option<String>,
    pub(crate) table_name: String,
}

/// Arguments for single-item read operations (GetItem).
///
/// These arguments apply to operations that retrieve a single item, such as GetItem.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct SingleReadArgs {
    /// Whether to use a consistent read.
    ///
    /// `true` for strongly consistent reads, `false` or `None` for eventually consistent reads.
    /// Consistent reads consume more capacity units but guarantee you see the latest data.
    pub consistent_read: Option<bool>,
    /// Which attributes to retrieve (projection expression).
    ///
    /// If `None`, all attributes are retrieved. Use `SelectionMap` to specify
    /// which attributes to include in the response.
    pub selection: Option<common::selection::SelectionMap>,
    /// The name of the table to read from.
    pub table_name: String,
}

impl From<SingleReadArgs> for SingleReadInput {
    fn from(single_read_args: SingleReadArgs) -> Self {
        let (expression_attribute_names, projection_expression) = match single_read_args.selection {
            Some(selection) => {
                let selection_operation: common::ExpressionInput = selection.into();
                (
                    Some(selection_operation.expression_attribute_names),
                    Some(selection_operation.expression),
                )
            }
            None => (None, None),
        };
        Self {
            consistent_read: single_read_args.consistent_read,
            expression_attribute_names,
            projection_expression,
            table_name: single_read_args.table_name,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MultipleReadInput {
    pub(crate) consistent_read: Option<bool>,
    pub(crate) exclusive_start_key: Option<collections::HashMap<String, types::AttributeValue>>,
    pub(crate) expression_attribute_names: Option<collections::HashMap<String, String>>,
    pub(crate) expression_attribute_values:
        Option<collections::HashMap<String, types::AttributeValue>>,
    pub(crate) filter_expression: Option<String>,
    pub(crate) index_name: Option<String>,
    pub(crate) limit: Option<i32>,
    pub(crate) projection_expression: Option<String>,
    pub(crate) select: Option<types::Select>,
    pub(crate) table_name: String,
}

/// Arguments for multiple-item read operations (Query, Scan).
///
/// These arguments apply to operations that can return multiple items, such as Query and Scan.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MultipleReadArgs<T> {
    /// Filter condition to apply to the results.
    ///
    /// For Query operations, this is a filter expression (applied after key condition).
    /// For Scan operations, this is a filter expression applied to all scanned items.
    pub condition: Option<common::condition::ConditionMap<T>>,
    /// Whether to use a consistent read.
    ///
    /// `true` for strongly consistent reads, `false` or `None` for eventually consistent reads.
    pub consistent_read: Option<bool>,
    /// The exclusive start key for pagination.
    ///
    /// Used to continue a previous Query or Scan operation from where it left off.
    /// Typically obtained from the `last_evaluated_key` in the previous response.
    pub exclusive_start_key: Option<collections::HashMap<String, T>>,
    /// The name of a global secondary index or local secondary index to query.
    ///
    /// If specified, the operation will query the index instead of the base table.
    pub index_name: Option<String>,
    /// The maximum number of items to evaluate (not necessarily the number of matching items).
    ///
    /// DynamoDB will return up to this many items. If more items match, you'll need
    /// to paginate using `exclusive_start_key`.
    pub limit: Option<i32>,
    /// Which attributes to return.
    ///
    /// Use `Select::AllAttributes` (default), `Select::AllProjectedAttributes`,
    /// `Select::SpecificAttributes` (with `selection`), or `Select::Count`.
    pub select: Option<types::Select>,
    /// Which attributes to retrieve (projection expression).
    ///
    /// Only used when `select` is `Select::SpecificAttributes`. If `None` and
    /// `select` is `Select::SpecificAttributes`, all attributes are returned.
    pub selection: Option<common::selection::SelectionMap>,
    /// The name of the table to read from.
    pub table_name: String,
}

impl<T: Serialize> TryFrom<MultipleReadArgs<T>> for MultipleReadInput {
    type Error = Error;

    fn try_from(multiple_read_args: MultipleReadArgs<T>) -> Result<Self> {
        let exclusive_start_key = match multiple_read_args.exclusive_start_key {
            Some(exclusive_start_key) => {
                let mut serialized_exclusive_start_key =
                    collections::HashMap::with_capacity(exclusive_start_key.len());
                for (key, value) in exclusive_start_key {
                    let value = to_attribute_value(value)?;
                    serialized_exclusive_start_key.insert(key, value);
                }
                Some(serialized_exclusive_start_key)
            }
            None => None,
        };
        let condition_operation: Option<common::ExpressionInput> = multiple_read_args
            .condition
            .map(|condition| condition.try_into())
            .transpose()?;
        let selection_operation: Option<common::ExpressionInput> = multiple_read_args
            .selection
            .map(|selection| selection.into());
        let (
            expression_attribute_names,
            expression_attribute_values,
            filter_expression,
            projection_expression,
        ) = match (condition_operation, selection_operation) {
            (Some(mut condition_operation), Some(selection_operation)) => {
                condition_operation
                    .expression_attribute_names
                    .extend(selection_operation.expression_attribute_names);
                (
                    Some(condition_operation.expression_attribute_names),
                    Some(condition_operation.expression_attribute_values),
                    Some(condition_operation.expression),
                    Some(selection_operation.expression),
                )
            }
            (Some(condition_operation), None) => (
                Some(condition_operation.expression_attribute_names),
                Some(condition_operation.expression_attribute_values),
                Some(condition_operation.expression),
                None,
            ),
            (None, Some(selection_operation)) => (
                Some(selection_operation.expression_attribute_names),
                None,
                None,
                Some(selection_operation.expression),
            ),
            (None, None) => (None, None, None, None),
        };
        let operation = Self {
            consistent_read: multiple_read_args.consistent_read,
            exclusive_start_key,
            expression_attribute_names,
            expression_attribute_values,
            filter_expression,
            index_name: multiple_read_args.index_name,
            limit: multiple_read_args.limit,
            projection_expression,
            select: multiple_read_args.select,
            table_name: multiple_read_args.table_name,
        };
        Ok(operation)
    }
}

/// get paginated output
#[macro_export]
macro_rules! get_paginated_output {
    ($paginator:expr, $output_type:ty) => {{
        let mut outputs = Vec::new();
        while let Some(page) = $paginator.next().await {
            outputs.push(page?);
        }
        let (items, count, scanned, capacities) = outputs.into_iter().fold(
            (Vec::new(), 0, 0, Vec::new()),
            |(mut items, count, scanned, mut caps), output| {
                if let Some(other_items) = output.items {
                    items.extend(other_items);
                }
                if let Some(cap) = output.consumed_capacity {
                    caps.push(cap);
                }
                (
                    items,
                    count + output.count,
                    scanned + output.scanned_count,
                    caps,
                )
            },
        );
        let aggregated_capacity = $crate::read::common::aggregate_capacity(capacities);
        let output = <$output_type>::builder()
            .set_items(Some(items))
            .set_count(Some(count))
            .set_scanned_count(Some(scanned))
            .set_consumed_capacity(Some(aggregated_capacity))
            .build();
        Ok(output)
    }};
}

pub(crate) fn aggregate_capacity(
    capacities: Vec<types::ConsumedCapacity>,
) -> types::ConsumedCapacity {
    let (cap, read, write, table) = capacities.into_iter().fold(
        (0.0, 0.0, 0.0, None),
        |(cap, read, write, table), capacity| {
            (
                cap + capacity.capacity_units.unwrap_or(0.0),
                read + capacity.read_capacity_units.unwrap_or(0.0),
                write + capacity.write_capacity_units.unwrap_or(0.0),
                table.or(capacity.table_name),
            )
        },
    );
    types::ConsumedCapacity::builder()
        .set_table_name(table)
        .set_capacity_units(Some(cap))
        .set_read_capacity_units(Some(read))
        .set_write_capacity_units(Some(write))
        .build()
}

/// apply common single read operation settings to a builder
#[macro_export]
macro_rules! apply_single_read_operation {
    ($builder:expr, $single_read_operation:expr) => {
        $builder
            .set_consistent_read($single_read_operation.consistent_read)
            .set_expression_attribute_names($single_read_operation.expression_attribute_names)
            .set_projection_expression($single_read_operation.projection_expression)
            .table_name($single_read_operation.table_name)
    };
}

/// apply common multiple read operation settings to a builder
#[macro_export]
macro_rules! apply_multiple_read_operation {
    ($builder:expr, $multiple_read_operation:expr) => {
        $builder
            .set_consistent_read($multiple_read_operation.consistent_read)
            .set_exclusive_start_key($multiple_read_operation.exclusive_start_key)
            .set_expression_attribute_names($multiple_read_operation.expression_attribute_names)
            .set_expression_attribute_values($multiple_read_operation.expression_attribute_values)
            .set_filter_expression($multiple_read_operation.filter_expression)
            .set_index_name($multiple_read_operation.index_name)
            .set_limit($multiple_read_operation.limit)
            .set_projection_expression($multiple_read_operation.projection_expression)
            .set_select($multiple_read_operation.select)
            .table_name($multiple_read_operation.table_name)
    };
}

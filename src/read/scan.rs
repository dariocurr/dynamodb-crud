use crate::read;

use aws_sdk_dynamodb::{Client, error, operation, types};
use serde::Serialize;
use serde_dynamo::{Error, Result};

/// scan operation
#[derive(Clone, Debug, Default, PartialEq)]
struct ScanInput {
    multiple_read_operation: read::common::MultipleReadInput,
    return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    segment: Option<i32>,
    total_segments: Option<i32>,
}

/// Scan operation.
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
/// use dynamodb_crud::read;
/// use serde_json::Value;
///
/// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let scan: read::scan::Scan<Value> = read::scan::Scan {
///     multiple_read_args: read::common::MultipleReadArgs {
///         table_name: "users".to_string(),
///         ..Default::default()
///     },
///     ..Default::default()
/// };
/// scan.send(client).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Scan<T> {
    /// Additional read operation arguments (table name, filter, selection, etc.).
    pub multiple_read_args: read::common::MultipleReadArgs<T>,
    /// Whether to return the consumed capacity information.
    pub return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    /// The segment number for parallel scans (0-indexed).
    pub segment: Option<i32>,
    /// The total number of segments for parallel scans.
    pub total_segments: Option<i32>,
}

impl<T: Serialize> TryFrom<Scan<T>> for ScanInput {
    type Error = Error;

    fn try_from(scan: Scan<T>) -> Result<Self> {
        let multiple_read_operation: read::common::MultipleReadInput =
            scan.multiple_read_args.try_into()?;
        let operation = Self {
            multiple_read_operation,
            return_consumed_capacity: scan.return_consumed_capacity,
            segment: scan.segment,
            total_segments: scan.total_segments,
        };
        Ok(operation)
    }
}

impl<T: Serialize> Scan<T> {
    /// Execute the scan operation.
    pub async fn send(
        self,
        client: &Client,
    ) -> Result<operation::scan::ScanOutput, error::SdkError<operation::scan::ScanError>> {
        let scan: ScanInput = self.try_into().map_err(error::BuildError::other)?;
        let builder = client
            .scan()
            .set_return_consumed_capacity(scan.return_consumed_capacity)
            .set_segment(scan.segment)
            .set_total_segments(scan.total_segments);
        let mut paginator =
            crate::apply_multiple_read_operation!(builder, scan.multiple_read_operation)
                .into_paginator()
                .send();
        crate::get_paginated_output!(paginator, operation::scan::ScanOutput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;

    use rstest::rstest;
    use serde_json::Value;
    use std::collections;

    #[rstest]
    #[case::empty(
        Scan {
            multiple_read_args: read::common::MultipleReadArgs {
                table_name: "a".to_string(),
                ..Default::default()
            },
            ..Default::default()
        },
        ScanInput {
            multiple_read_operation: read::common::MultipleReadInput {
                table_name: "a".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    )]
    #[case::full(
        Scan {
            multiple_read_args: read::common::MultipleReadArgs {
                condition: Some(
                    common::condition::ConditionMap::Leaves(
                        common::condition::LogicalOperator::And,
                        vec![
                            common::condition::KeyCondition {
                                name: "a".to_string(),
                                condition: common::condition::Condition::Equals(
                                    Value::String(
                                        "b".to_string()
                                    )
                                ),
                            },
                        ]
                    )
                ),
                consistent_read: Some(false),
                exclusive_start_key: Some(
                    collections::HashMap::from(
                        [
                            (
                                "c".to_string(),
                                Value::String(
                                    "d".to_string()
                                )
                            ),
                        ]
                    )
                ),
                index_name: Some("e".to_string()),
                limit: Some(10),
                select: Some(
                    types::Select::Count
                ),
                selection: Some(
                    common::selection::SelectionMap::Leaves(
                        vec![
                            "f".to_string(),
                            "g".to_string()
                        ]
                    )
                ),
                table_name: "h".to_string(),
            },
            return_consumed_capacity: Some(
                types::ReturnConsumedCapacity::Total
            ),
            segment: Some(1),
            total_segments: Some(10),
        },
        ScanInput {
            multiple_read_operation: read::common::MultipleReadInput {
                consistent_read: Some(false),
                exclusive_start_key: Some(
                    collections::HashMap::from(
                        [
                            (
                                "c".to_string(),
                                types::AttributeValue::S(
                                    "d".to_string()
                                )
                            ),
                        ]
                    )
                ),
                expression_attribute_names: Some(
                    collections::HashMap::from(
                        [
                            ("#a".to_string(), "a".to_string()),
                            ("#f".to_string(), "f".to_string()),
                            ("#g".to_string(), "g".to_string()),
                        ]
                    )
                ),
                expression_attribute_values: Some(
                    collections::HashMap::from(
                        [
                            (
                                ":a_eq0".to_string(),
                                types::AttributeValue::S(
                                    "b".to_string()
                                )
                            ),
                        ]
                    )
                ),
                filter_expression: Some(
                    "(#a = :a_eq0)".to_string()
                ),
                index_name: Some("e".to_string()),
                limit: Some(10),
                projection_expression: Some(
                    "#f, #g".to_string()
                ),
                select: Some(
                    types::Select::Count
                ),
                table_name: "h".to_string(),
            },
            return_consumed_capacity: Some(
                types::ReturnConsumedCapacity::Total
            ),
            segment: Some(1),
            total_segments: Some(10),
        }
    )]
    fn test_scan(#[case] args: Scan<Value>, #[case] expected: ScanInput) {
        let actual: ScanInput = args.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

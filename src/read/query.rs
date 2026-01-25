use crate::{common, read};

use aws_sdk_dynamodb::{Client, error, operation, types};
use serde::Serialize;
use serde_dynamo::{Error, Result};

/// query operation
#[derive(Clone, Debug, Default, PartialEq)]
struct QueryInput {
    key_condition_expression: String,
    multiple_read_operation: read::common::MultipleReadInput,
    return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    scan_index_forward: Option<bool>,
}

/// Query operation.
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
/// use dynamodb_crud::{common, read};
///
/// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let query = read::query::Query {
///     partition_key: common::key::Key {
///         name: "id".to_string(),
///         value: "1".to_string(),
///     },
///     multiple_read_args: read::common::MultipleReadArgs {
///         table_name: "users".to_string(),
///         ..Default::default()
///     },
///     ..Default::default()
/// };
/// query.send(client).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Query<T> {
    /// Additional read operation arguments (table name, filter, selection, etc.).
    pub multiple_read_args: read::common::MultipleReadArgs<T>,
    /// The partition key value to query for.
    pub partition_key: common::key::Key<T>,
    /// Whether to return the consumed capacity information.
    pub return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    /// Whether to scan the index forward (ascending) or backward (descending).
    pub scan_index_forward: Option<bool>,
    /// Optional condition to apply to the sort key.
    pub sort_key_condition: Option<common::condition::KeyCondition<T>>,
}

impl<T: Serialize> Query<T> {
    fn get_key_condition_expression(
        partition_key: common::key::Key<T>,
        sort_key: Option<common::condition::KeyCondition<T>>,
    ) -> Result<common::ExpressionInput> {
        let condition = common::condition::Condition::Equals(partition_key.value);
        let partition_key = common::condition::KeyCondition {
            condition,
            name: partition_key.name,
        };
        let mut keys = vec![partition_key];
        if let Some(sort_key) = sort_key {
            keys.push(sort_key);
        }
        common::condition::KeyCondition::get_expression_operation(keys)
    }
}

impl<T: Serialize> TryFrom<Query<T>> for QueryInput {
    type Error = Error;

    fn try_from(query: Query<T>) -> Result<Self> {
        let mut multiple_read_operation: read::common::MultipleReadInput =
            query.multiple_read_args.try_into()?;
        let key_condition_operation =
            Query::get_key_condition_expression(query.partition_key, query.sort_key_condition)?;
        let key_condition_expression = key_condition_operation.merge_into(
            &mut multiple_read_operation.expression_attribute_names,
            &mut multiple_read_operation.expression_attribute_values,
        );
        let operation = Self {
            key_condition_expression,
            multiple_read_operation,
            return_consumed_capacity: query.return_consumed_capacity,
            scan_index_forward: query.scan_index_forward,
        };
        Ok(operation)
    }
}

impl<T: Serialize> Query<T> {
    /// Execute the query operation.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "dynamodb_crud.query", err)
    )]
    pub async fn send(
        self,
        client: &Client,
    ) -> Result<operation::query::QueryOutput, error::SdkError<operation::query::QueryError>> {
        let query: QueryInput = self.try_into().map_err(error::BuildError::other)?;
        let builder = client
            .query()
            .key_condition_expression(query.key_condition_expression)
            .set_return_consumed_capacity(query.return_consumed_capacity)
            .set_scan_index_forward(query.scan_index_forward);
        let mut paginator =
            crate::apply_multiple_read_operation!(builder, query.multiple_read_operation)
                .into_paginator()
                .send();
        crate::get_paginated_output!(paginator, operation::query::QueryOutput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use serde_json::Value;
    use std::collections;

    #[rstest]
    #[case::empty(
        Query {
            multiple_read_args: read::common::MultipleReadArgs {
                table_name: "a".to_string(),
                ..Default::default()
            },
            partition_key: common::key::Key {
                name: "b".to_string(),
                value: Value::String(
                    "c".to_string()
                ),
            },
            ..Default::default()
        },
        QueryInput {
            key_condition_expression: "#b = :b_eq0".to_string(),
            multiple_read_operation: read::common::MultipleReadInput {
                expression_attribute_names: Some(
                    collections::HashMap::from(
                        [
                            ("#b".to_string(), "b".to_string()),
                        ]
                    )
                ),
                expression_attribute_values: Some(
                    collections::HashMap::from(
                        [
                            (
                                ":b_eq0".to_string(),
                                types::AttributeValue::S(
                                    "c".to_string()
                                )
                            ),
                        ]
                    )
                ),
                table_name: "a".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    )]
    #[case::full(
        Query {
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
            partition_key: common::key::Key {
                name: "i".to_string(),
                value: Value::String(
                    "j".to_string()
                ),
            },
            return_consumed_capacity: Some(
                types::ReturnConsumedCapacity::Total
            ),
            scan_index_forward: Some(true),
            sort_key_condition: Some(
                common::condition::KeyCondition {
                    name: "k".to_string(),
                    condition: common::condition::Condition::Equals(
                        Value::String(
                            "l".to_string()
                        )
                    ),
                }
            ),
        },
        QueryInput {
            key_condition_expression: "#i = :i_eq0 AND #k = :k_eq1".to_string(),
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
                            ("#i".to_string(), "i".to_string()),
                            ("#k".to_string(), "k".to_string()),
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
                            (
                                ":i_eq0".to_string(),
                                types::AttributeValue::S(
                                    "j".to_string()
                                )
                            ),
                            (
                                ":k_eq1".to_string(),
                                types::AttributeValue::S(
                                    "l".to_string()
                                )
                            ),
                        ]
                    )
                ),
                filter_expression: Some(
                    "#a = :a_eq0".to_string()
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
            scan_index_forward: Some(true),
        }
    )]
    fn test_query(#[case] args: Query<Value>, #[case] expected: QueryInput) {
        let actual: QueryInput = args.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

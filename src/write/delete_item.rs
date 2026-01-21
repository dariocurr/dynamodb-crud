use crate::{common, write};

use aws_sdk_dynamodb::{Client, error, operation, types};
use serde::Serialize;
use serde_dynamo::{Error, Result};
use std::collections;

/// delete item operation
#[derive(Debug, PartialEq)]
struct DeleteItemInput {
    keys: collections::HashMap<String, types::AttributeValue>,
    write_operation: write::common::WriteInput,
}

/// Delete item operation.
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
/// use dynamodb_crud::{common, write};
///
/// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let delete_item = write::delete_item::DeleteItem {
///     keys: common::key::Keys {
///         partition_key: common::key::Key {
///             name: "id".to_string(),
///             value: "1".to_string(),
///         },
///         ..Default::default()
///     },
///     write_args: write::common::WriteArgs {
///         table_name: "users".to_string(),
///         ..Default::default()
///     },
/// };
/// delete_item.send(client).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, PartialEq)]
pub struct DeleteItem<T> {
    /// The primary key of the item to delete.
    pub keys: common::key::Keys<T>,
    /// Additional write operation arguments (table name, condition, return values, etc.).
    pub write_args: write::common::WriteArgs<T>,
}

impl<T: Serialize> TryFrom<DeleteItem<T>> for DeleteItemInput {
    type Error = Error;

    fn try_from(delete_item: DeleteItem<T>) -> Result<Self> {
        let keys = delete_item.keys.try_into()?;
        let write_operation: write::common::WriteInput = delete_item.write_args.try_into()?;
        let operation = Self {
            keys,
            write_operation,
        };
        Ok(operation)
    }
}

impl<T: Serialize> DeleteItem<T> {
    /// Execute the delete item operation.
    pub async fn send(
        self,
        client: &Client,
    ) -> Result<
        operation::delete_item::DeleteItemOutput,
        error::SdkError<operation::delete_item::DeleteItemError>,
    > {
        let delete_item: DeleteItemInput = self.try_into().map_err(error::BuildError::other)?;
        let builder = client.delete_item().set_key(Some(delete_item.keys));
        crate::apply_write_operation!(builder, delete_item.write_operation)
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use serde_json::Value;

    #[rstest]
    #[case::empty(
        DeleteItem {
            keys: common::key::Keys {
                partition_key: common::key::Key {
                    name: "a".to_string(),
                    value: Value::String(
                        "b".to_string()
                    ),
                },
                ..Default::default()
            },
            write_args: write::common::WriteArgs {
                table_name: "c".to_string(),
                ..Default::default()
            },
        },
        DeleteItemInput {
            keys: collections::HashMap::from(
                [
                    (
                        "a".to_string(),
                        types::AttributeValue::S(
                            "b".to_string()
                        )
                    ),
                ]
            ),
            write_operation: write::common::WriteInput {
                table_name: "c".to_string(),
                ..Default::default()
            },
        }
    )]
    #[case::full(
        DeleteItem {
            keys: common::key::Keys {
                partition_key: common::key::Key {
                    name: "a".to_string(),
                    value: Value::String(
                        "b".to_string()
                    ),
                },
                sort_key: Some(
                    common::key::Key {
                        name: "c".to_string(),
                        value: Value::String(
                            "d".to_string()
                        ),
                    }
                ),
            },
            write_args: write::common::WriteArgs {
                condition: Some(
                    common::condition::ConditionMap::Leaves(
                        common::condition::LogicalOperator::And,
                        vec![
                            common::condition::KeyCondition {
                                name: "e".to_string(),
                                condition: common::condition::Condition::Equals(
                                    Value::String(
                                        "f".to_string()
                                    )
                                ),
                            },
                        ]
                    )
                ),
                return_consumed_capacity: Some(
                    types::ReturnConsumedCapacity::Total
                ),
                return_item_collection_metrics: Some(
                    types::ReturnItemCollectionMetrics::Size
                ),
                return_values: Some(
                    types::ReturnValue::AllOld
                ),
                return_values_on_condition_check_failure: Some(
                    types::ReturnValuesOnConditionCheckFailure::AllOld
                ),
                table_name: "g".to_string(),
            },
        },
        DeleteItemInput {
            keys: collections::HashMap::from(
                [
                    (
                        "a".to_string(),
                        types::AttributeValue::S(
                            "b".to_string()
                        )
                    ),
                    (
                        "c".to_string(),
                        types::AttributeValue::S(
                            "d".to_string()
                        )
                    ),
                ]
            ),
            write_operation: write::common::WriteInput {
                condition_expression: Some(
                    "(#e = :e_eq0)".to_string()
                ),
                expression_attribute_names: Some(
                    collections::HashMap::from(
                        [
                            ("#e".to_string(), "e".to_string()),
                        ]
                    )
                ),
                expression_attribute_values: Some(
                    collections::HashMap::from(
                        [
                            (
                                ":e_eq0".to_string(),
                                types::AttributeValue::S(
                                    "f".to_string()
                                )
                            ),
                        ]
                    )
                ),
                return_consumed_capacity: Some(
                    types::ReturnConsumedCapacity::Total
                ),
                return_item_collection_metrics: Some(
                    types::ReturnItemCollectionMetrics::Size
                ),
                return_values: Some(
                    types::ReturnValue::AllOld
                ),
                return_values_on_condition_check_failure: Some(
                    types::ReturnValuesOnConditionCheckFailure::AllOld
                ),
                table_name: "g".to_string(),
            },
        }
    )]
    fn test_delete_item(#[case] args: DeleteItem<Value>, #[case] expected: DeleteItemInput) {
        let actual: DeleteItemInput = args.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

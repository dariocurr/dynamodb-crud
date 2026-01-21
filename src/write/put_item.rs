use crate::write;

use aws_sdk_dynamodb::{Client, error, operation, types};
use serde::Serialize;
use serde_dynamo::{Error, Result, to_item};
use std::collections;

/// put item operation
#[derive(Debug, PartialEq)]
struct PutItemInput {
    item: collections::HashMap<String, types::AttributeValue>,
    write_operation: write::common::WriteInput,
}

/// Put item operation.
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
/// use dynamodb_crud::write;
/// use serde_json::json;
///
/// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let put_item = write::put_item::PutItem {
///     item: json!({"id": "1", "name": "John"}),
///     write_args: write::common::WriteArgs {
///         table_name: "users".to_string(),
///         ..Default::default()
///     },
/// };
/// put_item.send(client).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, PartialEq)]
pub struct PutItem<T> {
    /// The item to put into the table.
    pub item: T,
    /// Additional write operation arguments (table name, condition, return values, etc.).
    pub write_args: write::common::WriteArgs<T>,
}

impl<T: Serialize> TryFrom<PutItem<T>> for PutItemInput {
    type Error = Error;

    fn try_from(put_item: PutItem<T>) -> Result<Self> {
        let item = to_item(put_item.item)?;
        let write_operation: write::common::WriteInput = put_item.write_args.try_into()?;
        let operation = Self {
            item,
            write_operation,
        };
        Ok(operation)
    }
}

impl<T: Serialize> PutItem<T> {
    /// Execute the put item operation.
    pub async fn send(
        self,
        client: &Client,
    ) -> Result<
        operation::put_item::PutItemOutput,
        error::SdkError<operation::put_item::PutItemError>,
    > {
        let put_item: PutItemInput = self.try_into().map_err(error::BuildError::other)?;
        let builder = client.put_item().set_item(Some(put_item.item));
        crate::apply_write_operation!(builder, put_item.write_operation)
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;

    use rstest::rstest;
    use serde_json::{Value, json};

    #[rstest]
    #[case::empty(
        PutItem {
            item: json!(
                {
                    "a": "b"
                }
            ),
            write_args: write::common::WriteArgs {
                table_name: "c".to_string(),
                ..Default::default()
            },
        },
        PutItemInput {
            item: collections::HashMap::from(
                [(
                    "a".to_string(),
                    types::AttributeValue::S(
                        "b".to_string()
                    ),
                )]
            ),
            write_operation: write::common::WriteInput {
                table_name: "c".to_string(),
                ..Default::default()
            },
        }
    )]
    #[case::full(
        PutItem {
            item: json!(
                {
                    "a": "b"
                }
            ),
            write_args: write::common::WriteArgs {
                condition: Some(
                    common::condition::ConditionMap::Leaves(
                        common::condition::LogicalOperator::And,
                        vec![
                            common::condition::KeyCondition {
                                name: "c".to_string(),
                                condition: common::condition::Condition::Equals(
                                    Value::String(
                                        "d".to_string()
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
                table_name: "e".to_string(),
            },
        },
        PutItemInput {
            item: collections::HashMap::from(
                [(
                    "a".to_string(),
                    types::AttributeValue::S(
                        "b".to_string()
                    ),
                )]
            ),
            write_operation: write::common::WriteInput {
                condition_expression: Some(
                    "(#c = :c_eq0)".to_string()
                ),
                expression_attribute_names: Some(
                    collections::HashMap::from(
                        [
                            ("#c".to_string(), "c".to_string()),
                        ]
                    )
                ),
                expression_attribute_values: Some(
                    collections::HashMap::from(
                        [
                            (
                                ":c_eq0".to_string(),
                                types::AttributeValue::S(
                                    "d".to_string()
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
                table_name: "e".to_string(),
            },
        }
    )]
    fn test_put_item(#[case] args: PutItem<Value>, #[case] expected: PutItemInput) {
        let actual: PutItemInput = args.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

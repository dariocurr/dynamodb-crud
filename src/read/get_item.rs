use crate::{common, read};

use aws_sdk_dynamodb::{Client, error, operation, types};
use serde::Serialize;
use serde_dynamo::{Error, Result};
use std::collections;

/// get item operation
#[derive(Clone, Debug, Default, PartialEq)]
struct GetItemInput {
    keys: collections::HashMap<String, types::AttributeValue>,
    return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    single_read_operation: read::common::SingleReadInput,
}

/// Get item operation.
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
/// use dynamodb_crud::{common, read};
///
/// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let get_item = read::get_item::GetItem {
///     keys: common::key::Keys {
///         partition_key: common::key::Key {
///             name: "id".to_string(),
///             value: "1".to_string(),
///         },
///         ..Default::default()
///     },
///     single_read_args: read::common::SingleReadArgs {
///         table_name: "users".to_string(),
///         ..Default::default()
///     },
///     ..Default::default()
/// };
/// get_item.send(client).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GetItem<T> {
    /// The primary key of the item to retrieve.
    pub keys: common::key::Keys<T>,
    /// Whether to return the consumed capacity information.
    pub return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    /// Additional read operation arguments (table name, consistent read, selection).
    pub single_read_args: read::common::SingleReadArgs,
}

impl<T: Serialize> TryFrom<GetItem<T>> for GetItemInput {
    type Error = Error;

    fn try_from(get_item: GetItem<T>) -> Result<Self> {
        let single_operation: read::common::SingleReadInput = get_item.single_read_args.into();
        let keys = get_item.keys.try_into()?;
        let operation = Self {
            keys,
            return_consumed_capacity: get_item.return_consumed_capacity,
            single_read_operation: single_operation,
        };
        Ok(operation)
    }
}

impl<T: Serialize> GetItem<T> {
    /// Execute the get item operation.
    pub async fn send(
        self,
        client: &Client,
    ) -> Result<
        operation::get_item::GetItemOutput,
        error::SdkError<operation::get_item::GetItemError>,
    > {
        let get_item: GetItemInput = self.try_into().map_err(error::BuildError::other)?;
        let builder = client
            .get_item()
            .set_key(Some(get_item.keys))
            .set_return_consumed_capacity(get_item.return_consumed_capacity);
        crate::apply_single_read_operation!(builder, get_item.single_read_operation)
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
        GetItem {
            keys: common::key::Keys {
                partition_key: common::key::Key {
                    name: "a".to_string(),
                    value: Value::String(
                        "b".to_string()
                    ),
                },
                ..Default::default()
            },
            single_read_args: read::common::SingleReadArgs {
                table_name: "c".to_string(),
                ..Default::default()
            },
            ..Default::default()
        },
        GetItemInput {
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
            single_read_operation: read::common::SingleReadInput {
                table_name: "c".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    )]
    #[case::full(
        GetItem {
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
            return_consumed_capacity: Some(
                types::ReturnConsumedCapacity::Indexes
            ),
            single_read_args: read::common::SingleReadArgs {
                consistent_read: Some(false),
                selection: Some(
                    common::selection::SelectionMap::Leaves(
                        vec![
                            "e".to_string(),
                            "f".to_string()
                        ]
                    )
                ),
                table_name: "g".to_string(),
            },
        },
        GetItemInput {
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
            return_consumed_capacity: Some(
                types::ReturnConsumedCapacity::Indexes
            ),
            single_read_operation: read::common::SingleReadInput {
                consistent_read: Some(false),
                expression_attribute_names: Some(
                    collections::HashMap::from(
                        [
                            ("#e".to_string(), "e".to_string()),
                            ("#f".to_string(), "f".to_string()),
                        ]
                    )
                ),
                projection_expression: Some(
                    "#e, #f".to_string()
                ),
                table_name: "g".to_string(),
            },
        }
    )]
    fn test_get_item(#[case] args: GetItem<Value>, #[case] expected: GetItemInput) {
        let actual: GetItemInput = args.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

use crate::{common, read};

use aws_sdk_dynamodb::{Client, error, operation, types};
use indexmap::IndexMap;
use serde::Serialize;
use serde_dynamo::{Error, Result};
use std::collections;

/// Batch get item operation.
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
/// use dynamodb_crud::{common, read};
/// use indexmap::IndexMap;
///
/// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let batch_get = read::batch_get_item::BatchGetItem {
///     items: IndexMap::from([(
///         read::common::SingleReadArgs {
///             table_name: "users".to_string(),
///             ..Default::default()
///         },
///         vec![
///             common::key::Keys {
///                 partition_key: common::key::Key {
///                     name: "id".to_string(),
///                     value: "1".to_string(),
///                 },
///                 ..Default::default()
///             },
///         ],
///     )]),
///     ..Default::default()
/// };
/// batch_get.send(client).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default, PartialEq)]
pub struct BatchGetItem<T> {
    /// A map of read arguments to lists of keys to retrieve.
    pub items: IndexMap<read::common::SingleReadArgs, Vec<common::key::Keys<T>>>,
    /// Whether to return the consumed capacity information.
    pub return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
}

impl<T: Serialize> TryFrom<BatchGetItem<T>> for operation::batch_get_item::BatchGetItemInput {
    type Error = Error;

    fn try_from(batch_get_item: BatchGetItem<T>) -> Result<Self> {
        let mut request_items = collections::HashMap::with_capacity(batch_get_item.items.len());
        for (args, keys) in batch_get_item.items {
            let single_operation: read::common::SingleReadInput = args.into();
            let mut serialized_keys = Vec::with_capacity(keys.len());
            for key in keys {
                let key = key.try_into()?;
                serialized_keys.push(key);
            }
            let keys_and_attributes = types::KeysAndAttributes::builder()
                .set_consistent_read(single_operation.consistent_read)
                .set_expression_attribute_names(single_operation.expression_attribute_names)
                .set_keys(Some(serialized_keys))
                .set_projection_expression(single_operation.projection_expression)
                .build()
                .unwrap();
            request_items.insert(single_operation.table_name, keys_and_attributes);
        }
        let input = Self::builder()
            .set_request_items(Some(request_items))
            .set_return_consumed_capacity(batch_get_item.return_consumed_capacity)
            .build()
            .unwrap();
        Ok(input)
    }
}

impl<T: Serialize> BatchGetItem<T> {
    /// Execute the batch get item operation.
    pub async fn send(
        self,
        client: &Client,
    ) -> Result<
        operation::batch_get_item::BatchGetItemOutput,
        error::SdkError<operation::batch_get_item::BatchGetItemError>,
    > {
        let batch_get_item: operation::batch_get_item::BatchGetItemInput =
            self.try_into().map_err(error::BuildError::other)?;
        client
            .batch_get_item()
            .set_request_items(batch_get_item.request_items)
            .set_return_consumed_capacity(batch_get_item.return_consumed_capacity)
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
        BatchGetItem {
            items: IndexMap::from(
                [(
                    read::common::SingleReadArgs {
                        table_name: "a".to_string(),
                        ..Default::default()
                    },
                    vec![
                        common::key::Keys {
                            partition_key: common::key::Key {
                                name: "b".to_string(),
                                value: Value::String(
                                    "c".to_string()
                                ),
                            },
                            ..Default::default()
                        }
                    ],
                )]
            ),
            ..Default::default()
        },
        operation::batch_get_item::BatchGetItemInput::builder()
            .set_request_items(
                Some(
                    collections::HashMap::from(
                        [(
                            "a".to_string(),
                            types::KeysAndAttributes::builder()
                                .set_keys(
                                    Some(
                                        vec![
                                            collections::HashMap::from(
                                                [
                                                    (
                                                        "b".to_string(),
                                                        types::AttributeValue::S(
                                                            "c".to_string()
                                                        )
                                                    ),
                                                ]
                                            )
                                        ]
                                    )
                                )
                                .build()
                                .unwrap(),
                        )]
                    )
                )
            )
            .build()
            .unwrap()
    )]
    #[case::full(
        BatchGetItem {
            items: IndexMap::from(
                [
                    (
                        read::common::SingleReadArgs {
                            consistent_read: Some(false),
                            selection: Some(
                                common::selection::SelectionMap::Leaves(
                                    vec![
                                        "a".to_string(),
                                        "b".to_string()
                                    ]
                                )
                            ),
                            table_name: "c".to_string(),
                        },
                        vec![
                            common::key::Keys {
                                partition_key: common::key::Key {
                                    name: "d".to_string(),
                                    value: Value::String(
                                        "e".to_string()
                                    ),
                                },
                                sort_key: Some(
                                    common::key::Key {
                                        name: "f".to_string(),
                                        value: Value::String(
                                            "g".to_string()
                                        ),
                                    }
                                ),
                            }
                        ],
                    ),
                    (
                        read::common::SingleReadArgs {
                            consistent_read: Some(true),
                            selection: Some(
                                common::selection::SelectionMap::Leaves(
                                    vec![
                                        "h".to_string(),
                                        "i".to_string()
                                    ]
                                )
                            ),
                            table_name: "j".to_string(),
                        },
                        vec![
                            common::key::Keys {
                                partition_key: common::key::Key {
                                    name: "k".to_string(),
                                    value: Value::String(
                                        "l".to_string()
                                    ),
                                },
                                sort_key: Some(
                                    common::key::Key {
                                        name: "m".to_string(),
                                        value: Value::String(
                                            "n".to_string()
                                        ),
                                    }
                                ),
                            }
                        ],
                    ),
                ]
            ),
            return_consumed_capacity: Some(
                types::ReturnConsumedCapacity::Total
            ),
        },
        operation::batch_get_item::BatchGetItemInput::builder()
            .set_request_items(
                Some(
                    collections::HashMap::from(
                        [
                            (
                                "c".to_string(),
                                types::KeysAndAttributes::builder()
                                    .set_consistent_read(
                                        Some(false)
                                    )
                                    .set_expression_attribute_names(
                                        Some(
                                            collections::HashMap::from(
                                                [
                                                    ("#a".to_string(), "a".to_string()),
                                                    ("#b".to_string(), "b".to_string()),
                                                ]
                                            )
                                        )
                                    )
                                    .set_keys(
                                        Some(
                                            vec![
                                                collections::HashMap::from(
                                                    [
                                                        (
                                                            "d".to_string(),
                                                            types::AttributeValue::S(
                                                                "e".to_string()
                                                            )
                                                        ),
                                                        (
                                                            "f".to_string(),
                                                            types::AttributeValue::S(
                                                                "g".to_string()
                                                            )
                                                        ),
                                                    ]
                                                )
                                            ]
                                        )
                                    )
                                    .set_projection_expression(
                                        Some(
                                            "#a, #b".to_string()
                                        )
                                    )
                                    .build()
                                    .unwrap(),
                            ),
                            (
                                "j".to_string(),
                                types::KeysAndAttributes::builder()
                                    .set_consistent_read(
                                        Some(true)
                                    )
                                    .set_expression_attribute_names(
                                        Some(
                                            collections::HashMap::from(
                                                [
                                                    ("#h".to_string(), "h".to_string()),
                                                    ("#i".to_string(), "i".to_string()),
                                                ]
                                            )
                                        )
                                    )
                                    .set_keys(
                                        Some(
                                            vec![
                                                collections::HashMap::from(
                                                    [
                                                        (
                                                            "k".to_string(),
                                                            types::AttributeValue::S(
                                                                "l".to_string()
                                                            )
                                                        ),
                                                        (
                                                            "m".to_string(),
                                                            types::AttributeValue::S(
                                                                "n".to_string()
                                                            )
                                                        ),
                                                    ]
                                                )
                                            ]
                                        )
                                    )
                                    .set_projection_expression(
                                        Some(
                                            "#h, #i".to_string()
                                        )
                                    )
                                    .build()
                                    .unwrap(),
                            ),
                        ]
                    )
                )
            )
            .set_return_consumed_capacity(
                Some(
                    types::ReturnConsumedCapacity::Total
                )
            )
            .build()
            .unwrap()
    )]
    fn test_batch_get_item(
        #[case] args: BatchGetItem<Value>,
        #[case] expected: operation::batch_get_item::BatchGetItemInput,
    ) {
        let actual: operation::batch_get_item::BatchGetItemInput = args.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

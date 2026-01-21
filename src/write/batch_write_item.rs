use crate::common;

use aws_sdk_dynamodb::{Client, error, operation, types};
use serde::Serialize;
use serde_dynamo::{Error, Result, to_item};
use std::collections;

/// A put item request within a batch write operation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BatchWriteItemRequestPutItem<T> {
    /// The item to put into the table.
    pub item: T,
}

/// A delete item request within a batch write operation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BatchWriteItemRequestDeleteItem<T> {
    /// The primary key of the item to delete.
    pub keys: common::key::Keys<T>,
}

/// A single request within a batch write operation.
///
/// Each request can be either a PutItem (create/replace) or DeleteItem (remove) operation.
#[derive(Clone, Debug, PartialEq)]
pub enum BatchWriteItemRequest<T> {
    /// Put item request - creates or replaces an item.
    PutItem(BatchWriteItemRequestPutItem<T>),
    /// Delete item request - removes an item by its primary key.
    DeleteItem(BatchWriteItemRequestDeleteItem<T>),
}

impl<T: Serialize> TryFrom<BatchWriteItemRequest<T>> for types::WriteRequest {
    type Error = Error;

    fn try_from(write_request: BatchWriteItemRequest<T>) -> Result<Self> {
        let builder = match write_request {
            BatchWriteItemRequest::PutItem(put_item) => {
                let item = to_item(put_item.item)?;
                let put_request = types::PutRequest::builder()
                    .set_item(Some(item))
                    .build()
                    .unwrap();
                Self::builder().set_put_request(Some(put_request))
            }
            BatchWriteItemRequest::DeleteItem(delete_item) => {
                let keys = delete_item.keys.try_into()?;
                let delete_request = types::DeleteRequest::builder()
                    .set_key(Some(keys))
                    .build()
                    .unwrap();
                Self::builder().set_delete_request(Some(delete_request))
            }
        };
        let request = builder.build();
        Ok(request)
    }
}

/// Batch write item operation.
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
/// use dynamodb_crud::write;
/// use std::collections::HashMap;
///
/// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let batch_write = write::batch_write_item::BatchWriteItem {
///     request_items: HashMap::from([(
///         "users".to_string(),
///         vec![
///             write::batch_write_item::BatchWriteItemRequest::PutItem(
///                 write::batch_write_item::BatchWriteItemRequestPutItem {
///                     item: serde_json::json!({"id": "1", "name": "John"}),
///                 },
///             ),
///         ],
///     )]),
///     ..Default::default()
/// };
/// batch_write.send(client).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BatchWriteItem<T> {
    /// A map of table names to lists of write requests.
    pub request_items: collections::HashMap<String, Vec<BatchWriteItemRequest<T>>>,
    /// Whether to return the consumed capacity information.
    pub return_consumed_capacity: Option<types::ReturnConsumedCapacity>,
    /// Whether to return item collection metrics.
    pub return_item_collection_metrics: Option<types::ReturnItemCollectionMetrics>,
}

impl<T: Serialize> TryFrom<BatchWriteItem<T>> for operation::batch_write_item::BatchWriteItemInput {
    type Error = Error;

    fn try_from(batch_write_item: BatchWriteItem<T>) -> Result<Self> {
        let mut request_items =
            collections::HashMap::with_capacity(batch_write_item.request_items.len());
        for (table_name, table_request_items) in batch_write_item.request_items {
            let mut serialized_table_request_items = Vec::with_capacity(table_request_items.len());
            for request_item in table_request_items {
                let request_item = request_item.try_into()?;
                serialized_table_request_items.push(request_item);
            }
            request_items.insert(table_name, serialized_table_request_items);
        }
        let operation = Self::builder()
            .set_request_items(Some(request_items))
            .set_return_consumed_capacity(batch_write_item.return_consumed_capacity)
            .set_return_item_collection_metrics(batch_write_item.return_item_collection_metrics)
            .build()
            .unwrap();
        Ok(operation)
    }
}

impl<T: Serialize> BatchWriteItem<T> {
    /// Execute the batch write item operation.
    pub async fn send(
        self,
        client: &Client,
    ) -> Result<
        operation::batch_write_item::BatchWriteItemOutput,
        error::SdkError<operation::batch_write_item::BatchWriteItemError>,
    > {
        let batch_write_item: operation::batch_write_item::BatchWriteItemInput =
            self.try_into().map_err(error::BuildError::other)?;
        client
            .batch_write_item()
            .set_request_items(batch_write_item.request_items)
            .set_return_consumed_capacity(batch_write_item.return_consumed_capacity)
            .set_return_item_collection_metrics(batch_write_item.return_item_collection_metrics)
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use serde_json::{Value, json};

    #[rstest]
    #[case::empty(
        BatchWriteItem {
            request_items: collections::HashMap::from(
                [(
                    "a".to_string(),
                    vec![
                        BatchWriteItemRequest::DeleteItem(
                            BatchWriteItemRequestDeleteItem {
                                keys: common::key::Keys {
                                    partition_key: common::key::Key {
                                        name: "b".to_string(),
                                        value: Value::String(
                                            "c".to_string()
                                        ),
                                    },
                                    ..Default::default()
                                },
                            }
                        ),
                        BatchWriteItemRequest::PutItem(
                            BatchWriteItemRequestPutItem {
                                item: json!(
                                    {
                                        "d": "e"
                                    }
                                ),
                            }
                        )
                    ],
                )]
            ),
            ..Default::default()
        },
        operation::batch_write_item::BatchWriteItemInput::builder()
            .set_request_items(
                Some(
                    collections::HashMap::from(
                        [(
                            "a".to_string(),
                            vec![
                                types::WriteRequest::builder()
                                    .set_delete_request(
                                        Some(
                                            types::DeleteRequest::builder()
                                                .set_key(
                                                    Some(
                                                        collections::HashMap::from(
                                                            [(
                                                                "b".to_string(),
                                                                types::AttributeValue::S(
                                                                    "c".to_string()
                                                                ),
                                                            )]
                                                        )
                                                    )
                                                )
                                                .build()
                                                .unwrap(),
                                        )
                                    )
                                    .build(),
                                types::WriteRequest::builder()
                                    .set_put_request(
                                        Some(
                                            types::PutRequest::builder()
                                                .set_item(
                                                    Some(
                                                        collections::HashMap::from(
                                                            [(
                                                                "d".to_string(),
                                                                types::AttributeValue::S(
                                                                    "e".to_string()
                                                                ),
                                                            )]
                                                        )
                                                    )
                                                )
                                                .build()
                                                .unwrap(),
                                        )
                                    )
                                    .build(),
                            ],
                        )]
                    )
                )
            )
            .build()
            .unwrap()
    )]
    #[case::full(
        BatchWriteItem {
            request_items: collections::HashMap::from(
                [
                    (
                        "a".to_string(),
                        vec![
                            BatchWriteItemRequest::DeleteItem(
                                BatchWriteItemRequestDeleteItem {
                                    keys: common::key::Keys {
                                        partition_key: common::key::Key {
                                            name: "b".to_string(),
                                            value: Value::String(
                                                "c".to_string()
                                            ),
                                        },
                                        sort_key: Some(
                                            common::key::Key {
                                                name: "d".to_string(),
                                                value: Value::String(
                                                    "e".to_string()
                                                ),
                                            }
                                        ),
                                    },
                                }
                            ),
                            BatchWriteItemRequest::PutItem(
                                BatchWriteItemRequestPutItem {
                                    item: json!(
                                        {
                                            "f": "g"
                                        }
                                    ),
                                }
                            ),
                        ],
                    ),
                    (
                        "h".to_string(),
                        vec![
                            BatchWriteItemRequest::DeleteItem(
                                BatchWriteItemRequestDeleteItem {
                                    keys: common::key::Keys {
                                        partition_key: common::key::Key {
                                            name: "i".to_string(),
                                            value: Value::String(
                                                "j".to_string()
                                            ),
                                        },
                                        sort_key: Some(
                                            common::key::Key {
                                                name: "k".to_string(),
                                                value: Value::String(
                                                    "l".to_string()
                                                ),
                                            }
                                        ),
                                    },
                                }
                            ),
                            BatchWriteItemRequest::PutItem(
                                BatchWriteItemRequestPutItem {
                                    item: json!(
                                        {
                                            "m": "n"
                                        }
                                    ),
                                }
                            ),
                        ],
                    ),
                ]
            ),
            return_consumed_capacity: Some(
                types::ReturnConsumedCapacity::Indexes
            ),
            return_item_collection_metrics: Some(
                types::ReturnItemCollectionMetrics::None
            ),
        },
        operation::batch_write_item::BatchWriteItemInput::builder()
            .set_request_items(
                Some(
                    collections::HashMap::from(
                        [
                            (
                                "a".to_string(),
                                vec![
                                    types::WriteRequest::builder()
                                        .set_delete_request(
                                            Some(
                                                types::DeleteRequest::builder()
                                                    .set_key(
                                                        Some(
                                                            collections::HashMap::from(
                                                                [
                                                                    (
                                                                        "b".to_string(),
                                                                        types::AttributeValue::S(
                                                                            "c".to_string()
                                                                        )
                                                                    ),
                                                                    (
                                                                        "d".to_string(),
                                                                        types::AttributeValue::S(
                                                                            "e".to_string()
                                                                        )
                                                                    ),
                                                                ]
                                                            )
                                                        )
                                                    )
                                                    .build()
                                                    .unwrap(),
                                            )
                                        )
                                        .build(),
                                    types::WriteRequest::builder()
                                        .set_put_request(
                                            Some(
                                                types::PutRequest::builder()
                                                    .set_item(
                                                        Some(
                                                            collections::HashMap::from(
                                                                [(
                                                                    "f".to_string(),
                                                                    types::AttributeValue::S(
                                                                        "g".to_string()
                                                                    ),
                                                                )]
                                                            )
                                                        )
                                                    )
                                                    .build()
                                                    .unwrap(),
                                            )
                                        )
                                        .build(),
                                ],
                            ),
                            (
                                "h".to_string(),
                                vec![
                                    types::WriteRequest::builder()
                                        .set_delete_request(
                                            Some(
                                                types::DeleteRequest::builder()
                                                    .set_key(
                                                        Some(
                                                            collections::HashMap::from(
                                                                [
                                                                    (
                                                                        "i".to_string(),
                                                                        types::AttributeValue::S(
                                                                            "j".to_string()
                                                                        )
                                                                    ),
                                                                    (
                                                                        "k".to_string(),
                                                                        types::AttributeValue::S(
                                                                            "l".to_string()
                                                                        )
                                                                    ),
                                                                ]
                                                            )
                                                        )
                                                    )
                                                    .build()
                                                    .unwrap(),
                                            )
                                        )
                                        .build(),
                                    types::WriteRequest::builder()
                                        .set_put_request(
                                            Some(
                                                types::PutRequest::builder()
                                                    .set_item(
                                                        Some(
                                                            collections::HashMap::from(
                                                                [(
                                                                    "m".to_string(),
                                                                    types::AttributeValue::S(
                                                                        "n".to_string()
                                                                    ),
                                                                )]
                                                            )
                                                        )
                                                    )
                                                    .build()
                                                    .unwrap(),
                                            )
                                        )
                                        .build(),
                                ],
                            ),
                        ]
                    )
                )
            )
            .set_return_consumed_capacity(
                Some(
                    types::ReturnConsumedCapacity::Indexes
                )
            )
            .set_return_item_collection_metrics(
                Some(
                    types::ReturnItemCollectionMetrics::None
                )
            )
            .build()
            .unwrap()
    )]
    fn test_batch_write_item(
        #[case] args: BatchWriteItem<Value>,
        #[case] expected: operation::batch_write_item::BatchWriteItemInput,
    ) {
        let actual: operation::batch_write_item::BatchWriteItemInput = args.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

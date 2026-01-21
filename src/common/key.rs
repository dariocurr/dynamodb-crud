use aws_sdk_dynamodb::types;
use serde::Serialize;
use serde_dynamo::{Error, Result, to_attribute_value};
use std::collections;

/// Key component.
///
/// ```rust
/// use dynamodb_crud::common::key;
///
/// let key = key::Key {
///     name: "id".to_string(),
///     value: "1".to_string(),
/// };
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Key<T> {
    /// The attribute name of the key.
    pub name: String,
    /// The value of the key.
    pub value: T,
}

/// Primary key (partition key and optional sort key).
///
/// ```rust
/// use dynamodb_crud::common::key;
///
/// let keys = key::Keys {
///     partition_key: key::Key {
///         name: "id".to_string(),
///         value: "1".to_string(),
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Keys<T> {
    /// The partition key (required).
    pub partition_key: Key<T>,
    /// The sort key (optional, only for tables with composite primary keys).
    pub sort_key: Option<Key<T>>,
}

impl<T: Serialize> TryFrom<Keys<T>> for collections::HashMap<String, types::AttributeValue> {
    type Error = Error;

    fn try_from(key: Keys<T>) -> Result<Self> {
        let partition_key_value = to_attribute_value(key.partition_key.value)?;
        let mut keys = Self::from([(key.partition_key.name, partition_key_value)]);
        if let Some(sort_key) = key.sort_key {
            let sort_key_value = to_attribute_value(sort_key.value)?;
            keys.insert(sort_key.name, sort_key_value);
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use serde_json::Value;

    #[rstest]
    #[case::partition_key_only_string(
        Keys {
            partition_key: Key {
                name: "a".to_string(),
                value: Value::String(
                    "b".to_string()
                ),
            },
            ..Default::default()
        },
        collections::HashMap::from(
            [(
                "a".to_string(),
                types::AttributeValue::S(
                    "b".to_string()
                ),
            )]
        )
    )]
    #[case::partition_key_only_number(
        Keys {
            partition_key: Key {
                name: "a".to_string(),
                value: Value::Number(
                    42.into()
                ),
            },
            ..Default::default()
        },
        collections::HashMap::from(
            [(
                "a".to_string(),
                types::AttributeValue::N(
                    "42".to_string()
                ),
            )]
        )
    )]
    #[case::partition_key_and_sort_key_strings(
        Keys {
            partition_key: Key {
                name: "a".to_string(),
                value: Value::String(
                    "b".to_string()
                ),
            },
            sort_key: Some(
                Key {
                    name: "c".to_string(),
                    value: Value::String(
                        "d".to_string()
                    ),
                }
            ),
        },
        collections::HashMap::from(
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
        )
    )]
    #[case::partition_key_string_sort_key_number(
        Keys {
            partition_key: Key {
                name: "a".to_string(),
                value: Value::String(
                    "b".to_string()
                ),
            },
            sort_key: Some(
                Key {
                    name: "c".to_string(),
                    value: Value::Number(
                        100.into()
                    ),
                }
            ),
        },
        collections::HashMap::from(
            [
                (
                    "a".to_string(),
                    types::AttributeValue::S(
                        "b".to_string()
                    )
                ),
                (
                    "c".to_string(),
                    types::AttributeValue::N(
                        "100".to_string()
                    )
                ),
            ]
        )
    )]
    fn test_keys_to_hash_map(
        #[case] keys: Keys<Value>,
        #[case] expected: collections::HashMap<String, types::AttributeValue>,
    ) {
        let actual: collections::HashMap<String, types::AttributeValue> = keys.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

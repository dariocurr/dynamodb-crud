use crate::{common, write};

use aws_sdk_dynamodb::{Client, error, operation, types};
use indexmap::IndexMap;
use serde::Serialize;
use serde_dynamo::{Error, Result, to_attribute_value};
use std::collections;

/// Separator for attribute path components.
const PATH_SEPARATOR: &str = ".";

/// Map for ADD and DELETE operations.
#[derive(Clone, Debug, PartialEq)]
pub enum AddOrDeleteInputsMap<T> {
    /// Leaf operations - flat list of (attribute_name, value) pairs.
    Leaves(Vec<(String, T)>),
    /// Node operations - nested operations for hierarchical attribute paths.
    Node(IndexMap<String, AddOrDeleteInputsMap<T>>),
}

impl<T: Serialize> AddOrDeleteInputsMap<T> {
    fn get_add_or_delete_expression_recursive(
        self,
        keys: &[String],
        index: &mut usize,
    ) -> Result<common::ExpressionInput> {
        let mut operations = Vec::new();
        match self {
            Self::Leaves(leaves) => {
                for (key, value) in leaves {
                    let (placeholder, new_keys) = common::add_placeholder(keys, &key);
                    let path = new_keys.join(PATH_SEPARATOR);
                    let value = to_attribute_value(value)?;
                    let value_placeholder = format!(":add_or_delete{index}");
                    *index += 1;
                    let expression = format!("{path} {value_placeholder}");
                    let expression_attribute_names =
                        collections::HashMap::from([(placeholder, key)]);
                    let expression_attribute_values =
                        collections::HashMap::from([(value_placeholder, value)]);
                    let operation = common::ExpressionInput {
                        expression,
                        expression_attribute_names,
                        expression_attribute_values,
                    };
                    operations.push(operation);
                }
            }
            Self::Node(map) => {
                for (key, value) in map {
                    let (placeholder, new_keys) = common::add_placeholder(keys, &key);
                    let mut operation =
                        value.get_add_or_delete_expression_recursive(&new_keys, index)?;
                    operation
                        .expression_attribute_names
                        .insert(placeholder, key);
                    operations.push(operation);
                }
            }
        }
        let operation = common::ExpressionInput::merge(" ", operations);
        Ok(operation)
    }
}

/// SET operation for updating attributes.
///
/// ```rust
/// use dynamodb_crud::write::update_item;
///
/// let assign = update_item::SetInput::Assign("value".to_string());
/// let increment = update_item::SetInput::Increment(10);
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum SetInput<T> {
    /// Assign a new value to the attribute (replaces existing value).
    Assign(T),
    /// Increment a numeric attribute by the specified value.
    Increment(T),
    /// Decrement a numeric attribute by the specified value.
    Decrement(T),
    /// Append values to the end of a list attribute.
    ListAppend(T),
    /// Prepend values to the beginning of a list attribute.
    ListPrepend(T),
    /// Assign a value only if the attribute doesn't exist.
    IfNotExists(T),
}

impl<T> SetInput<T> {
    fn get_set_expression(self, path: &str, value_placeholder: &str) -> (T, String) {
        match self {
            SetInput::Assign(value) => {
                let expression = format!("{path} = {value_placeholder}");
                (value, expression)
            }
            SetInput::Increment(value) => {
                let expression = format!("{path} = {path} + {value_placeholder}");
                (value, expression)
            }
            SetInput::Decrement(value) => {
                let expression = format!("{path} = {path} - {value_placeholder}");
                (value, expression)
            }
            SetInput::ListAppend(value) => {
                let expression = format!("{path} = list_append({path}, {value_placeholder})");
                (value, expression)
            }
            SetInput::ListPrepend(value) => {
                let expression = format!("{path} = list_append({value_placeholder}, {path})");
                (value, expression)
            }
            SetInput::IfNotExists(value) => {
                let expression = format!("{path} = if_not_exists({path}, {value_placeholder})");
                (value, expression)
            }
        }
    }
}

/// Map for SET operations.
#[derive(Clone, Debug, PartialEq)]
pub enum SetInputsMap<T> {
    /// Leaf operations - flat list of (attribute_name, set_operation) pairs.
    Leaves(Vec<(String, SetInput<T>)>),
    /// Node operations - nested operations for hierarchical attribute paths.
    Node(IndexMap<String, SetInputsMap<T>>),
}

impl<T: Serialize> SetInputsMap<T> {
    fn get_set_expression_recursive(
        self,
        keys: &[String],
        index: &mut usize,
    ) -> Result<common::ExpressionInput> {
        let mut operations = Vec::new();
        match self {
            Self::Leaves(leaves) => {
                for (key, set_operation) in leaves {
                    let (placeholder, new_keys) = common::add_placeholder(keys, &key);
                    let path = new_keys.join(PATH_SEPARATOR);
                    let value_placeholder = format!(":set{index}");
                    let (value, expression) =
                        set_operation.get_set_expression(&path, &value_placeholder);
                    let value = to_attribute_value(value)?;
                    let expression_attribute_names =
                        collections::HashMap::from([(placeholder, key)]);
                    let expression_attribute_values =
                        collections::HashMap::from([(value_placeholder, value)]);
                    *index += 1;
                    let operation = common::ExpressionInput {
                        expression,
                        expression_attribute_names,
                        expression_attribute_values,
                    };
                    operations.push(operation);
                }
            }
            Self::Node(map) => {
                for (key, value) in map {
                    let (placeholder, new_keys) = common::add_placeholder(keys, &key);
                    let mut operation = value.get_set_expression_recursive(&new_keys, index)?;
                    operation
                        .expression_attribute_names
                        .insert(placeholder, key);
                    operations.push(operation);
                }
            }
        }
        let operation = common::ExpressionInput::merge(", ", operations);
        Ok(operation)
    }
}

/// Update expression map.
///
/// ```rust
/// use dynamodb_crud::write::update_item;
///
/// let expr = update_item::UpdateExpressionMap::Set(
///     update_item::SetInputsMap::Leaves(vec![
///         ("name".to_string(), update_item::SetInput::Assign("New".to_string())),
///     ]),
/// );
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum UpdateExpressionMap<T> {
    /// ADD operations - add values to numbers or sets.
    Add(AddOrDeleteInputsMap<T>),
    /// DELETE operations - delete values from sets.
    Delete(AddOrDeleteInputsMap<T>),
    /// REMOVE operations - remove attributes from items.
    Remove(common::selection::SelectionMap),
    /// SET operations - set or modify attribute values.
    Set(SetInputsMap<T>),
    /// Combined operations - multiple operation types in a single update expression.
    Combined(Vec<UpdateExpressionMap<T>>),
}

impl<T: Serialize> UpdateExpressionMap<T> {
    fn get_update_expression_recursive(
        self,
        keys: &[String],
        index: &mut usize,
    ) -> Result<common::ExpressionInput> {
        match self {
            Self::Add(add_operations) => {
                let mut operation =
                    add_operations.get_add_or_delete_expression_recursive(keys, index)?;
                operation.expression = format!("ADD {}", operation.expression);
                Ok(operation)
            }
            Self::Delete(delete_operations) => {
                let mut operation =
                    delete_operations.get_add_or_delete_expression_recursive(keys, index)?;
                operation.expression = format!("DELETE {}", operation.expression);
                Ok(operation)
            }
            Self::Remove(remove_operations) => {
                let mut operation = remove_operations.get_selection_operation_recursive(keys);
                operation.expression = format!("REMOVE {}", operation.expression);
                Ok(operation)
            }
            Self::Set(set_operations) => {
                let mut operation = set_operations.get_set_expression_recursive(keys, index)?;
                operation.expression = format!("SET {}", operation.expression);
                Ok(operation)
            }
            Self::Combined(combined_operations) => {
                let mut operations = Vec::with_capacity(combined_operations.len());
                for operation in combined_operations {
                    let operation = operation.get_update_expression_recursive(keys, index)?;
                    operations.push(operation);
                }
                let operation = common::ExpressionInput::merge(" ", operations);
                Ok(operation)
            }
        }
    }
}

impl<T: Serialize> TryFrom<UpdateExpressionMap<T>> for common::ExpressionInput {
    type Error = Error;

    fn try_from(update_expression_map: UpdateExpressionMap<T>) -> Result<Self> {
        let mut index = 0;
        update_expression_map.get_update_expression_recursive(&[], &mut index)
    }
}

/// update item operation
#[derive(Clone, Debug, Default, PartialEq)]
struct UpdateItemInput {
    keys: collections::HashMap<String, types::AttributeValue>,
    update_expression: String,
    write_operation: write::common::WriteInput,
}

/// Update item operation.
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
/// use dynamodb_crud::{common, write};
///
/// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let update_item = write::update_item::UpdateItem {
///     keys: common::key::Keys {
///         partition_key: common::key::Key {
///             name: "id".to_string(),
///             value: "1".to_string(),
///         },
///         ..Default::default()
///     },
///     update_expression: write::update_item::UpdateExpressionMap::Set(
///         write::update_item::SetInputsMap::Leaves(vec![
///             ("name".to_string(), write::update_item::SetInput::Assign("New".to_string())),
///         ]),
///     ),
///     write_args: write::common::WriteArgs {
///         table_name: "users".to_string(),
///         ..Default::default()
///     },
/// };
/// update_item.send(client).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct UpdateItem<T> {
    /// The primary key of the item to update.
    pub keys: common::key::Keys<T>,
    /// The update expression specifying what changes to make.
    pub update_expression: UpdateExpressionMap<T>,
    /// Additional write operation arguments (table name, condition, return values, etc.).
    pub write_args: write::common::WriteArgs<T>,
}

impl<T: Serialize> TryFrom<UpdateItem<T>> for UpdateItemInput {
    type Error = Error;

    fn try_from(update_item: UpdateItem<T>) -> Result<Self> {
        let keys = update_item.keys.try_into()?;
        let mut write_operation: write::common::WriteInput = update_item.write_args.try_into()?;
        let operation = update_item.update_expression.try_into()?;
        let update_expression = write_operation.merge_expression(operation);
        let operation = Self {
            keys,
            update_expression,
            write_operation,
        };
        Ok(operation)
    }
}

impl<T: Serialize> UpdateItem<T> {
    /// Execute the update item operation.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "dynamodb_crud.update_item", err)
    )]
    pub async fn send(
        self,
        client: &Client,
    ) -> Result<
        operation::update_item::UpdateItemOutput,
        error::SdkError<operation::update_item::UpdateItemError>,
    > {
        let update_item: UpdateItemInput = self.try_into().map_err(error::BuildError::other)?;
        let builder = client
            .update_item()
            .set_key(Some(update_item.keys))
            .update_expression(update_item.update_expression);
        crate::apply_write_operation!(builder, update_item.write_operation)
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
    #[case::set_assign(
        UpdateExpressionMap::Set(
            SetInputsMap::Leaves(
                vec![
                    (
                        "attr".to_string(),
                        SetInput::Assign(
                            Value::String(
                                "val".to_string()
                            )
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "SET #attr = :set0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#attr".to_string(), "attr".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::S(
                            "val".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::set_increment(
        UpdateExpressionMap::Set(
            SetInputsMap::Leaves(
                vec![
                    (
                        "count".to_string(),
                        SetInput::Increment(
                            Value::Number(
                                5.into()
                            )
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "SET #count = #count + :set0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#count".to_string(), "count".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::N(
                            "5".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::set_decrement(
        UpdateExpressionMap::Set(
            SetInputsMap::Leaves(
                vec![
                    (
                        "count".to_string(),
                        SetInput::Decrement(
                            Value::Number(
                                3.into()
                            )
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "SET #count = #count - :set0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#count".to_string(), "count".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::N(
                            "3".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::set_list_append(
        UpdateExpressionMap::Set(
            SetInputsMap::Leaves(
                vec![
                    (
                        "list".to_string(),
                        SetInput::ListAppend(
                            Value::Array(
                                vec![
                                    Value::String(
                                        "item".to_string()
                                    )
                                ]
                            )
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "SET #list = list_append(#list, :set0)".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#list".to_string(), "list".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::L(
                            vec![
                                types::AttributeValue::S(
                                    "item".to_string()
                                )
                            ]
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::set_list_prepend(
        UpdateExpressionMap::Set(
            SetInputsMap::Leaves(
                vec![
                    (
                        "list".to_string(),
                        SetInput::ListPrepend(
                            Value::Array(
                                vec![
                                    Value::String(
                                        "item".to_string()
                                    )
                                ]
                            )
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "SET #list = list_append(:set0, #list)".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#list".to_string(), "list".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::L(
                            vec![
                                types::AttributeValue::S(
                                    "item".to_string()
                                )
                            ]
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::set_if_not_exists(
        UpdateExpressionMap::Set(
            SetInputsMap::Leaves(
                vec![
                    (
                        "attr".to_string(),
                        SetInput::IfNotExists(
                            Value::String(
                                "default".to_string()
                            )
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "SET #attr = if_not_exists(#attr, :set0)".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#attr".to_string(), "attr".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::S(
                            "default".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::set_multiple(
        UpdateExpressionMap::Set(
            SetInputsMap::Leaves(
                vec![
                    (
                        "attr1".to_string(),
                        SetInput::Assign(
                            Value::String(
                                "val1".to_string()
                            )
                        )
                    ),
                    (
                        "attr2".to_string(),
                        SetInput::Assign(
                            Value::String(
                                "val2".to_string()
                            )
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "SET #attr1 = :set0, #attr2 = :set1".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#attr1".to_string(), "attr1".to_string()),
                    ("#attr2".to_string(), "attr2".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::S(
                            "val1".to_string()
                        )
                    ),
                    (
                        ":set1".to_string(),
                        types::AttributeValue::S(
                            "val2".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::remove_single(
        UpdateExpressionMap::Remove(
            common::selection::SelectionMap::Leaves(
                vec![
                    "attr".to_string(),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "REMOVE #attr".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#attr".to_string(), "attr".to_string()),
                ]
            ),
            ..Default::default()
        }
    )]
    #[case::remove_multiple(
        UpdateExpressionMap::Remove(
            common::selection::SelectionMap::Leaves(
                vec![
                    "attr1".to_string(),
                    "attr2".to_string(),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "REMOVE #attr1, #attr2".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#attr1".to_string(), "attr1".to_string()),
                    ("#attr2".to_string(), "attr2".to_string()),
                ]
            ),
            ..Default::default()
        }
    )]
    #[case::add_number(
        UpdateExpressionMap::Add(
            AddOrDeleteInputsMap::Leaves(
                vec![
                    (
                        "count".to_string(),
                        Value::Number(
                            10.into()
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "ADD #count :add_or_delete0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#count".to_string(), "count".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":add_or_delete0".to_string(),
                        types::AttributeValue::N(
                            "10".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::add_set(
        UpdateExpressionMap::Add(
            AddOrDeleteInputsMap::Leaves(
                vec![
                    (
                        "tags".to_string(),
                        Value::Array(
                            vec![
                                Value::String(
                                    "tag1".to_string()
                                ),
                                Value::String(
                                    "tag2".to_string()
                                )
                            ]
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "ADD #tags :add_or_delete0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#tags".to_string(), "tags".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":add_or_delete0".to_string(),
                        types::AttributeValue::L(
                            vec![
                                types::AttributeValue::S(
                                    "tag1".to_string()
                                ),
                                types::AttributeValue::S(
                                    "tag2".to_string()
                                )
                            ]
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::delete_set(
        UpdateExpressionMap::Delete(
            AddOrDeleteInputsMap::Leaves(
                vec![
                    (
                        "tags".to_string(),
                        Value::Array(
                            vec![
                                Value::String(
                                    "tag1".to_string()
                                )
                            ]
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "DELETE #tags :add_or_delete0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#tags".to_string(), "tags".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":add_or_delete0".to_string(),
                        types::AttributeValue::L(
                            vec![
                                types::AttributeValue::S(
                                    "tag1".to_string()
                                )
                            ]
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::nested_path_set(
        UpdateExpressionMap::Set(
            SetInputsMap::Node(
                IndexMap::from(
                    [
                        (
                            "user".to_string(),
                            SetInputsMap::Leaves(
                                vec![
                                    (
                                        "name".to_string(),
                                        SetInput::Assign(
                                            Value::String(
                                                "John".to_string()
                                            )
                                        )
                                    ),
                                ]
                            )
                        ),
                    ]
                )
            )
        ),
        common::ExpressionInput {
            expression: "SET #user.#name = :set0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#user".to_string(), "user".to_string()),
                    ("#name".to_string(), "name".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::S(
                            "John".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::nested_path_deep(
        UpdateExpressionMap::Set(
            SetInputsMap::Node(
                IndexMap::from(
                    [
                        (
                            "user".to_string(),
                            SetInputsMap::Node(
                                IndexMap::from(
                                    [
                                        (
                                            "profile".to_string(),
                                            SetInputsMap::Leaves(
                                                vec![
                                                    (
                                                        "email".to_string(),
                                                        SetInput::Assign(
                                                            Value::String(
                                                                "test@example.com".to_string()
                                                            )
                                                        )
                                                    ),
                                                ]
                                            )
                                        ),
                                    ]
                                )
                            )
                        ),
                    ]
                )
            )
        ),
        common::ExpressionInput {
            expression: "SET #user.#profile.#email = :set0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#user".to_string(), "user".to_string()),
                    ("#profile".to_string(), "profile".to_string()),
                    ("#email".to_string(), "email".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::S(
                            "test@example.com".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::combined_operations(
        UpdateExpressionMap::Combined(
            vec![
                UpdateExpressionMap::Set(
                    SetInputsMap::Leaves(
                        vec![
                            (
                                "attr1".to_string(),
                                SetInput::Assign(
                                    Value::String(
                                        "val1".to_string()
                                    )
                                )
                            ),
                        ]
                    )
                ),
                UpdateExpressionMap::Remove(
                    common::selection::SelectionMap::Leaves(
                        vec![
                            "oldAttr".to_string(),
                        ]
                    )
                ),
                UpdateExpressionMap::Add(
                    AddOrDeleteInputsMap::Leaves(
                        vec![
                            (
                                "count".to_string(),
                                Value::Number(
                                    5.into()
                                )
                            ),
                        ]
                    )
                ),
            ]
        ),
        common::ExpressionInput {
            expression: "SET #attr1 = :set0 REMOVE #oldAttr ADD #count :add_or_delete1".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#attr1".to_string(), "attr1".to_string()),
                    ("#oldAttr".to_string(), "oldAttr".to_string()),
                    ("#count".to_string(), "count".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":set0".to_string(),
                        types::AttributeValue::S(
                            "val1".to_string()
                        )
                    ),
                    (
                        ":add_or_delete1".to_string(),
                        types::AttributeValue::N(
                            "5".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    fn test_update_expression_map(
        #[case] update_expression_map: UpdateExpressionMap<Value>,
        #[case] expected: common::ExpressionInput,
    ) {
        let actual: common::ExpressionInput = update_expression_map.try_into().unwrap();
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case::empty(
        UpdateItem {
            keys: common::key::Keys {
                partition_key: common::key::Key {
                    name: "a".to_string(),
                    value: Value::String(
                        "b".to_string()
                    ),
                },
                ..Default::default()
            },
            update_expression: UpdateExpressionMap::Set(
                SetInputsMap::Leaves(
                    vec![
                        (
                            "c".to_string(),
                            SetInput::Assign(
                                Value::String(
                                    "d".to_string()
                                )
                            )
                        ),
                    ]
                )
            ),
            write_args: write::common::WriteArgs {
                table_name: "e".to_string(),
                ..Default::default()
            },
        },
        UpdateItemInput {
            keys: collections::HashMap::from(
                [(
                    "a".to_string(),
                    types::AttributeValue::S(
                        "b".to_string()
                    ),
                )]
            ),
            update_expression: "SET #c = :set0".to_string(),
            write_operation: write::common::WriteInput {
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
                                ":set0".to_string(),
                                types::AttributeValue::S(
                                    "d".to_string()
                                )
                            ),
                        ]
                    )
                ),
                table_name: "e".to_string(),
                ..Default::default()
            },
        }
    )]
    #[case::full(
        UpdateItem {
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
            update_expression: UpdateExpressionMap::Set(
                SetInputsMap::Leaves(
                    vec![
                        (
                            "c".to_string(),
                            SetInput::Assign(
                                Value::String(
                                    "d".to_string()
                                )
                            )
                        ),
                    ]
                )
            ),
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
        UpdateItemInput {
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
            update_expression: "SET #c = :set0".to_string(),
            write_operation: write::common::WriteInput {
                condition_expression: Some(
                    "#e = :e_eq0".to_string()
                ),
                expression_attribute_names: Some(
                    collections::HashMap::from(
                        [
                            ("#e".to_string(), "e".to_string()),
                            ("#c".to_string(), "c".to_string()),
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
                            (
                                ":set0".to_string(),
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
                table_name: "g".to_string(),
            },
        }
    )]
    fn test_update_item(#[case] args: UpdateItem<Value>, #[case] expected: UpdateItemInput) {
        let actual: UpdateItemInput = args.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

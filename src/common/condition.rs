use crate::common;

use aws_sdk_dynamodb::types;
use indexmap::IndexMap;
use serde::Serialize;
use serde_dynamo::{Error, Result, to_attribute_value};
use std::{collections, ops};

/// Logical operator for combining conditions.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalOperator {
    /// Logical AND - all conditions must be true.
    And,
    /// Logical OR - at least one condition must be true.
    Or,
}

impl ops::Deref for LogicalOperator {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::And => " AND ",
            Self::Or => " OR ",
        }
    }
}

/// Condition types for DynamoDB expressions.
///
/// ```rust
/// use dynamodb_crud::common::condition;
///
/// let eq = condition::Condition::Equals("value".to_string());
/// let gt = condition::Condition::GreaterThan(100);
/// let null: condition::Condition<String> = condition::Condition::Null;
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum Condition<T> {
    /// Checks if an attribute begins with a specified prefix (string types only).
    BeginsWith(String),
    /// Checks if an attribute value is between two values (inclusive).
    Between(T, T),
    /// Checks if an attribute contains a specified value.
    Contains(T),
    /// Checks if an attribute value equals a specified value.
    Equals(T),
    /// Checks if an attribute value is greater than a specified value.
    GreaterThan(T),
    /// Checks if an attribute value is greater than or equal to a specified value.
    GreaterThanOrEqual(T),
    /// Checks if an attribute value is in a list of specified values.
    In(Vec<T>),
    /// Checks if an attribute value is less than a specified value.
    LessThan(T),
    /// Checks if an attribute value is less than or equal to a specified value.
    LessThanOrEqual(T),
    /// Checks if an attribute does not contain a specified value.
    NotContains(T),
    /// Checks if an attribute value does not equal a specified value.
    NotEqual(T),
    /// Checks if an attribute exists (is not null).
    NotNull,
    /// Checks if an attribute does not exist (is null).
    Null,
}

impl<T: Serialize> Condition<T> {
    fn get_expression(
        self,
        key: &str,
        key_placeholder: &str,
        index: &mut usize,
    ) -> Result<(String, collections::HashMap<String, types::AttributeValue>)> {
        let mut expression_attribute_values = collections::HashMap::new();
        let expression = match self {
            Self::BeginsWith(prefix) => {
                let value_placeholder = format!(":{}_begins_with{}", key, index);
                *index += 1;
                let expression = format!("begins_with({}, {})", key_placeholder, value_placeholder);
                expression_attribute_values
                    .insert(value_placeholder, types::AttributeValue::S(prefix));
                expression
            }
            Self::Between(value1, value2) => {
                let value1 = to_attribute_value(value1)?;
                let value2 = to_attribute_value(value2)?;
                let value_placeholder_1 = format!(":{}_between{}", key, index);
                *index += 1;
                let value_placeholder_2 = format!(":{}_between{}", key, index);
                *index += 1;
                let expression = format!(
                    "{} BETWEEN {} AND {}",
                    key_placeholder, value_placeholder_1, value_placeholder_2
                );
                expression_attribute_values.insert(value_placeholder_1, value1);
                expression_attribute_values.insert(value_placeholder_2, value2);
                expression
            }
            Self::Contains(value) => {
                let value = to_attribute_value(value)?;
                let value_placeholder = format!(":{}_contains{}", key, index);
                *index += 1;
                let expression = format!("contains({}, {})", key_placeholder, value_placeholder);
                expression_attribute_values.insert(value_placeholder, value);
                expression
            }
            Self::Equals(value) => {
                let value = to_attribute_value(value)?;
                let value_placeholder = format!(":{}_eq{}", key, index);
                *index += 1;
                let expression = format!("{} = {}", key_placeholder, value_placeholder);
                expression_attribute_values.insert(value_placeholder, value);
                expression
            }
            Self::GreaterThan(value) => {
                let value = to_attribute_value(value)?;
                let value_placeholder = format!(":{}_gt{}", key, index);
                *index += 1;
                let expression = format!("{} > {}", key_placeholder, value_placeholder);
                expression_attribute_values.insert(value_placeholder, value);
                expression
            }
            Self::GreaterThanOrEqual(value) => {
                let value = to_attribute_value(value)?;
                let value_placeholder = format!(":{}_gte{}", key, index);
                *index += 1;
                let expression = format!("{} >= {}", key_placeholder, value_placeholder);
                expression_attribute_values.insert(value_placeholder, value);
                expression
            }
            Self::In(values) => {
                let mut placeholders = Vec::with_capacity(values.len());
                for (in_index, value) in values.into_iter().enumerate() {
                    let value = to_attribute_value(value)?;
                    let placeholder = format!(":{}_in{}_{}", key, index, in_index);
                    *index += 1;
                    expression_attribute_values.insert(placeholder.clone(), value);
                    placeholders.push(placeholder);
                }
                let placeholders = placeholders.join(", ");
                format!("{} IN ({})", key_placeholder, placeholders)
            }
            Self::LessThan(value) => {
                let value = to_attribute_value(value)?;
                let value_placeholder = format!(":{}_lt{}", key, index);
                *index += 1;
                let expression = format!("{} < {}", key_placeholder, value_placeholder);
                expression_attribute_values.insert(value_placeholder, value);
                expression
            }
            Self::LessThanOrEqual(value) => {
                let value = to_attribute_value(value)?;
                let value_placeholder = format!(":{}_lte{}", key, index);
                *index += 1;
                let expression = format!("{} <= {}", key_placeholder, value_placeholder);
                expression_attribute_values.insert(value_placeholder, value);
                expression
            }
            Self::NotContains(value) => {
                let value = to_attribute_value(value)?;
                let value_placeholder = format!(":{}_not_contains{}", key, index);
                *index += 1;
                let expression =
                    format!("NOT contains({}, {})", key_placeholder, value_placeholder);
                expression_attribute_values.insert(value_placeholder, value);
                expression
            }
            Self::NotEqual(value) => {
                let value = to_attribute_value(value)?;
                let value_placeholder = format!(":{}_ne{}", key, index);
                *index += 1;
                let expression = format!("{} <> {}", key_placeholder, value_placeholder);
                expression_attribute_values.insert(value_placeholder, value);
                expression
            }
            Self::NotNull => {
                format!("attribute_exists({})", key_placeholder)
            }
            Self::Null => {
                format!("attribute_not_exists({})", key_placeholder)
            }
        };
        Ok((expression, expression_attribute_values))
    }
}

/// Condition applied to an attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyCondition<T> {
    /// The condition to apply to the attribute.
    pub condition: Condition<T>,
    /// The name of the attribute to apply the condition to.
    pub name: String,
}

impl<T: Serialize> KeyCondition<T> {
    pub(crate) fn get_expression_operation(keys: Vec<Self>) -> Result<common::ExpressionInput> {
        let mut expressions = Vec::with_capacity(keys.len());
        let mut expression_attribute_names = collections::HashMap::with_capacity(keys.len());
        let mut expression_attribute_values = collections::HashMap::new();
        let mut index = 0;
        for key in keys {
            let placeholder = format!("#{}", key.name);
            let (expression, condition_expression_attribute_values) = key
                .condition
                .get_expression(&key.name, &placeholder, &mut index)?;
            expressions.push(expression);
            expression_attribute_names.insert(placeholder, key.name);
            expression_attribute_values.extend(condition_expression_attribute_values);
        }
        let expression = expressions.join(LogicalOperator::And.as_ref());
        let operation = common::ExpressionInput {
            expression,
            expression_attribute_names,
            expression_attribute_values,
        };
        Ok(operation)
    }
}

/// Map of conditions with logical operators.
///
/// ```rust
/// use dynamodb_crud::common::condition;
///
/// let map = condition::ConditionMap::Leaves(
///     condition::LogicalOperator::And,
///     vec![
///         condition::KeyCondition {
///             name: "status".to_string(),
///             condition: condition::Condition::Equals("active".to_string()),
///         },
///     ],
/// );
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum ConditionMap<T> {
    /// Leaf conditions - flat list of conditions combined with the logical operator.
    Leaves(LogicalOperator, Vec<KeyCondition<T>>),
    /// Node conditions - nested conditions for hierarchical attribute paths.
    Node(LogicalOperator, IndexMap<String, ConditionMap<T>>),
}

impl<T: Serialize> TryFrom<ConditionMap<T>> for common::ExpressionInput {
    type Error = Error;

    fn try_from(condition_map: ConditionMap<T>) -> Result<Self> {
        condition_map.get_expression_operation_recursive(&[], &mut 0, false)
    }
}

impl<T: Serialize> ConditionMap<T> {
    fn is_composite(&self, is_nested: bool) -> bool {
        match self {
            Self::Leaves(_, leaves) => is_nested && leaves.len() > 1,
            Self::Node(_, map) => {
                let has_multiple_keys = map.len() > 1;
                let child_is_nested = is_nested || has_multiple_keys;
                for value in map.values() {
                    if value.is_composite(child_is_nested) {
                        // if any child is composite, we don't need to wrap this node:
                        // the child will be wrapped individually
                        return false;
                    }
                }
                if is_nested {
                    // nested level: composite only if has multiple keys
                    has_multiple_keys
                } else {
                    // root level: never composite
                    false
                }
            }
        }
    }

    fn get_expression_operation_recursive(
        self,
        keys: &[String],
        index: &mut usize,
        mut is_nested: bool,
    ) -> Result<common::ExpressionInput> {
        let mut operations = Vec::new();
        let is_composite = self.is_composite(is_nested);
        let operator = match self {
            Self::Leaves(operator, key_conditions) => {
                for key_condition in key_conditions {
                    let (placeholder, new_keys) =
                        common::add_placeholder(keys, &key_condition.name);
                    let key_placeholder = new_keys.join(".");
                    let (expression, expression_attribute_values) = key_condition
                        .condition
                        .get_expression(&key_condition.name, &key_placeholder, index)?;
                    let expression_attribute_names =
                        collections::HashMap::from([(placeholder, key_condition.name)]);
                    let operation = common::ExpressionInput {
                        expression,
                        expression_attribute_names,
                        expression_attribute_values,
                    };
                    operations.push(operation);
                }
                operator
            }
            Self::Node(operator, map) => {
                operations.reserve(map.len());
                is_nested = is_nested || map.len() > 1;
                for (key, value) in map {
                    let (placeholder, new_keys) = common::add_placeholder(keys, &key);
                    let mut condition_operation =
                        value.get_expression_operation_recursive(&new_keys, index, is_nested)?;
                    condition_operation
                        .expression_attribute_names
                        .insert(placeholder, key);
                    operations.push(condition_operation);
                }
                operator
            }
        };
        let mut operation = common::ExpressionInput::merge(&operator, operations);
        if is_composite {
            operation.expression = format!("({})", operation.expression);
        }
        Ok(operation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use serde_json::Value;

    #[rstest]
    #[case::leaves_single_condition(
        ConditionMap::Leaves(
            LogicalOperator::And,
            vec![
                KeyCondition {
                    name: "a".to_string(),
                    condition: Condition::Equals(
                        Value::Number(
                            1.into()
                        )
                    ),
                },
            ]
        ),
        common::ExpressionInput {
            expression: "#a = :a_eq0".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [(
                    "#a".to_string(),
                    "a".to_string(),
                )]
            ),
            expression_attribute_values: collections::HashMap::from(
                [(
                    ":a_eq0".to_string(),
                    types::AttributeValue::N(
                        "1".to_string()
                    ),
                )]
            ),
        }
    )]
    #[case::leaves_multiple_conditions_and(
        ConditionMap::Leaves(
            LogicalOperator::And,
            vec![
                KeyCondition {
                    name: "a".to_string(),
                    condition: Condition::Equals(
                        Value::String(
                            "b".to_string()
                        )
                    ),
                },
                KeyCondition {
                    name: "c".to_string(),
                    condition: Condition::Equals(
                        Value::Number(
                            1.into()
                        )
                    ),
                },
            ]
        ),
        common::ExpressionInput {
            expression: "#a = :a_eq0 AND #c = :c_eq1".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                    ("#c".to_string(), "c".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":a_eq0".to_string(),
                        types::AttributeValue::S(
                            "b".to_string()
                        )
                    ),
                    (
                        ":c_eq1".to_string(),
                        types::AttributeValue::N(
                            "1".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::leaves_multiple_conditions_or(
        ConditionMap::Leaves(
            LogicalOperator::Or,
            vec![
                KeyCondition {
                    name: "a".to_string(),
                    condition: Condition::Between(
                        Value::Number(
                            1.into()
                        ),
                        Value::Number(
                            10.into()
                        ),
                    ),
                },
                KeyCondition {
                    name: "b".to_string(),
                    condition: Condition::BeginsWith(
                        "c".to_string()
                    ),
                },
            ]
        ),
        common::ExpressionInput {
            expression: "#a BETWEEN :a_between0 AND :a_between1 OR begins_with(#b, :b_begins_with2)".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                    ("#b".to_string(), "b".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":a_between0".to_string(),
                        types::AttributeValue::N(
                            "1".to_string()
                        )
                    ),
                    (
                        ":a_between1".to_string(),
                        types::AttributeValue::N(
                            "10".to_string()
                        )
                    ),
                    (
                        ":b_begins_with2".to_string(),
                        types::AttributeValue::S(
                            "c".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::node_single_level(
        ConditionMap::Node(
            LogicalOperator::And,
            IndexMap::from(
                [
                    (
                        "a".to_string(),
                        ConditionMap::Leaves(
                            LogicalOperator::And,
                            vec![
                                KeyCondition {
                                    name: "b".to_string(),
                                    condition: Condition::Equals(
                                        Value::String(
                                            "c".to_string()
                                        )
                                    ),
                                },
                            ]
                        )
                    ),
                    (
                        "b".to_string(),
                        ConditionMap::Leaves(
                            LogicalOperator::Or,
                            vec![
                                KeyCondition {
                                    name: "d".to_string(),
                                    condition: Condition::Equals(
                                        Value::String(
                                            "e".to_string()
                                        )
                                    ),
                                },
                            ]
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "#a.#b = :b_eq0 AND #b.#d = :d_eq1".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                    ("#b".to_string(), "b".to_string()),
                    ("#b".to_string(), "b".to_string()),
                    ("#d".to_string(), "d".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":b_eq0".to_string(),
                        types::AttributeValue::S(
                            "c".to_string()
                        )
                    ),
                    (
                        ":d_eq1".to_string(),
                        types::AttributeValue::S(
                            "e".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::node_nested(
        ConditionMap::Node(
            LogicalOperator::And,
            IndexMap::from(
                [
                    (
                        "a".to_string(),
                        ConditionMap::Node(
                            LogicalOperator::And,
                            IndexMap::from(
                                [
                                    (
                                        "b".to_string(),
                                        ConditionMap::Leaves(
                                            LogicalOperator::And,
                                            vec![
                                                KeyCondition {
                                                    name: "c".to_string(),
                                                    condition: Condition::Equals(
                                                        Value::String(
                                                            "d".to_string()
                                                        )
                                                    ),
                                                },
                                                KeyCondition {
                                                    name: "e".to_string(),
                                                    condition: Condition::Equals(
                                                        Value::String(
                                                            "f".to_string()
                                                        )
                                                    ),
                                                },
                                            ]
                                        )
                                    )
                                ]
                            )
                        )
                    ),
                    (
                        "b".to_string(),
                        ConditionMap::Leaves(
                            LogicalOperator::Or,
                            vec![
                                KeyCondition {
                                    name: "g".to_string(),
                                    condition: Condition::Equals(
                                        Value::String(
                                            "h".to_string()
                                        )
                                    ),
                                },
                                KeyCondition {
                                    name: "i".to_string(),
                                    condition: Condition::Equals(
                                        Value::String(
                                            "j".to_string()
                                        )
                                    ),
                                },
                            ]
                        )
                    )
                ]
            )
        ),
        common::ExpressionInput {
            expression: "(#a.#b.#c = :c_eq0 AND #a.#b.#e = :e_eq1) AND (#b.#g = :g_eq2 OR #b.#i = :i_eq3)".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                    ("#b".to_string(), "b".to_string()),
                    ("#c".to_string(), "c".to_string()),
                    ("#e".to_string(), "e".to_string()),
                    ("#g".to_string(), "g".to_string()),
                    ("#i".to_string(), "i".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":c_eq0".to_string(),
                        types::AttributeValue::S(
                            "d".to_string()
                        )
                    ),
                    (
                        ":e_eq1".to_string(),
                        types::AttributeValue::S(
                            "f".to_string()
                        )
                    ),
                    (
                        ":g_eq2".to_string(),
                        types::AttributeValue::S(
                            "h".to_string()
                        )
                    ),
                    (
                        ":i_eq3".to_string(),
                        types::AttributeValue::S(
                            "j".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::leaves_same_key_or_equals(
        ConditionMap::Leaves(
            LogicalOperator::Or,
            vec![
                KeyCondition {
                    name: "a".to_string(),
                    condition: Condition::Equals(
                        Value::Number(
                            1.into()
                        )
                    ),
                },
                KeyCondition {
                    name: "a".to_string(),
                    condition: Condition::Equals(
                        Value::Number(
                            2.into()
                        )
                    ),
                },
            ]
        ),
        common::ExpressionInput {
            expression: "#a = :a_eq0 OR #a = :a_eq1".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [(
                    "#a".to_string(),
                    "a".to_string(),
                )]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":a_eq0".to_string(),
                        types::AttributeValue::N(
                            "1".to_string()
                        )
                    ),
                    (
                        ":a_eq1".to_string(),
                        types::AttributeValue::N(
                            "2".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::leaves_same_key_or_different_operators(
        ConditionMap::Leaves(
            LogicalOperator::Or,
            vec![
                KeyCondition {
                    name: "a".to_string(),
                    condition: Condition::GreaterThan(
                        Value::Number(
                            5.into()
                        )
                    ),
                },
                KeyCondition {
                    name: "a".to_string(),
                    condition: Condition::LessThan(
                        Value::Number(
                            3.into()
                        )
                    ),
                },
            ]
        ),
        common::ExpressionInput {
            expression: "#a > :a_gt0 OR #a < :a_lt1".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [(
                    "#a".to_string(),
                    "a".to_string(),
                )]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":a_gt0".to_string(),
                        types::AttributeValue::N(
                            "5".to_string()
                        )
                    ),
                    (
                        ":a_lt1".to_string(),
                        types::AttributeValue::N(
                            "3".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::node_nested_same_key_or(
        ConditionMap::Node(
            LogicalOperator::And,
            IndexMap::from(
                [
                    (
                        "a".to_string(),
                        ConditionMap::Leaves(
                            LogicalOperator::Or,
                            vec![
                                KeyCondition {
                                    name: "b".to_string(),
                                    condition: Condition::Equals(
                                        Value::String(
                                            "x".to_string()
                                        )
                                    ),
                                },
                                KeyCondition {
                                    name: "b".to_string(),
                                    condition: Condition::Equals(
                                        Value::String(
                                            "y".to_string()
                                        )
                                    ),
                                },
                            ]
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "#a.#b = :b_eq0 OR #a.#b = :b_eq1".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                    ("#b".to_string(), "b".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":b_eq0".to_string(),
                        types::AttributeValue::S(
                            "x".to_string()
                        )
                    ),
                    (
                        ":b_eq1".to_string(),
                        types::AttributeValue::S(
                            "y".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    #[case::inter_key_collision_across_leaves(
        ConditionMap::Node(
            LogicalOperator::And,
            IndexMap::from(
                [
                    (
                        "x".to_string(),
                        ConditionMap::Leaves(
                            LogicalOperator::And,
                            vec![
                                KeyCondition {
                                    name: "a".to_string(),
                                    condition: Condition::Equals(
                                        Value::Number(
                                            1.into()
                                        )
                                    ),
                                },
                            ]
                        )
                    ),
                    (
                        "y".to_string(),
                        ConditionMap::Leaves(
                            LogicalOperator::And,
                            vec![
                                KeyCondition {
                                    name: "a".to_string(),
                                    condition: Condition::Equals(
                                        Value::Number(
                                            2.into()
                                        )
                                    ),
                                },
                            ]
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "#x.#a = :a_eq0 AND #y.#a = :a_eq1".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#x".to_string(), "x".to_string()),
                    ("#y".to_string(), "y".to_string()),
                    ("#a".to_string(), "a".to_string()),
                ]
            ),
            expression_attribute_values: collections::HashMap::from(
                [
                    (
                        ":a_eq0".to_string(),
                        types::AttributeValue::N(
                            "1".to_string()
                        )
                    ),
                    (
                        ":a_eq1".to_string(),
                        types::AttributeValue::N(
                            "2".to_string()
                        )
                    ),
                ]
            ),
        }
    )]
    fn test_condition_map_to_condition_operation(
        #[case] condition_map: ConditionMap<Value>,
        #[case] expected: common::ExpressionInput,
    ) {
        let actual: common::ExpressionInput = condition_map.try_into().unwrap();
        assert_eq!(actual, expected);
    }
}

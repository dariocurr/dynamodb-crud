use crate::common;

use indexmap::IndexMap;
use std::{collections, hash};

/// Map for selecting attributes in projection expressions.
///
/// ```rust
/// use dynamodb_crud::common::selection;
///
/// let selection = selection::SelectionMap::Leaves(vec![
///     "id".to_string(),
///     "name".to_string(),
/// ]);
/// ```
#[derive(Debug, Eq, PartialEq)]
pub enum SelectionMap {
    /// Leaf selection - a flat list of attribute names to select.
    Leaves(Vec<String>),
    /// Node selection - nested selection for hierarchical attribute paths.
    Node(IndexMap<String, SelectionMap>),
}

impl hash::Hash for SelectionMap {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Leaves(leaves) => leaves.hash(state),
            Self::Node(map) => map.iter().for_each(|(key, value)| {
                key.hash(state);
                value.hash(state);
            }),
        }
    }
}

impl From<SelectionMap> for common::ExpressionInput {
    fn from(selection_map: SelectionMap) -> Self {
        selection_map.get_selection_operation_recursive(&[])
    }
}

impl SelectionMap {
    pub(crate) fn get_selection_operation_recursive(
        self,
        keys: &[String],
    ) -> common::ExpressionInput {
        let operations: Vec<_> = match self {
            Self::Leaves(leaves) => leaves
                .into_iter()
                .map(|leaf| {
                    let (placeholder, new_keys) = common::add_placeholder(keys, &leaf);
                    let expression_attribute_names =
                        collections::HashMap::from([(placeholder, leaf)]);
                    let expression = new_keys.join(".");
                    common::ExpressionInput {
                        expression,
                        expression_attribute_names,
                        ..Default::default()
                    }
                })
                .collect(),
            Self::Node(map) => map
                .into_iter()
                .map(|(key, value)| {
                    let (placeholder, new_keys) = common::add_placeholder(keys, &key);
                    let mut operation = value.get_selection_operation_recursive(&new_keys);
                    operation
                        .expression_attribute_names
                        .insert(placeholder, key);
                    operation
                })
                .collect(),
        };
        common::ExpressionInput::merge(", ", operations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[case::leaves_single(
        SelectionMap::Leaves(
            vec![
                "a".to_string(),
            ]
        ),
        common::ExpressionInput {
            expression: "#a".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                ]
            ),
            ..Default::default()
        }
    )]
    #[case::leaves_multiple(
        SelectionMap::Leaves(
            vec![
                "a".to_string(),
                "b".to_string(),
            ]
        ),
        common::ExpressionInput {
            expression: "#a, #b".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                    ("#b".to_string(), "b".to_string()),
                ]
            ),
            ..Default::default()
        }
    )]
    #[case::node_single_level(
        SelectionMap::Node(
            IndexMap::from(
                [
                    (
                        "a".to_string(),
                        SelectionMap::Leaves(
                            vec![
                                "b".to_string(),
                                "c".to_string(),
                            ]
                        )
                    ),
                    (
                        "d".to_string(),
                        SelectionMap::Leaves(
                            vec![
                                "e".to_string(),
                                "f".to_string(),
                            ]
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "#a.#b, #a.#c, #d.#e, #d.#f".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                    ("#b".to_string(), "b".to_string()),
                    ("#c".to_string(), "c".to_string()),
                    ("#d".to_string(), "d".to_string()),
                    ("#e".to_string(), "e".to_string()),
                    ("#f".to_string(), "f".to_string()),
                ]
            ),
            ..Default::default()
        }
    )]
    #[case::node_nested(
        SelectionMap::Node(
            IndexMap::from(
                [
                    (
                        "a".to_string(),
                        SelectionMap::Node(
                            IndexMap::from(
                                [
                                    (
                                        "b".to_string(),
                                        SelectionMap::Leaves(
                                            vec![
                                                "c".to_string(),
                                                "d".to_string(),
                                            ]
                                        )
                                    ),
                                ]
                            )
                        )
                    ),
                    (
                        "b".to_string(),
                        SelectionMap::Leaves(
                            vec![
                                "e".to_string(),
                                "f".to_string(),
                            ]
                        )
                    ),
                ]
            )
        ),
        common::ExpressionInput {
            expression: "#a.#b.#c, #a.#b.#d, #b.#e, #b.#f".to_string(),
            expression_attribute_names: collections::HashMap::from(
                [
                    ("#a".to_string(), "a".to_string()),
                    ("#b".to_string(), "b".to_string()),
                    ("#c".to_string(), "c".to_string()),
                    ("#d".to_string(), "d".to_string()),
                    ("#e".to_string(), "e".to_string()),
                    ("#f".to_string(), "f".to_string()),
                ]
            ),
            ..Default::default()
        }
    )]
    fn test_selection_map_to_selection_operation(
        #[case] selection_map: SelectionMap,
        #[case] expected: common::ExpressionInput,
    ) {
        let actual: common::ExpressionInput = selection_map.into();
        assert_eq!(actual, expected);
    }
}

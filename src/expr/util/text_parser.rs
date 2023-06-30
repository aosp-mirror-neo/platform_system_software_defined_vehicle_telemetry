// Copyright 2023 Google LLC. All Rights Reserved.

//! Provides a utility function to parse a string expression into protobuf nodes.
//!
//! Potential usage for building a complete MetricsConfig in the sample client
//! application.

use log::info;
use sdvtelemetry_proto_rust::expression::{
    combination_node::{
        ArithmeticOperator as ArithmeticOperatorProto, LogicalOperator as LogicalOperatorProto,
        Operator as OperatorProto, RelationalOperator as RelationalOperatorProto,
    },
    CombinationNode, ConstantLeafNode, FieldLeafNode, Node as NodeProto,
};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    OpenParenthesis,
    CloseParenthesis,
    PubField(String, Vec<String>),
    Logical(LogicalOperatorProto),
    Relational(RelationalOperatorProto),
    Arithmetic(ArithmeticOperatorProto),
}

/// Implementation of the shunting yard algorithm.
/// https://en.wikipedia.org/wiki/Shunting_yard_algorithm
#[derive(Default)]
pub struct Parser {
    // If the same Node already exists, maintaining this vector allows us to re-use it.
    existing_nodes: Vec<NodeProto>,
    // Maps string representation of NodeProto to its index in `existing_nodes` vector
    node_to_index: HashMap<String, u32>,
}

impl Parser {
    /// Constructor
    pub fn new() -> Self {
        Parser { existing_nodes: Vec::new(), node_to_index: HashMap::new() }
    }

    /// Return all nodes constructed from all the past `parse()` calls.
    pub fn get_nodes(&self) -> &[NodeProto] {
        &self.existing_nodes
    }

    /// Reset parse history.
    pub fn clear_nodes(&mut self) {
        self.existing_nodes.clear();
        self.node_to_index.clear()
    }

    /// Convert an expression to protobuf nodes representing the expression.
    /// Updates the list of existing_nodes in the struct, which allows for re-using
    /// Nodes from the existing list.
    ///
    /// Only a limited set of operators are supported.
    /// See `parse_operator` function for operators than can be parsed.
    ///
    /// # Arguments
    ///
    /// * `expr` - The textual expression.
    ///
    /// # Returns
    ///
    /// Index to the root node which represents the expression, or Err() if failed.
    pub fn parse(&mut self, expr: &str) -> Result<u32, String> {
        let tokens = tokenize(expr)?;

        info!("Tokens = {:?}", tokens);

        // operand is NodeProto (represented as an index in `self.existing_nodes` vector)
        let mut operand_stack: Vec<u32> = Vec::new();

        // operator is all operators + parentheses
        let mut operator_stack: Vec<Token> = Vec::new();

        for token in tokens {
            match token {
                Token::PubField(pub_name, fields) => {
                    self.push_node_to_operands_stack(
                        NodeProto::from_pub_name_and_fields(pub_name, fields),
                        &mut operand_stack,
                    );
                }
                Token::OpenParenthesis => operator_stack.push(token),
                Token::CloseParenthesis => {
                    while let Some(operator) = operator_stack.pop() {
                        if operator == Token::OpenParenthesis {
                            break;
                        }
                        if operator_stack.is_empty() {
                            return Err("Mismatched parentheses".to_string());
                        }
                        self.push_node_to_operands_stack(
                            Self::build_combination_node(operator, &mut operand_stack)?,
                            &mut operand_stack,
                        );
                    }
                }
                Token::Logical(_) | Token::Arithmetic(_) | Token::Relational(_) => {
                    if let Some(top_operator) = &operator_stack.last() {
                        // if LHS has higher or equal operator precedence, build node for LHS
                        if top_operator.precedence() >= token.precedence() {
                            self.push_node_to_operands_stack(
                                Self::build_combination_node(
                                    operator_stack.pop().unwrap(),
                                    &mut operand_stack,
                                )?,
                                &mut operand_stack,
                            );
                        }
                    }
                    operator_stack.push(token);
                }
                // '_' matches constant tokens, so it builds constant leaf nodes
                _ => self.push_node_to_operands_stack(
                    NodeProto::from_constant_token(&token)?,
                    &mut operand_stack,
                ),
            }
        }

        while let Some(operator) = operator_stack.pop() {
            self.push_node_to_operands_stack(
                Self::build_combination_node(operator, &mut operand_stack)?,
                &mut operand_stack,
            );
        }

        // return index to root node that represents the text expression
        Ok(*operand_stack.last().unwrap())
    }

    /// Builds a CombinationNode using these 3 pieces of information:
    /// 1. the operator proto,  2. the left operand, 3. the right operand
    fn build_combination_node(
        operator_token: Token,
        operand_stack: &mut Vec<u32>,
    ) -> Result<NodeProto, String> {
        let op = operator_token.to_operator_proto()?;
        // stack is LIFO, right operand is pushed *after* left operand,
        // therefore right operand is popped *before* left operand
        let (right, left) = {
            if op == OperatorProto::LogicalOperator(LogicalOperatorProto::NOT.into()) {
                (None, operand_stack.pop()) // unary operator -> no right operand
            } else {
                (operand_stack.pop(), operand_stack.pop())
            }
        };
        Ok(NodeProto::from_children(op, left, right))
    }

    /// Pushes the node to the operand stack, reuses existing node if possible.
    fn push_node_to_operands_stack(&mut self, node: NodeProto, operand_stack: &mut Vec<u32>) {
        let hash_key = node.to_string_representation();
        if let Some(index) = self.node_to_index.get(&hash_key) {
            // reuse node
            operand_stack.push(*index);
        } else {
            // new node, push to the end of the vector
            self.existing_nodes.push(node);
            let index = (self.existing_nodes.len() - 1) as u32;
            self.node_to_index.insert(hash_key, index);
            operand_stack.push(index);
        }
    }
}

fn is_number_or_identifier(c: char) -> bool {
    matches!(c, '0'..='9' | '.' | 'a'..='z' | 'A'..='Z' | '_')
}

/// Look ahead to find the end index of current value/identifier.
fn peek_value(input: &[char], i: usize) -> usize {
    let mut i = i;
    while i < input.len() && is_number_or_identifier(input[i]) {
        i += 1;
    }
    i
}

/// Look ahead to find the end index of current operator.
fn peek_operator(input: &[char], i: usize) -> usize {
    let mut i = i;
    while i < input.len()
        && !is_number_or_identifier(input[i])
        && !matches!(input[i], ' ' | '(' | ')')
    {
        i += 1;
    }
    i
}

fn parse_value(buf: &str) -> Result<Token, String> {
    Ok(if let Ok(integer) = buf.parse::<i32>() {
        Token::Int(integer)
    } else if let Ok(integer) = buf.parse::<i64>() {
        Token::Long(integer)
    } else if let Ok(number) = buf.parse::<f32>() {
        Token::Float(number)
    } else if let Ok(number) = buf.parse::<f64>() {
        Token::Double(number)
    } else if buf == "true" {
        Token::Boolean(true)
    } else if buf == "false" {
        Token::Boolean(false)
    } else if !buf.is_empty() {
        let parts: Vec<&str> = buf.split('.').collect();
        Token::PubField(parts[0].to_string(), parts[1..].iter().map(|&f| f.to_string()).collect())
    } else {
        return Err(format!("Unknown value: {}", buf));
    })
}

fn parse_parenthesis(buf: &str) -> Result<Token, String> {
    Ok(match buf {
        "(" => Token::OpenParenthesis,
        ")" => Token::CloseParenthesis,
        _ => return Err(format!("Input {} is not a parenthesis", buf)),
    })
}

fn parse_operator(buf: &str) -> Result<Token, String> {
    Ok(match buf {
        "+" => Token::Arithmetic(ArithmeticOperatorProto::ADD),
        "-" => Token::Arithmetic(ArithmeticOperatorProto::SUBTRACT),
        "*" => Token::Arithmetic(ArithmeticOperatorProto::MULTIPLY),
        "/" => Token::Arithmetic(ArithmeticOperatorProto::DIVIDE),
        "**" => Token::Arithmetic(ArithmeticOperatorProto::POWER),
        "%" => Token::Arithmetic(ArithmeticOperatorProto::MODULUS),
        "&&" => Token::Logical(LogicalOperatorProto::AND),
        "||" => Token::Logical(LogicalOperatorProto::OR),
        "^" => Token::Logical(LogicalOperatorProto::XOR),
        "!" => Token::Logical(LogicalOperatorProto::NOT),
        "==" => Token::Relational(RelationalOperatorProto::EQ),
        "!=" => Token::Relational(RelationalOperatorProto::NOT_EQ),
        ">=" => Token::Relational(RelationalOperatorProto::GT_OR_EQ),
        ">" => Token::Relational(RelationalOperatorProto::GT),
        "<=" => Token::Relational(RelationalOperatorProto::LT_OR_EQ),
        "<" => Token::Relational(RelationalOperatorProto::LT),
        _ => return Err(format!("Unknown operator: {}", buf)),
    })
}

/// Convert expression string to a list of tokens.
fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens: Vec<Token> = Vec::new();

    let input_chars: Vec<char> = input.chars().collect();
    let mut i: usize = 0;
    loop {
        if i >= input.len() {
            break;
        }
        let c = input_chars[i];
        match c {
            // match numbers and potential publisher & field names
            '0'..='9' | '.' | 'a'..='z' | 'A'..='Z' | '_' => {
                let end_idx = peek_value(&input_chars, i);
                tokens.push(parse_value(&input[i..end_idx])?);
                i = end_idx;
            }
            // match minus sign
            '-' => {
                // push negative number if the last token is an operator
                if tokens.is_empty() || tokens.last().unwrap().is_operator() {
                    let end_idx = peek_value(&input_chars, i + 1);
                    tokens.push(parse_value(&input[i..end_idx])?);
                    i = end_idx;
                } else {
                    tokens.push(parse_operator(&input[i..i + 1])?);
                    i += 1;
                }
            }
            // match parentheses
            '(' | ')' => {
                tokens.push(parse_parenthesis(&input[i..i + 1])?);
                i += 1;
            }
            // skip white spaces
            ' ' => {
                i += 1;
            }
            // all other characters are operators
            _ => {
                let end_idx = peek_operator(&input_chars, i);
                tokens.push(parse_operator(&input[i..end_idx])?);
                i = end_idx;
            }
        }
    }
    Ok(tokens)
}

impl Token {
    fn precedence(&self) -> u8 {
        match self {
            Token::Logical(op) => match op {
                LogicalOperatorProto::AND => 5,
                LogicalOperatorProto::OR => 4,
                LogicalOperatorProto::XOR => 7,
                LogicalOperatorProto::NOT => 14,
            },
            Token::Arithmetic(op) => match op {
                ArithmeticOperatorProto::ADD | ArithmeticOperatorProto::SUBTRACT => 12,
                ArithmeticOperatorProto::MULTIPLY
                | ArithmeticOperatorProto::DIVIDE
                | ArithmeticOperatorProto::MODULUS => 13,
                ArithmeticOperatorProto::POWER => 14,
            },
            Token::Relational(op) => match op {
                RelationalOperatorProto::EQ | RelationalOperatorProto::NOT_EQ => 9,
                RelationalOperatorProto::GT
                | RelationalOperatorProto::GT_OR_EQ
                | RelationalOperatorProto::LT
                | RelationalOperatorProto::LT_OR_EQ => 10,
                _ => 0,
            },
            _ => 0,
        }
    }

    fn is_operator(&self) -> bool {
        matches!(self, Token::Logical(_) | Token::Arithmetic(_) | Token::Relational(_))
    }

    fn to_operator_proto(&self) -> Result<OperatorProto, String> {
        Ok(match self {
            Token::Logical(op) => OperatorProto::LogicalOperator((*op).into()),
            Token::Arithmetic(op) => OperatorProto::ArithmeticOperator((*op).into()),
            Token::Relational(op) => OperatorProto::RelationalOperator((*op).into()),
            _ => return Err(format!("Cannot convert {:?} to operator", self)),
        })
    }
}

trait NodeProtoExtension {
    fn from_constant_token(token: &Token) -> Result<NodeProto, String>;
    fn from_pub_name_and_fields(pub_name: String, fields: Vec<String>) -> NodeProto;
    fn from_children(op: OperatorProto, left: Option<u32>, right: Option<u32>) -> NodeProto;

    /// Create String representation of a protobuf Node to use as the hash key.
    /// Protobuf Nodes themselves cannot be used as hash keys.
    fn to_string_representation(&self) -> String;
}

impl NodeProtoExtension for NodeProto {
    fn from_constant_token(token: &Token) -> Result<NodeProto, String> {
        let mut node = NodeProto::new();
        node.set_constant_leaf_node(ConstantLeafNode::new());
        match token {
            Token::Boolean(value) => {
                node.mut_constant_leaf_node().set_bool_value(*value);
            }
            Token::Int(value) => {
                node.mut_constant_leaf_node().set_int32_value(*value);
            }
            Token::Long(value) => {
                node.mut_constant_leaf_node().set_int64_value(*value);
            }
            Token::Float(value) => {
                node.mut_constant_leaf_node().set_float_value(*value);
            }
            Token::Double(value) => {
                node.mut_constant_leaf_node().set_double_value(*value);
            }
            _ => return Err(format!("Cannot create constant leaf node from token {:?}", token)),
        }
        Ok(node)
    }

    fn from_pub_name_and_fields(pub_name: String, fields: Vec<String>) -> NodeProto {
        let mut node = NodeProto::new();
        let mut leaf = FieldLeafNode::new();
        leaf.publisher_name = pub_name;
        leaf.field_names = fields;
        node.set_field_leaf_node(leaf);
        node
    }

    fn from_children(op: OperatorProto, left: Option<u32>, right: Option<u32>) -> NodeProto {
        let mut node = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.left_index = left;
        combination_node.right_index = right;
        match op {
            OperatorProto::ArithmeticOperator(op) => {
                combination_node.set_arithmetic_operator(op.unwrap());
            }
            OperatorProto::LogicalOperator(op) => {
                combination_node.set_logical_operator(op.unwrap());
            }
            OperatorProto::RelationalOperator(op) => {
                combination_node.set_relational_operator(op.unwrap());
            }
            _ => {
                panic!("No operator, cannot build protobuf Node");
            }
        }
        node.set_combination_node(combination_node);
        node
    }

    fn to_string_representation(&self) -> String {
        format!(
            "Node {{ constant: {}, field: {}, combination: {} }}",
            self.constant_leaf_node(),
            self.field_leaf_node(),
            self.combination_node()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdvtelemetry_proto_rust::expression::{
        combination_node::{ArithmeticOperator, LogicalOperator, RelationalOperator},
        CombinationNode, ConstantLeafNode, FieldLeafNode, Node as NodeProto,
    };

    #[test]
    fn test_parse_simple_logical() -> Result<(), String> {
        let mut parser = Parser::new();
        let expr = "true || false";
        // setup this tree: true || false
        //            index 2 (||)
        //          /              \
        //    index 0              index 1
        //    TRUE                  FALSE
        let mut leaf_true = NodeProto::new();
        let mut leaf_false = NodeProto::new();
        let mut root_node = NodeProto::new();

        let mut constant_leaf = ConstantLeafNode::new();
        constant_leaf.set_bool_value(true);
        leaf_true.set_constant_leaf_node(constant_leaf.clone());
        constant_leaf.set_bool_value(false);
        leaf_false.set_constant_leaf_node(constant_leaf.clone());

        let mut combination_node = CombinationNode::new();
        combination_node.left_index = Some(0);
        combination_node.right_index = Some(1);
        combination_node.set_logical_operator(LogicalOperator::OR);
        root_node.set_combination_node(combination_node);

        let expected_nodes = vec![leaf_true, leaf_false, root_node];

        // action
        let index = parser.parse(expr)?;

        // assert
        assert_eq!(index, 2);
        assert_eq!(parser.get_nodes().clone(), expected_nodes);
        Ok(())
    }

    #[test]
    fn test_parse_simple_arithmetic() -> Result<(), String> {
        let mut parser = Parser::new();
        let expr = "2+3";
        // setup this tree: 2+3
        //            index 2 (+)
        //          /              \
        //    index 0              index 1
        //     2                      3
        let mut leaf_two = NodeProto::new();
        let mut leaf_three = NodeProto::new();
        let mut root_node = NodeProto::new();

        let mut constant_leaf = ConstantLeafNode::new();
        constant_leaf.set_int32_value(2);
        leaf_two.set_constant_leaf_node(constant_leaf.clone());
        constant_leaf.set_int32_value(3);
        leaf_three.set_constant_leaf_node(constant_leaf.clone());

        let mut combination_node = CombinationNode::new();
        combination_node.left_index = Some(0);
        combination_node.right_index = Some(1);
        combination_node.set_arithmetic_operator(ArithmeticOperator::ADD);
        root_node.set_combination_node(combination_node);

        let expected_nodes = vec![leaf_two, leaf_three, root_node];

        // action
        let index = parser.parse(expr)?;

        // assert
        assert_eq!(index, 2);
        assert_eq!(parser.get_nodes().clone(), expected_nodes);
        Ok(())
    }

    #[test]
    fn test_parse_arithmetic_order_of_precedence() -> Result<(), String> {
        let mut parser = Parser::new();
        let expr = "2+3**3";
        // setup this tree: 2 + 3 ** 3
        //            index 3 (+)
        //          /              \
        //    index 0              index 2
        //     2                      **
        //                        /        \
        //                    index 1     index 1
        //                       3          3
        let mut leaf_two = NodeProto::new();
        let mut leaf_three = NodeProto::new();
        let mut multiplication_node = NodeProto::new();
        let mut root_node = NodeProto::new();

        let mut constant_leaf = ConstantLeafNode::new();
        constant_leaf.set_int32_value(2);
        leaf_two.set_constant_leaf_node(constant_leaf.clone());
        constant_leaf.set_int32_value(3);
        leaf_three.set_constant_leaf_node(constant_leaf.clone());

        let mut power = CombinationNode::new();
        power.set_arithmetic_operator(ArithmeticOperator::POWER);
        power.left_index = Some(1);
        power.right_index = Some(1);
        multiplication_node.set_combination_node(power);

        let mut add = CombinationNode::new();
        add.set_arithmetic_operator(ArithmeticOperator::ADD);
        add.left_index = Some(0);
        add.right_index = Some(2);
        root_node.set_combination_node(add);

        let expected_nodes = vec![leaf_two, leaf_three, multiplication_node, root_node];

        // action
        let index = parser.parse(expr)?;

        // assert
        assert_eq!(index, 3);
        assert_eq!(parser.get_nodes().clone(), expected_nodes);
        Ok(())
    }

    #[test]
    fn test_parse_arithmetic_with_parentheses() -> Result<(), String> {
        let mut parser = Parser::new();
        let expr = "(2+3*3)%2";
        // setup this tree: (2 + 3 * 3) % 2
        //                         index 4 (%)
        //                     /                \
        //            index 3 (+)             index 0 (2)
        //          /              \
        //    index 0              index 2
        //     2                      *
        //                        /        \
        //                    index 1     index 1
        //                       3          3
        let mut leaf_two = NodeProto::new();
        let mut leaf_three = NodeProto::new();
        let mut multiplication_node = NodeProto::new();
        let mut add_node = NodeProto::new();
        let mut modulus_node = NodeProto::new();

        let mut constant_leaf = ConstantLeafNode::new();
        constant_leaf.set_int32_value(2);
        leaf_two.set_constant_leaf_node(constant_leaf.clone());
        constant_leaf.set_int32_value(3);
        leaf_three.set_constant_leaf_node(constant_leaf.clone());

        let mut multiply = CombinationNode::new();
        multiply.set_arithmetic_operator(ArithmeticOperator::MULTIPLY);
        multiply.left_index = Some(1);
        multiply.right_index = Some(1);
        multiplication_node.set_combination_node(multiply);

        let mut add = CombinationNode::new();
        add.set_arithmetic_operator(ArithmeticOperator::ADD);
        add.left_index = Some(0);
        add.right_index = Some(2);
        add_node.set_combination_node(add);

        let mut modulus = CombinationNode::new();
        modulus.set_arithmetic_operator(ArithmeticOperator::MODULUS);
        modulus.left_index = Some(3);
        modulus.right_index = Some(0);
        modulus_node.set_combination_node(modulus);

        let expected_nodes =
            vec![leaf_two, leaf_three, multiplication_node, add_node, modulus_node];

        // action
        let index = parser.parse(expr)?;

        // assert
        assert_eq!(index, 4);
        assert_eq!(parser.get_nodes().clone(), expected_nodes);
        Ok(())
    }

    #[test]
    fn test_parse_mixed_leaf_node_and_operator_types() -> Result<(), String> {
        let mut parser = Parser::new();
        let expr = "publisher.field_1 / 3 != 100 && publisher.field_2";
        // setup this tree: publisher.field_1 * 3 != 100 && publisher.field_2
        //                                      index 6 (&&)
        //                                  /                \
        //                         index 4 (!=)            index 5 (publisher.field_2)
        //                     /                \
        //            index 2 (/)             index 3 (100)
        //          /              \
        //    index 0              index 1
        // publisher.field_1           3

        let mut leaf_field_1 = NodeProto::new();
        let mut leaf_three = NodeProto::new();
        let mut divide_node = NodeProto::new();
        let mut leaf_one_hundred = NodeProto::new();
        let mut not_eq_node = NodeProto::new();
        let mut leaf_field_2 = NodeProto::new();
        let mut and_node = NodeProto::new();

        let mut field_1 = FieldLeafNode::new();
        field_1.publisher_name = "publisher".to_string();
        field_1.field_names.push("field_1".to_string());
        leaf_field_1.set_field_leaf_node(field_1);

        let mut constant_3 = ConstantLeafNode::new();
        constant_3.set_int32_value(3);
        leaf_three.set_constant_leaf_node(constant_3);

        let mut divide = CombinationNode::new();
        divide.set_arithmetic_operator(ArithmeticOperator::DIVIDE);
        divide.left_index = Some(0);
        divide.right_index = Some(1);
        divide_node.set_combination_node(divide);

        let mut constant_100 = ConstantLeafNode::new();
        constant_100.set_int32_value(100);
        leaf_one_hundred.set_constant_leaf_node(constant_100);

        let mut not_eq = CombinationNode::new();
        not_eq.set_relational_operator(RelationalOperator::NOT_EQ);
        not_eq.left_index = Some(2);
        not_eq.right_index = Some(3);
        not_eq_node.set_combination_node(not_eq);

        let mut field_2 = FieldLeafNode::new();
        field_2.publisher_name = "publisher".to_string();
        field_2.field_names.push("field_2".to_string());
        leaf_field_2.set_field_leaf_node(field_2);

        let mut and = CombinationNode::new();
        and.set_logical_operator(LogicalOperator::AND);
        and.left_index = Some(4);
        and.right_index = Some(5);
        and_node.set_combination_node(and);

        let expected_nodes = vec![
            leaf_field_1,
            leaf_three,
            divide_node,
            leaf_one_hundred,
            not_eq_node,
            leaf_field_2,
            and_node,
        ];

        // action
        let index = parser.parse(expr)?;

        // assert
        assert_eq!(index, 6);
        assert_eq!(parser.get_nodes().clone(), expected_nodes);
        Ok(())
    }

    #[test]
    fn test_parse_ambiguous_operators() -> Result<(), String> {
        let mut parser = Parser::new();
        // want to show:
        // * "-" is the sign of a number in this context, not the "subtract" operator
        // * ">=" is not pre-maturely parsed as ">"
        // * "!" is properly parsed as unary operator NOT
        let expr = "-3 >= publisher.field_1 && !publisher.field_2";
        // setup this tree: -3 >= publisher.field_1 && !publisher.field_2
        //                         index 5 (&&)
        //                     /                \
        //            index 2 (>=)              index 4 (NOT)
        //          /              \                   \
        //    index 0              index 1           index 3
        //       -3           publisher.field_1      publisher.field_2

        let mut leaf_minus_three = NodeProto::new();
        let mut leaf_field_1 = NodeProto::new();
        let mut gt_or_eq_node = NodeProto::new();
        let mut leaf_field_2 = NodeProto::new();
        let mut not_node = NodeProto::new();
        let mut and_node = NodeProto::new();

        let mut field_1 = FieldLeafNode::new();
        field_1.publisher_name = "publisher".to_string();
        field_1.field_names.push("field_1".to_string());
        leaf_field_1.set_field_leaf_node(field_1);

        let mut constant = ConstantLeafNode::new();
        constant.set_int32_value(-3);
        leaf_minus_three.set_constant_leaf_node(constant);

        let mut gt_or_eq = CombinationNode::new();
        gt_or_eq.set_relational_operator(RelationalOperator::GT_OR_EQ);
        gt_or_eq.left_index = Some(0);
        gt_or_eq.right_index = Some(1);
        gt_or_eq_node.set_combination_node(gt_or_eq);

        let mut field_2 = FieldLeafNode::new();
        field_2.publisher_name = "publisher".to_string();
        field_2.field_names.push("field_2".to_string());
        leaf_field_2.set_field_leaf_node(field_2);

        let mut not_eq = CombinationNode::new();
        not_eq.set_logical_operator(LogicalOperator::NOT);
        not_eq.left_index = Some(3);
        not_node.set_combination_node(not_eq);

        let mut and = CombinationNode::new();
        and.set_logical_operator(LogicalOperator::AND);
        and.left_index = Some(2);
        and.right_index = Some(4);
        and_node.set_combination_node(and);

        let expected_nodes =
            vec![leaf_minus_three, leaf_field_1, gt_or_eq_node, leaf_field_2, not_node, and_node];

        // action
        let index = parser.parse(expr)?;

        // assert
        assert_eq!(index, 5);
        assert_eq!(parser.get_nodes().clone(), expected_nodes);
        Ok(())
    }

    #[test]
    fn test_parse_reuse_nodes() -> Result<(), String> {
        let mut parser = Parser::new();
        // setup this tree: (2 + 3 * 3) / 2
        //                         index 4 (/)
        //                     /                \
        //            index 3 (+)             index 0 (2)
        //          /              \
        //    index 0              index 2
        //     2                      *
        //                        /        \
        //                    index 1     index 1
        //                       3          3
        // This tree is already verified in the test `test_parse_arithmetic_with_parentheses`
        parser.parse("(2+3*3)/2")?;

        // action
        // 3*3 already exists in the tree, it should return the index 2
        let index = parser.parse("3*3")?;

        // assert
        assert_eq!(index, 2);
        Ok(())
    }
}

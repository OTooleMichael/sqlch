use crate::dialect::Dialect;
use crate::tokenizer::{Token, TokenType};
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq)]
pub struct LooseGroup {
    pub context: SqlContext,
    pub elements: Vec<LooseAstElement>,
}

impl LooseGroup {
    fn push(&mut self, element: LooseAstElement) {
        self.elements.push(element)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LooseAstElement {
    Token(Token),
    Group(LooseGroup),
}

#[derive(Debug, Clone, Default)]
pub struct LooseAst {
    pub element: Option<LooseAstElement>,
}

impl LooseAst {
    pub fn push(&mut self, element: LooseAstElement) {
        match &mut self.element {
            Some(LooseAstElement::Group(group)) => {
                group.push(element);
            }
            _ => {
                let elements: Vec<LooseAstElement> = vec![element];
                self.element = Some(LooseAstElement::Group(LooseGroup {
                    context: SqlContext::Root,
                    elements,
                }));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum StackPopType {
    After,
    Before,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CaseClause {
    Root,
    WhenThen,
    When,
    Then,
    Else,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BetweenClause {
    Root,
    Val1,
    Val2,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParenType {
    WindowOver,
    WindowClause,
    SubQuery,
    CTESubQuery,
    Function,
    DataType,
    Extract,
    Cast,
    Struct,
    Other,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlClause {
    SelectFields,
    From,
    Join,
    Where,
    GroupBy,
    Having,
    Qualify,
    OrderBy,
    Limit,
    WindowClause,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlContext {
    Root,
    Stmt,
    SelectBlockClause(SqlClause),
    CaseStatement(CaseClause),
    BetweenClause(BetweenClause),
    WithClause,
    Parens(ParenType),
    JinjaBlock,
}

pub struct LooseAstBuilder {
    tokens: VecDeque<Token>,
    ast: LooseAst,
    group_stack: Vec<(SqlContext, Vec<LooseAstElement>)>,
}

impl LooseAstBuilder {
    pub fn new(tokens: Vec<Token>, _dialect: Dialect) -> Self {
        let token_queue = VecDeque::from(tokens);

        Self {
            tokens: token_queue,
            ast: LooseAst::default(),
            group_stack: Vec::new(),
        }
    }

    fn current_context(&self) -> SqlContext {
        self.group_stack
            .last()
            .map(|(ctx, _)| ctx.clone())
            .unwrap_or(SqlContext::Root)
    }

    fn context_contains(&self, ctx: SqlContext, max_look_back: usize) -> bool {
        for (i, (element, _)) in self.group_stack.iter().rev().enumerate() {
            if i > max_look_back {
                return false;
            }
            if std::mem::discriminant(element) == std::mem::discriminant(&ctx) {
                return true;
            }
        }
        false
    }

    fn end_ctx(&mut self, ctx: &SqlContext) -> bool {
        match ctx {
            SqlContext::Root => true,
            SqlContext::JinjaBlock => {
                // Don't terminate JinjaBlock contexts - they should complete naturally
                true
            }
            SqlContext::CaseStatement(CaseClause::Then)
            | SqlContext::CaseStatement(CaseClause::WhenThen)
            | SqlContext::CaseStatement(CaseClause::When)
            | SqlContext::CaseStatement(CaseClause::Else) => {
                self.group_end();
                false
            }
            SqlContext::SelectBlockClause(_) => {
                self.group_end();
                false
            }
            SqlContext::Parens(_) => {
                self.group_end();
                false
            }
            _ => {
                self.group_end();
                false
            }
        }
    }

    fn end_while<F>(&mut self, stop_fn: F)
    where
        F: Fn(&SqlContext) -> bool,
    {
        while let Some((ctx, _)) = self.group_stack.last() {
            if !stop_fn(ctx) {
                break;
            }
            if self.end_ctx(&ctx.clone()) {
                break;
            }
        }
    }

    fn end_contexts<F>(&mut self, stop_fn: F, stop_type: StackPopType)
    where
        F: Fn(&SqlContext) -> bool,
    {
        let mut stopper: Option<StackPopType> = None;
        while let Some((ctx, _)) = self.group_stack.last() {
            if stop_fn(ctx) {
                stopper = Some(stop_type.clone());
            }
            if matches!(stopper, Some(StackPopType::Before)) {
                break;
            }
            if self.end_ctx(&ctx.clone()) {
                break;
            }
            if matches!(stopper, Some(StackPopType::After)) {
                break;
            }
        }
    }

    fn terminate_contexts_for(&mut self, new_context: SqlContext) {
        if !matches!(new_context, SqlContext::SelectBlockClause(_)) {
            return;
        }
        self.end_contexts(
            |ctx| {
                matches!(
                    ctx,
                    SqlContext::SelectBlockClause(_) | SqlContext::JinjaBlock
                )
            },
            StackPopType::After,
        )
    }

    fn group_start(&mut self, ctx: SqlContext) {
        self.group_stack.push((ctx, Vec::new()));
    }

    fn group_end(&mut self) {
        let (context, latest_group_elements) = self
            .group_stack
            .pop()
            .unwrap_or((SqlContext::Root, Vec::new()));
        let group = LooseGroup {
            context,
            elements: latest_group_elements,
        };
        self.push(LooseAstElement::Group(group));
    }

    fn push(&mut self, element: LooseAstElement) {
        if let Some((_, current_group)) = self.group_stack.last_mut() {
            current_group.push(element);
        } else {
            self.ast.push(element);
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.front()
    }

    fn advance(&mut self) -> Option<Token> {
        self.tokens.pop_front()
    }

    pub fn build(mut self) -> LooseAst {
        // Start the first statement
        self.start_statement();

        while let Some(token) = self.advance() {
            self.process_token(token);
        }
        self.ast
    }

    fn start_statement(&mut self) {
        self.group_start(SqlContext::Stmt);
    }

    fn process_token(&mut self, token: Token) {
        match &token.token_type {
            TokenType::EOF => {
                while !self.group_stack.is_empty() {
                    self.group_end();
                }
                self.push(LooseAstElement::Token(token));
            }
            TokenType::Select => self.handle_select(token),
            TokenType::From => self.handle_from(token),
            TokenType::Where
            | TokenType::Having
            | TokenType::Qualify
            | TokenType::GroupBy
            | TokenType::Limit => {
                let token_type = token.token_type.clone();
                self.handle_whereish(token, &token_type);
            }
            TokenType::OrderBy => {
                if self.context_contains(SqlContext::Parens(ParenType::WindowOver), 4) {
                    self.push(LooseAstElement::Token(token));

                    return;
                }
                let token_type = token.token_type.clone();
                self.handle_whereish(token, &token_type);
            }
            TokenType::Over => {
                self.push(LooseAstElement::Token(token));

                self.peek_left_paren_ctx(ParenType::WindowOver);
            }
            TokenType::As => {
                self.handle_keyword(token);
                match self.current_context() {
                    SqlContext::WithClause => {
                        self.peek_left_paren_ctx(ParenType::CTESubQuery);
                    }
                    SqlContext::SelectBlockClause(SqlClause::WindowClause) => {
                        self.peek_left_paren_ctx(ParenType::WindowClause);
                    }
                    _ => {}
                }
            }
            TokenType::Varchar | TokenType::Decimal => {
                self.push(LooseAstElement::Token(token));
                self.peek_left_paren_ctx(ParenType::DataType);
            }
            TokenType::Extract => {
                self.push(LooseAstElement::Token(token));
                self.peek_left_paren_ctx(ParenType::Extract);
            }
            TokenType::Cast => {
                self.push(LooseAstElement::Token(token));
                self.peek_left_paren_ctx(ParenType::Cast);
            }
            TokenType::Struct => {
                self.push(LooseAstElement::Token(token));
                self.peek_left_paren_ctx(ParenType::Struct);
            }
            TokenType::Window => {
                self.handle_window(token);
            }
            TokenType::Between => self.handle_between(token),
            TokenType::Keyword | TokenType::In => self.handle_keyword(token),
            TokenType::PartitionBy => self.handle_partition_by(token),
            TokenType::Case => self.handle_case(token),
            TokenType::When => self.handle_when(token),
            TokenType::Then => self.handle_then(token),
            TokenType::Else => self.handle_else(token),
            TokenType::End => self.handle_end(token),
            TokenType::With => self.handle_with(token),
            TokenType::InnerJoin
            | TokenType::LeftJoin
            | TokenType::RightJoin
            | TokenType::FullJoin
            | TokenType::LeftOuterJoin
            | TokenType::RightOuterJoin
            | TokenType::FullOuterJoin
            | TokenType::CrossJoin
            | TokenType::NaturalJoin
            | TokenType::Join => self.handle_join(token),
            TokenType::Identifier | TokenType::Number | TokenType::StringLiteral => {
                self.handle_value(token)
            }
            TokenType::Comma => {
                self.end_while(|ctx| matches!(ctx, SqlContext::BetweenClause(_)));
                self.push(LooseAstElement::Token(token));
            }
            TokenType::Dot => self.handle_dot(token),
            TokenType::LeftParen => self.handle_left_paren(token, ParenType::Other),
            TokenType::RightParen => self.handle_right_paren(token),
            TokenType::JinjaIf | TokenType::JinjaFor => {
                self.handle_jinja_start(token);
            }
            TokenType::JinjaElif | TokenType::JinjaElse => {
                self.handle_jinja_transition(token);
            }
            TokenType::JinjaEndif | TokenType::JinjaEndfor => {
                self.handle_jinja_end(token);
            }
            TokenType::TemplateVariable | TokenType::TemplateBlock => {
                self.handle_jinja_template(token)
            }
            TokenType::Operator => self.handle_operator(token),
            TokenType::Star => {
                if matches!(self.peek_type(), Some(TokenType::RightParen)) {
                    self.push(LooseAstElement::Token(token));
                    return;
                }

                self.push(LooseAstElement::Token(token));
            }
            TokenType::And => self.handle_and(token),
            TokenType::Or => {
                self.end_while(|ctx| matches!(ctx, SqlContext::BetweenClause(_)));
                self.push(LooseAstElement::Token(token));
            }
            TokenType::SemiColon => {
                self.handle_semicolon(token);
            }
            _ => {
                if let Some(next_token) = self.peek() {
                    match next_token.token_type {
                        TokenType::LeftParen => {
                            // Function call - we'll let left paren handling take care of grouping
                            self.push(LooseAstElement::Token(token));
                        }
                        _ => {
                            self.push(LooseAstElement::Token(token));
                        }
                    }
                }
            }
        }
    }

    fn peek_type(&self) -> Option<TokenType> {
        if let Some(next_token) = self.peek() {
            return Some(next_token.token_type.clone());
        }
        None
    }

    fn handle_select(&mut self, token: Token) {
        if matches!(self.current_context(), SqlContext::WithClause) {
            self.group_end();
        }
        self.group_start(SqlContext::SelectBlockClause(SqlClause::SelectFields));
        self.push(LooseAstElement::Token(token));
        if let Some(next_token) = self.peek() {
            if matches!(next_token.token_type, TokenType::Keyword)
                && next_token.value().to_uppercase() == "DISTINCT"
            {
                let distinct = self.advance().unwrap();
                self.push(LooseAstElement::Token(distinct));
            }
        }
    }

    fn handle_from(&mut self, token: Token) {
        // If we're directly inside an EXTRACT context, treat FROM as a keyword
        if let Some((context, _)) = self.group_stack.last() {
            if matches!(context, SqlContext::Parens(ParenType::Extract)) {
                self.handle_keyword(token);
                return;
            }
        }

        let ctx = SqlContext::SelectBlockClause(SqlClause::From);
        self.terminate_contexts_for(ctx.clone());
        self.group_start(ctx);
        self.push(LooseAstElement::Token(token));
    }

    fn handle_join(&mut self, token: Token) {
        let ctx = SqlContext::SelectBlockClause(SqlClause::Join);
        self.terminate_contexts_for(ctx.clone());

        self.group_start(ctx);
        self.push(LooseAstElement::Token(token));
    }

    fn handle_whereish(&mut self, token: Token, trigger: &TokenType) {
        let cls = match trigger {
            TokenType::Where => SqlClause::Where,
            TokenType::Having => SqlClause::Having,
            TokenType::Qualify => SqlClause::Qualify,
            TokenType::GroupBy => SqlClause::GroupBy,
            TokenType::OrderBy => SqlClause::OrderBy,
            TokenType::Limit => SqlClause::Limit,
            _ => return, // Handle unexpected token types gracefully
        };
        let ctx = SqlContext::SelectBlockClause(cls);
        self.terminate_contexts_for(ctx.clone());

        self.group_start(ctx);
        self.push(LooseAstElement::Token(token));
    }

    fn handle_partition_by(&mut self, token: Token) {
        self.push(LooseAstElement::Token(token));
    }

    fn handle_case(&mut self, token: Token) {
        self.group_start(SqlContext::CaseStatement(CaseClause::Root));
        self.push(LooseAstElement::Token(token));
    }

    fn handle_between(&mut self, token: Token) {
        self.group_start(SqlContext::BetweenClause(BetweenClause::Root));
        self.group_start(SqlContext::BetweenClause(BetweenClause::Val1));
        self.push(LooseAstElement::Token(token));
    }

    fn handle_and(&mut self, token: Token) {
        if let SqlContext::BetweenClause(_) = self.current_context() {
            self.group_end();
        }
        // Normal AND processing - revert to simple logic for now
        self.push(LooseAstElement::Token(token));
    }

    fn handle_when(&mut self, token: Token) {
        self.move_to_case_root();

        self.group_start(SqlContext::CaseStatement(CaseClause::WhenThen));

        self.group_start(SqlContext::CaseStatement(CaseClause::When));
        self.push(LooseAstElement::Token(token));
    }

    fn handle_then(&mut self, token: Token) {
        self.group_end();

        self.group_start(SqlContext::CaseStatement(CaseClause::Then));
        self.push(LooseAstElement::Token(token));
    }

    fn handle_else(&mut self, token: Token) {
        self.move_to_case_root();

        self.group_start(SqlContext::CaseStatement(CaseClause::Else));
        self.push(LooseAstElement::Token(token));
    }

    fn handle_end(&mut self, token: Token) {
        self.move_to_case_root();

        self.push(LooseAstElement::Token(token));
        self.group_end();
    }

    fn move_to_case_root(&mut self) {
        self.end_contexts(
            |ctx| matches!(ctx, SqlContext::CaseStatement(CaseClause::Root)),
            StackPopType::Before,
        );
    }

    fn handle_with(&mut self, token: Token) {
        self.group_start(SqlContext::WithClause);
        self.push(LooseAstElement::Token(token));
    }

    fn handle_keyword(&mut self, token: Token) {
        self.push(LooseAstElement::Token(token));
    }

    fn handle_value(&mut self, token: Token) {
        let curr_type = token.token_type.clone();
        self.push(LooseAstElement::Token(token));
        if let Some(next) = self.peek() {
            match next.token_type {
                TokenType::LeftParen => {
                    if curr_type == TokenType::Identifier {
                        let p_token = self.advance().unwrap();
                        self.handle_left_paren(p_token, ParenType::Function);
                    }
                }
                TokenType::Comma | TokenType::Dot => {}
                _ => {}
            }
        }
    }

    fn handle_dot(&mut self, token: Token) {
        self.push(LooseAstElement::Token(token));
    }

    fn handle_operator(&mut self, token: Token) {
        match token.value().as_str() {
            "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=" => {
                self.push(LooseAstElement::Token(token));
            }
            _ => {
                self.push(LooseAstElement::Token(token));
            }
        }
    }

    fn handle_left_paren(&mut self, token: Token, paren_type: ParenType) {
        assert!(matches!(token.token_type, TokenType::LeftParen));

        if matches!(paren_type, ParenType::CTESubQuery) {
            self.group_start(SqlContext::Parens(paren_type));
            self.push(LooseAstElement::Token(token));

            return;
        }

        let peek_type = self.peek_type();
        match peek_type {
            Some(TokenType::Star) => {
                let next_t = LooseAstElement::Token(self.advance().unwrap());
                self.group_start(SqlContext::Parens(ParenType::Other));
                self.push(LooseAstElement::Token(token));
                self.push(next_t);
            }
            Some(TokenType::Select) => {
                self.group_start(SqlContext::Parens(ParenType::SubQuery));
                self.push(LooseAstElement::Token(token));
            }
            _ => {
                self.group_start(SqlContext::Parens(paren_type));
                self.push(LooseAstElement::Token(token));
            }
        }
    }

    fn peek_left_paren_ctx(&mut self, paren_type: ParenType) {
        if let Some(next_token) = self.peek() {
            if next_token.token_type == TokenType::LeftParen {
                let t = self.advance().unwrap();
                self.handle_left_paren(t, paren_type);
            }
        }
    }

    fn handle_right_paren(&mut self, token: Token) {
        self.end_contexts(
            |ctx| matches!(ctx, SqlContext::Parens(_)),
            StackPopType::Before,
        );

        self.push(LooseAstElement::Token(token));
        self.group_end();
    }

    fn handle_jinja_template(&mut self, token: Token) {
        self.push(LooseAstElement::Token(token));
    }

    fn handle_jinja_start(&mut self, token: Token) {
        self.group_start(SqlContext::JinjaBlock);
        self.push(LooseAstElement::Token(token));
    }

    fn close_jinja_block(&mut self) {
        // Close any SQL clauses that are still open within the Jinja block
        self.end_contexts(
            |ctx| matches!(ctx, SqlContext::JinjaBlock),
            StackPopType::Before,
        );
        // Close the current Jinja content with dedent

        self.group_end();
    }

    fn handle_jinja_transition(&mut self, token: Token) {
        self.close_jinja_block();
        self.handle_jinja_start(token);
    }

    fn handle_jinja_end(&mut self, token: Token) {
        self.close_jinja_block();
        self.push(LooseAstElement::Token(token));
    }

    fn handle_window(&mut self, token: Token) {
        let ctx = SqlContext::SelectBlockClause(SqlClause::WindowClause);
        self.terminate_contexts_for(ctx.clone());
        self.group_start(ctx);
        self.push(LooseAstElement::Token(token));
    }

    fn handle_semicolon(&mut self, token: Token) {
        // Close all contexts except Root to end the current statement
        self.end_contexts(|ctx| matches!(ctx, SqlContext::Root), StackPopType::Before);

        // Add the semicolon token (though it's not in the YAML, it's part of the source)
        self.push(LooseAstElement::Token(token));

        // End the current Stmt context
        if matches!(self.current_context(), SqlContext::Stmt) {
            self.group_end();
        }

        // Start a new statement if there are more tokens
        if !self.tokens.is_empty() {
            self.start_statement();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::Tokenizer;
    use serde::Deserialize;
    use std::fs;

    #[derive(Debug, Deserialize)]
    struct TestCase {
        name: String,
        sql: String,
        ast: TestGroup,
    }

    #[derive(Debug, Deserialize)]
    struct TestGroup {
        context: String,
        elements: Option<Vec<TestElement>>,
    }

    #[derive(Debug, Deserialize, Clone)]
    #[serde(untagged)]
    enum TestElement {
        Token {
            #[serde(rename = "type")]
            element_type: String,
            token_type: String,
            value: Option<String>,
        },
        Group {
            context: String,
            elements: Option<Vec<TestElement>>,
        },
    }

    #[derive(Debug, Deserialize)]
    struct TestFile {
        file: Vec<TestCase>,
    }

    fn parse_context(context_str: &str) -> SqlContext {
        match context_str {
            "Root" => SqlContext::Root,
            "Stmt" => SqlContext::Stmt,
            "SelectBlockClause(SelectFields)" => {
                SqlContext::SelectBlockClause(SqlClause::SelectFields)
            }
            "SelectBlockClause(From)" => SqlContext::SelectBlockClause(SqlClause::From),
            "SelectBlockClause(Join)" => SqlContext::SelectBlockClause(SqlClause::Join),
            "SelectBlockClause(Where)" => SqlContext::SelectBlockClause(SqlClause::Where),
            "SelectBlockClause(GroupBy)" => SqlContext::SelectBlockClause(SqlClause::GroupBy),
            "SelectBlockClause(Having)" => SqlContext::SelectBlockClause(SqlClause::Having),
            "SelectBlockClause(Qualify)" => SqlContext::SelectBlockClause(SqlClause::Qualify),
            "SelectBlockClause(OrderBy)" => SqlContext::SelectBlockClause(SqlClause::OrderBy),
            "SelectBlockClause(Limit)" => SqlContext::SelectBlockClause(SqlClause::Limit),
            "SelectBlockClause(WindowClause)" => {
                SqlContext::SelectBlockClause(SqlClause::WindowClause)
            }
            "CaseStatement(Root)" => SqlContext::CaseStatement(CaseClause::Root),
            "CaseStatement(WhenThen)" => SqlContext::CaseStatement(CaseClause::WhenThen),
            "CaseStatement(When)" => SqlContext::CaseStatement(CaseClause::When),
            "CaseStatement(Then)" => SqlContext::CaseStatement(CaseClause::Then),
            "CaseStatement(Else)" => SqlContext::CaseStatement(CaseClause::Else),
            "BetweenClause(Root)" => SqlContext::BetweenClause(BetweenClause::Root),
            "BetweenClause(Val1)" => SqlContext::BetweenClause(BetweenClause::Val1),
            "WithClause" => SqlContext::WithClause,
            "Parens(WindowOver)" => SqlContext::Parens(ParenType::WindowOver),
            "Parens(WindowClause)" => SqlContext::Parens(ParenType::WindowClause),
            "Parens(SubQuery)" => SqlContext::Parens(ParenType::SubQuery),
            "Parens(CTESubQuery)" => SqlContext::Parens(ParenType::CTESubQuery),
            "Parens(Function)" => SqlContext::Parens(ParenType::Function),
            "Parens(DataType)" => SqlContext::Parens(ParenType::DataType),
            "Parens(Extract)" => SqlContext::Parens(ParenType::Extract),
            "Parens(Cast)" => SqlContext::Parens(ParenType::Cast),
            "Parens(Struct)" => SqlContext::Parens(ParenType::Struct),
            "Parens(Other)" => SqlContext::Parens(ParenType::Other),
            "JinjaBlock" => SqlContext::JinjaBlock,
            _ => panic!("Unknown context: {context_str}"),
        }
    }

    fn compare_groups(actual: &LooseGroup, expected: &TestGroup, test_name: &str, path: &str) {
        assert_eq!(
            actual.context,
            parse_context(&expected.context),
            "Test '{}' failed at {}: Context mismatch. Expected {:?}, got {:?}",
            test_name,
            path,
            expected.context,
            actual.context
        );

        match &expected.elements {
            Some(expected_elements) => {
                assert_eq!(
                    actual.elements.len(),
                    expected_elements.len(),
                    "Test '{}' failed at {}: Element count mismatch. Expected {}, got {}",
                    test_name,
                    path,
                    expected_elements.len(),
                    actual.elements.len()
                );

                for (i, (actual_element, expected_element)) in actual
                    .elements
                    .iter()
                    .zip(expected_elements.iter())
                    .enumerate()
                {
                    let element_path = format!("{path}[{i}]");
                    compare_elements(actual_element, expected_element, test_name, &element_path);
                }
            }
            None => {
                // If no elements key is provided, don't check recursively - just match the context
            }
        }
    }

    fn compare_elements(
        actual: &LooseAstElement,
        expected: &TestElement,
        test_name: &str,
        path: &str,
    ) {
        match (actual, expected) {
            (
                LooseAstElement::Token(actual_token),
                TestElement::Token {
                    element_type,
                    token_type,
                    value,
                },
            ) => {
                assert_eq!(
                    element_type, "Token",
                    "Test '{test_name}' failed at {path}: Expected Token element"
                );

                let actual_token_type = match actual_token.token_type {
                    crate::tokenizer::TokenType::Select => "Select",
                    crate::tokenizer::TokenType::From => "From",
                    crate::tokenizer::TokenType::Where => "Where",
                    crate::tokenizer::TokenType::Having => "Having",
                    crate::tokenizer::TokenType::Qualify => "Qualify",
                    crate::tokenizer::TokenType::GroupBy => "GroupBy",
                    crate::tokenizer::TokenType::OrderBy => "OrderBy",
                    crate::tokenizer::TokenType::Limit => "Limit",
                    crate::tokenizer::TokenType::Case => "Case",
                    crate::tokenizer::TokenType::End => "End",
                    crate::tokenizer::TokenType::When => "When",
                    crate::tokenizer::TokenType::Then => "Then",
                    crate::tokenizer::TokenType::Else => "Else",
                    crate::tokenizer::TokenType::Identifier => "Identifier",
                    crate::tokenizer::TokenType::Number => "Number",
                    crate::tokenizer::TokenType::StringLiteral => "StringLiteral",
                    crate::tokenizer::TokenType::Operator => "Operator",
                    crate::tokenizer::TokenType::Comma => "Comma",
                    crate::tokenizer::TokenType::Dot => "Dot",
                    crate::tokenizer::TokenType::Star => "*",
                    crate::tokenizer::TokenType::LeftParen => "LeftParen",
                    crate::tokenizer::TokenType::RightParen => "RightParen",
                    crate::tokenizer::TokenType::SemiColon => "SemiColon",
                    crate::tokenizer::TokenType::EOF => "EOF",
                    _ => "Other",
                };

                assert_eq!(
                    token_type, actual_token_type,
                    "Test '{test_name}' failed at {path}: Token type mismatch. Expected {token_type}, got {actual_token_type}"
                );

                if let Some(expected_value) = value {
                    assert_eq!(
                        expected_value,
                        &actual_token.value(),
                        "Test '{}' failed at {}: Token value mismatch. Expected {}, got {}",
                        test_name,
                        path,
                        expected_value,
                        actual_token.value()
                    );
                }
            }
            (LooseAstElement::Group(actual_group), TestElement::Group { context, elements }) => {
                let test_group = TestGroup {
                    context: context.clone(),
                    elements: elements.clone(),
                };
                compare_groups(actual_group, &test_group, test_name, path);
            }
            _ => panic!("Test '{test_name}' failed at {path}: Element type mismatch"),
        }
    }

    fn load_test_file(path: &str) -> TestFile {
        let content = fs::read_to_string(path).expect("Failed to read test file");
        serde_yaml::from_str(&content).expect("Failed to parse YAML")
    }

    fn run_yaml_tests(test_file_path: &str) {
        let test_file = load_test_file(test_file_path);

        for test_case in test_file.file {
            println!("Running: {}", test_case.name);

            let mut tokenizer = Tokenizer::new(&test_case.sql);
            let tokenizer_result = tokenizer
                .tokenize()
                .expect("Tokenization should succeed in tests");

            let builder = LooseAstBuilder::new(tokenizer_result.tokens, Dialect::default());
            let ast = builder.build();

            match &ast.element {
                Some(LooseAstElement::Group(actual_group)) => {
                    compare_groups(actual_group, &test_case.ast, &test_case.name, "root");
                }
                _ => panic!("Test '{}' failed: Expected root group", test_case.name),
            }

            println!("  ✓ Passed");
        }
    }

    #[test]
    fn test_basic_loose_parser() {
        run_yaml_tests("test/fixtures/loose_parser/basic.yml");
    }

    #[test]
    fn test_case_statements_loose_parser() {
        run_yaml_tests("test/fixtures/loose_parser/case_statements.yml");
    }
}

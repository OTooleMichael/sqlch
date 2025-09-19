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
pub enum ParenType {
    WindowOver,
    SubQuery,
    CTESubQuery,
    Function,
    DataType,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlContext {
    Root,
    SelectBlockClause(SqlClause),
    CaseStatement(CaseClause),
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
            match ctx {
                SqlContext::Root => break,
                SqlContext::JinjaBlock => {
                    // Don't terminate JinjaBlock contexts - they should complete naturally
                    break;
                }
                SqlContext::CaseStatement(CaseClause::Then)
                | SqlContext::CaseStatement(CaseClause::WhenThen)
                | SqlContext::CaseStatement(CaseClause::When)
                | SqlContext::CaseStatement(CaseClause::Else) => {
                    self.group_end();
                }
                SqlContext::SelectBlockClause(_) => {
                    self.group_end();
                }
                SqlContext::Parens(_) => {
                    self.group_end();
                }
                _ => {
                    self.group_end();
                }
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
        while let Some(token) = self.advance() {
            self.process_token(token);
        }
        self.ast
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
                if !matches!(self.current_context(), SqlContext::WithClause) {
                    return;
                }
                self.peek_left_paren_ctx(ParenType::CTESubQuery);
            }
            TokenType::Varchar | TokenType::Decimal => {
                self.push(LooseAstElement::Token(token));
                self.peek_left_paren_ctx(ParenType::DataType);
            }
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
            TokenType::Comma => self.handle_comma(token),
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
            TokenType::And | TokenType::Or => {
                self.push(LooseAstElement::Token(token));
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

    fn handle_comma(&mut self, token: Token) {
        self.push(LooseAstElement::Token(token));
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
}

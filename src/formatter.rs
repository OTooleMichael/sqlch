use crate::dialect::Dialect;
use crate::tokenizer::{Token, TokenType};
use serde::Deserialize;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq)]
pub enum FormatElement {
    Token(Token), // Keep token info for renderer decisions
    Space,
    HardBreak, // SQL structural line breaks
    SoftBreak, // Can become space or break depending on line width
    ShortBreak,
    SoftLine, // Can become nothing (flat) or break depending on line width
    LineGap,  // A new line, and a full empty line
    Indent,
    Dedent,
    Group(Vec<FormatElement>), // Universal grouping - any () creates a group
}

impl FormatElement {
    fn prio(&self) -> u8 {
        match self {
            FormatElement::LineGap => 0,
            FormatElement::HardBreak => 1,
            FormatElement::SoftBreak => 2,
            FormatElement::SoftLine => 3,
            FormatElement::Space => 4,
            FormatElement::Indent => 255,
            FormatElement::Dedent => 255,
            _ => 255,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FormatAst {
    elements: Vec<FormatElement>,
}

impl FormatAst {
    pub fn push(&mut self, element: FormatElement) {
        self.elements.push(element);
    }

    pub fn elements(&self) -> &[FormatElement] {
        &self.elements
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JinjaBlockType {
    If,
    For,
}

#[derive(Debug, Clone)]
pub struct FormatSettings {
    pub indent_style: IndentStyle,
    pub indent_size: usize,
    pub line_width: usize,
}

#[derive(Debug, Clone)]
pub enum IndentStyle {
    Spaces,
    Tabs,
}

impl Default for FormatSettings {
    fn default() -> Self {
        Self {
            indent_style: IndentStyle::Spaces,
            indent_size: 2,
            line_width: 80,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TestSettings {
    pub indent: Option<TestIndent>,
    pub line_width: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct TestIndent {
    pub style: String,
    pub size: usize,
}

impl From<&TestSettings> for FormatSettings {
    fn from(test_settings: &TestSettings) -> Self {
        let mut settings = FormatSettings::default();

        if let Some(indent) = &test_settings.indent {
            settings.indent_style = match indent.style.as_str() {
                "tabs" => IndentStyle::Tabs,
                _ => IndentStyle::Spaces,
            };
            settings.indent_size = indent.size;
        }

        if let Some(width) = test_settings.line_width {
            settings.line_width = width;
        }

        settings
    }
}

pub struct Formatter {
    settings: FormatSettings,
    dialect: Dialect,
}
#[derive(Debug, Clone, PartialEq)]
enum CaseClause {
  Start,
  When,
  Then,
  Else,
  End,
}

#[derive(Debug, Clone, PartialEq)]
enum SqlClause {
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
enum SqlContext {
    Root,
    SelectBlock,
    SelectBlockClause(SqlClause),
    SubQuery,
    CaseStatement(CaseClause),
    WithClause,
    WindowOver,
    Parens,
}

// Phase 1: FormatAst Builder - Context-aware token processor
struct FormatAstBuilder {
    tokens: VecDeque<Token>,
    ast: FormatAst,
    context_stack: Vec<SqlContext>,
    group_stack: Vec<Vec<FormatElement>>,
}

impl FormatAstBuilder {
    fn new(tokens: Vec<Token>, _dialect: Dialect) -> Self {
        let token_queue = VecDeque::from(tokens);

        Self {
            tokens: token_queue,
            ast: FormatAst::default(),
            context_stack: vec![SqlContext::Root],
            group_stack: Vec::new(),
        }
    }

    fn current_context(&self) -> &SqlContext {
        self.context_stack.last().unwrap_or(&SqlContext::Root)
    }

    fn context_contains(&self, ctx: SqlContext, max_look_back: usize) -> bool {
        for (i, element) in self.context_stack.iter().rev().enumerate() {
            if i > max_look_back {
                return false;
            }
            if std::mem::discriminant(element) == std::mem::discriminant(&ctx) {
                return true;
            }
        }
        false
    }

    fn push_context(&mut self, context: SqlContext) {
        self.context_stack.push(context);
    }

    fn pop_context(&mut self) -> Option<SqlContext> {
        if self.context_stack.len() > 1 {
            self.context_stack.pop()
        } else {
            None
        }
    }

    fn terminate_contexts_for(&mut self, new_context: SqlContext) {
        if matches!(new_context, SqlContext::SelectBlockClause(_)) {
            let mut contexts_to_terminate = 0;
            for context in self.context_stack.iter().rev() {
                if matches!(context, SqlContext::SelectBlockClause(_)) {
                    contexts_to_terminate += 1;
                } else {
                    break;
                }
            }

            for _ in 0..contexts_to_terminate {
                if let Some(SqlContext::SelectBlockClause(_)) = self.pop_context() {
                    self.push_to_current_target(FormatElement::Dedent);
                    self.push_to_current_target(FormatElement::SoftBreak);
                    self.end_group();
                }
            }
        }
    }

    fn start_group(&mut self) {
        self.group_stack.push(Vec::new());
    }

    fn end_group(&mut self) {
        let latest_group = self.group_stack.pop().unwrap_or_default();
        self.push_to_current_target(FormatElement::Group(latest_group));
    }

    fn push_to_current_target(&mut self, element: FormatElement) {
        if let Some(current_group) = self.group_stack.last_mut() {
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

    fn build(mut self) -> FormatAst {
        while let Some(token) = self.advance() {
            self.process_token(token);
        }
        self.ast
    }

    fn process_token(&mut self, token: Token) {
        match &token.token_type {
            TokenType::EOF => {
                while !self.group_stack.is_empty() {
                    self.end_group();
                }
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
                if self.context_contains(SqlContext::WindowOver, 4) {
                    self.push_to_current_target(FormatElement::SoftBreak);
                    self.push_to_current_target(FormatElement::Token(token));
                    self.push_to_current_target(FormatElement::Space);
                    return;
                }
                let token_type = token.token_type.clone();
                self.handle_whereish(token, &token_type);
            }
            TokenType::Over => {
                self.push_to_current_target(FormatElement::Space);
                self.push_to_current_target(FormatElement::Token(token));
                self.push_to_current_target(FormatElement::Space);
                if let Some(next_token) = self.peek() {
                    if next_token.token_type == TokenType::LeftParen {
                        self.push_context(SqlContext::WindowOver)
                    }
                }
            }
            TokenType::Keyword | TokenType::In | TokenType::As => self.handle_keyword(token),
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
            TokenType::Operator => self.handle_operator(token),
            TokenType::LeftParen => self.handle_left_paren(token),
            TokenType::RightParen => self.handle_right_paren(token),
            TokenType::JinjaIf
            | TokenType::JinjaFor
            | TokenType::JinjaElif
            | TokenType::JinjaElse
            | TokenType::JinjaEndif
            | TokenType::JinjaEndfor => self.handle_jinja_block(token),
            TokenType::TemplateVariable | TokenType::TemplateBlock => {
                self.handle_jinja_template(token)
            }
            _ => {
                if let Some(next_token) = self.peek() {
                    match next_token.token_type {
                        TokenType::LeftParen => {
                            // Function call - we'll let left paren handling take care of grouping
                            self.push_to_current_target(FormatElement::Token(token));
                        }
                        _ => {
                            self.push_to_current_target(FormatElement::Token(token));
                            self.push_to_current_target(FormatElement::Space);
                        }
                    }
                }
            }
        }
    }

    fn handle_select(&mut self, token: Token) {
        self.push_to_current_target(FormatElement::Token(token));
        if let Some(next_token) = self.peek() {
            if matches!(next_token.token_type, TokenType::Keyword)
                && next_token.value().to_uppercase() == "DISTINCT"
            {
                self.push_to_current_target(FormatElement::Space);
                let distinct = self.advance().unwrap();
                self.push_to_current_target(FormatElement::Token(distinct));
            }
        }
        self.start_group();
        self.push_to_current_target(FormatElement::ShortBreak);
        self.push_to_current_target(FormatElement::Indent);
        self.push_context(SqlContext::SelectBlock);
        self.push_context(SqlContext::SelectBlockClause(SqlClause::SelectFields));
    }

    fn handle_from(&mut self, token: Token) {
        let ctx = SqlContext::SelectBlockClause(SqlClause::From);
        self.terminate_contexts_for(ctx.clone());
        self.push_context(ctx);
        self.start_group();
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
    }

    fn handle_join(&mut self, token: Token) {
        let ctx = SqlContext::SelectBlockClause(SqlClause::Join);
        self.terminate_contexts_for(ctx.clone());
        self.push_context(ctx);
        self.push_to_current_target(FormatElement::HardBreak);
        self.start_group();
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
        self.push_to_current_target(FormatElement::Indent);
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
        self.push_context(ctx);
        self.push_to_current_target(FormatElement::HardBreak);
        self.start_group();
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
        self.push_to_current_target(FormatElement::Indent);
        self.push_to_current_target(FormatElement::Space);
    }

    fn handle_partition_by(&mut self, token: Token) {
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
    }

    fn handle_case(&mut self, token: Token) {
        // Start the main CASE group that will contain everything
        self.start_group();
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::HardBreak);
        self.push_to_current_target(FormatElement::Indent);
        self.push_context(SqlContext::CaseStatement(CaseClause::Start));
    }

    fn handle_when(&mut self, token: Token) {
        match self.current_context() {
            SqlContext::CaseStatement(CaseClause::Start) => {
                // First WHEN after CASE - just start the group
            },
            SqlContext::CaseStatement(CaseClause::Then) => {
                // End the previous WHEN group before starting new one
                self.end_group();
                self.pop_context();
            },
            SqlContext::CaseStatement(CaseClause::Else) => {
                // End the ELSE group before starting new WHEN
                self.end_group();
                self.pop_context();
            },
            _ => {
                // We shouldn't get here, but handle it gracefully
            }
        }
        
        // Add a line break before each WHEN (except the first one)
        if !matches!(self.current_context(), SqlContext::CaseStatement(CaseClause::Start)) {
            self.push_to_current_target(FormatElement::HardBreak);
        }
        
        // Start a new group for this WHEN clause
        self.start_group();
        self.push_context(SqlContext::CaseStatement(CaseClause::When));
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
    }

    fn handle_then(&mut self, token: Token) {
        // Add a soft break that allows "WHEN condition THEN" to be on same line if it fits
        self.push_to_current_target(FormatElement::SoftBreak);
        self.push_context(SqlContext::CaseStatement(CaseClause::Then));
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
    }

    fn handle_else(&mut self, token: Token) {
        // End the current WHEN group if we're in one
        if matches!(self.current_context(), SqlContext::CaseStatement(CaseClause::Then)) {
            self.end_group();
            self.pop_context();
        }
        
        // Add line break before ELSE
        self.push_to_current_target(FormatElement::HardBreak);
        
        // Start ELSE group
        self.start_group();
        self.push_context(SqlContext::CaseStatement(CaseClause::Else));
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
    }

    fn handle_end(&mut self, token: Token) {
        match self.current_context() {
            SqlContext::CaseStatement(CaseClause::Then) => {
                // End the WHEN group
                self.end_group();
                self.pop_context();
                // Now we should be back in the main CASE context
                self.push_to_current_target(FormatElement::Dedent);
                self.push_to_current_target(FormatElement::HardBreak);
                self.push_to_current_target(FormatElement::Token(token));
                // End the main CASE group
                self.end_group();
                self.pop_context();
            },
            SqlContext::CaseStatement(CaseClause::Else) => {
                // End the ELSE group
                self.end_group();
                self.pop_context();
                // Now we should be back in the main CASE context
                self.push_to_current_target(FormatElement::Dedent);
                self.push_to_current_target(FormatElement::HardBreak);
                self.push_to_current_target(FormatElement::Token(token));
                // End the main CASE group
                self.end_group();
                self.pop_context();
            },
            SqlContext::CaseStatement(CaseClause::Start) => {
                // Empty CASE statement (shouldn't happen)
                self.push_to_current_target(FormatElement::Dedent);
                self.push_to_current_target(FormatElement::HardBreak);
                self.push_to_current_target(FormatElement::Token(token));
                self.end_group();
                self.pop_context();
            },
            _ => {
                // Not in CASE context, just add the token
                self.push_to_current_target(FormatElement::Token(token));
            }
        }
        
        // Only add space after END if we're not in a CASE context
        // This allows proper line breaking for subsequent SQL clauses like FROM
        if !matches!(
            self.current_context(),
            SqlContext::SelectBlockClause(SqlClause::SelectFields)
        ) {
            self.push_to_current_target(FormatElement::Space);
        }
    }

    fn handle_with(&mut self, token: Token) {
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
        self.push_context(SqlContext::WithClause);
    }

    fn handle_keyword(&mut self, token: Token) {
        let keyword = token.value().to_uppercase();

        match keyword.as_str() {
            "AND" | "OR" => {
                // Check if we're in a group first
                if !self.group_stack.is_empty() {
                    // In a group: replace trailing space with SoftBreak, but avoid double spaces
                    if let Some(current_group) = self.group_stack.last_mut() {
                        if matches!(current_group.last(), Some(FormatElement::Space)) {
                            current_group.pop();
                            current_group.push(FormatElement::SoftBreak);
                        }
                    }
                    self.push_to_current_target(FormatElement::Token(token));
                    self.push_to_current_target(FormatElement::Space);
                } else if matches!(
                    self.current_context(),
                    SqlContext::SelectBlockClause(SqlClause::Where)
                        | SqlContext::SelectBlockClause(SqlClause::Having)
                        | SqlContext::SelectBlockClause(SqlClause::Qualify)
                        | SqlContext::SelectBlockClause(SqlClause::Join)
                ) {
                    // Outside groups in conditional contexts: use hard breaks
                    self.push_to_current_target(FormatElement::HardBreak);
                    self.push_to_current_target(FormatElement::Token(token));
                    self.push_to_current_target(FormatElement::Space);
                } else {
                    // Default: use soft breaks for better line breaking
                    self.push_to_current_target(FormatElement::SoftBreak);
                    self.push_to_current_target(FormatElement::Token(token));
                    self.push_to_current_target(FormatElement::Space);
                }
            }
            _ => {
                self.push_to_current_target(FormatElement::Token(token));
                self.push_to_current_target(FormatElement::Space);
            }
        }
    }

    fn handle_value(&mut self, token: Token) {
        let curr_type = token.token_type.clone();
        self.push_to_current_target(FormatElement::Token(token));
        if let Some(next) = self.peek() {
            match next.token_type {
                TokenType::Comma | TokenType::Dot => {}
                TokenType::RightParen | TokenType::LeftParen => {
                    if curr_type != TokenType::Identifier {
                        self.push_to_current_target(FormatElement::Space);
                    }
                }
                _ => {
                    self.push_to_current_target(FormatElement::Space);
                }
            }
        }
    }

    fn handle_comma(&mut self, token: Token) {
        self.push_to_current_target(FormatElement::Token(token));

        // Check if we're in any group (not just InList)
        if !self.group_stack.is_empty() {
            // In a group: use soft breaks that adapt to width
            self.push_to_current_target(FormatElement::SoftBreak);
        } else {
            // Outside groups: use context-specific formatting
            match self.current_context() {
                SqlContext::SelectBlockClause(SqlClause::SelectFields) => {
                    self.push_to_current_target(FormatElement::HardBreak);
                }
                SqlContext::SelectBlockClause(SqlClause::GroupBy) => {
                    // First comma in GROUP BY: switch to multiline and indent
                    if self.group_stack.is_empty() {
                        // Start a group and break after GROUP BY
                        self.start_group();
                        self.insert_group_by_break();
                        self.push_to_current_target(FormatElement::Indent);
                    }
                    self.push_to_current_target(FormatElement::SoftBreak);
                }
                _ => {
                    self.push_to_current_target(FormatElement::Space);
                }
            }
        }
    }

    fn handle_dot(&mut self, token: Token) {
        self.push_to_current_target(FormatElement::Token(token));
    }

    fn handle_operator(&mut self, token: Token) {
        match token.value().as_str() {
            "=" | ">" | "<" | ">=" | "<=" | "!=" | "<>" => {
                // Check if we're in a group and need to manage spaces smartly
                if !self.group_stack.is_empty() {
                    // In a group: check if last element was a space to avoid doubles
                    if let Some(current_group) = self.group_stack.last_mut() {
                        if !matches!(current_group.last(), Some(FormatElement::Space)) {
                            current_group.push(FormatElement::Space);
                        }
                    }
                } else {
                    self.push_to_current_target(FormatElement::Space);
                }
                self.push_to_current_target(FormatElement::Token(token));
                self.push_to_current_target(FormatElement::Space);
            }
            "+" | "-" => {
                // Addition/subtraction: these should break in groups for readability
                if !self.group_stack.is_empty() {
                    // In a group: replace any trailing space with soft break for line breaking before operator
                    if let Some(current_group) = self.group_stack.last_mut() {
                        if let Some(FormatElement::Space) = current_group.last() {
                            current_group.pop();
                            current_group.push(FormatElement::SoftBreak);
                        }
                    }
                } else {
                    self.push_to_current_target(FormatElement::Space);
                }
                self.push_to_current_target(FormatElement::Token(token));
                self.push_to_current_target(FormatElement::Space);
            }
            "*" | "/" | "%" => {
                // Multiplication/division: normal spacing, higher precedence operations usually stay together
                if !self.group_stack.is_empty() {
                    if let Some(current_group) = self.group_stack.last_mut() {
                        let has_preceding_token = current_group
                            .iter()
                            .any(|el| matches!(el, FormatElement::Token(_)));
                        if has_preceding_token
                            && !matches!(current_group.last(), Some(FormatElement::Space))
                        {
                            current_group.push(FormatElement::Space);
                        }
                    }
                } else {
                    self.push_to_current_target(FormatElement::Space);
                }
                self.push_to_current_target(FormatElement::Token(token));
                // Don't add trailing space if next token is closing paren, comma, or dot
                if let Some(next) = self.peek() {
                    if !matches!(
                        next.token_type,
                        TokenType::RightParen | TokenType::Comma | TokenType::Dot
                    ) {
                        self.push_to_current_target(FormatElement::Space);
                    }
                } else {
                    self.push_to_current_target(FormatElement::Space);
                }
            }
            _ => {
                self.push_to_current_target(FormatElement::Token(token));
                // Don't add space if next token is closing paren, comma, or dot
                if let Some(next) = self.peek() {
                    if !matches!(
                        next.token_type,
                        TokenType::RightParen | TokenType::Comma | TokenType::Dot
                    ) {
                        self.push_to_current_target(FormatElement::Space);
                    }
                } else {
                    self.push_to_current_target(FormatElement::Space);
                }
            }
        }
    }

    fn handle_left_paren(&mut self, token: Token) {
        self.push_to_current_target(FormatElement::Token(token));
        self.start_group();
        let mut context_buff: Vec<SqlContext> = Vec::new();
        if let Some(SqlContext::WindowOver) = self.context_stack.last() {
            context_buff.push(self.context_stack.pop().unwrap());
        }
        self.push_context(SqlContext::Parens);
        for element in context_buff.iter() {
            self.push_context(element.clone());
        }
        if let Some(next_token) = self.peek() {
            match next_token.token_type {
                TokenType::Select => {
                    self.push_to_current_target(FormatElement::SoftLine);
                    self.push_to_current_target(FormatElement::Indent);
                    self.push_context(SqlContext::SubQuery);
                }
                _ => {
                    self.push_to_current_target(FormatElement::SoftLine);
                    self.push_to_current_target(FormatElement::Indent);
                }
            }
        } else {
            // Default - start with softline + indent
            self.push_to_current_target(FormatElement::SoftLine);
            self.push_to_current_target(FormatElement::Indent);
        }
    }

    fn handle_right_paren(&mut self, token: Token) {
        while let Some(context) = self.context_stack.last() {
            match context {
                SqlContext::Parens => {
                    self.push_to_current_target(FormatElement::Dedent);
                    self.push_to_current_target(FormatElement::SoftLine);
                    self.end_group();
                    self.pop_context();
                    break;
                }
                SqlContext::SelectBlockClause(_) => {
                    self.push_to_current_target(FormatElement::Dedent);
                    self.push_to_current_target(FormatElement::SoftBreak);
                    self.end_group();
                    self.pop_context();
                }
                _other_context => {
                    self.pop_context();
                }
            }
        }
        self.push_to_current_target(FormatElement::Token(token));
        if let Some(next_token) = self.peek() {
            match next_token.token_type {
                TokenType::Comma => return,
                _ => {}
            }
        };
        self.push_to_current_target(FormatElement::Space);
    }

    fn handle_jinja_block(&mut self, token: Token) {
        self.push_to_current_target(FormatElement::HardBreak);
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::HardBreak);
    }

    fn handle_jinja_template(&mut self, token: Token) {
        self.push_to_current_target(FormatElement::Token(token));
        self.push_to_current_target(FormatElement::Space);
    }

    fn insert_group_by_break(&mut self) {
        // Find the most recent GROUP BY pattern and insert HardBreak after the Space
        for i in (1..self.ast.elements.len()).rev() {
            if let FormatElement::Token(ref token) = &self.ast.elements[i] {
                if token.value().to_uppercase() == "BY" {
                    // Look backward to check if this is preceded by GROUP
                    if i >= 2 {
                        if let FormatElement::Token(ref group_token) = &self.ast.elements[i - 2] {
                            if group_token.value().to_uppercase() == "GROUP" {
                                // Found GROUP BY - insert HardBreak after the Space
                                // Pattern: GROUP, Space, BY, Space, [need HardBreak here]
                                if i + 1 < self.ast.elements.len()
                                    && matches!(&self.ast.elements[i + 1], FormatElement::Space)
                                {
                                    self.ast.elements.insert(i + 2, FormatElement::HardBreak);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

struct FormatRenderer<'a> {
    settings: &'a FormatSettings,
    ast: FormatAst,
    indent_level: usize,
    column: usize,
}

impl<'a> FormatRenderer<'a> {
    fn new(settings: &'a FormatSettings, ast: FormatAst) -> Self {
        Self {
            settings,
            ast,
            indent_level: 0,
            column: 0,
        }
    }

    fn render(&mut self) -> (String, Vec<FormatElement>) {
        let elements = self.ast.elements().to_vec(); // Clone to avoid borrow checker issues
        let mut flat_format_list: Vec<FormatElement> = Vec::new();

        // First pass: render to get the flat format list
        let mut temp_output = String::new();
        self.render_group(&elements, &mut temp_output, &mut flat_format_list);
        let final_output = self.normalize_final_format_list(&flat_format_list);
        (final_output, flat_format_list)
    }

    fn regenerate_output_from_flat_list(&mut self, elements: &[FormatElement]) -> String {
        let mut output = String::new();
        self.column = 0;
        let mut current_indent = 0usize;

        for element in elements {
            match element {
                FormatElement::Token(token) => {
                    // Apply indentation if we're at line start
                    if self.column == 0 && current_indent > 0 {
                        let indent_str = match self.settings.indent_style {
                            IndentStyle::Spaces => {
                                " ".repeat(current_indent * self.settings.indent_size)
                            }
                            IndentStyle::Tabs => "\t".repeat(current_indent),
                        };
                        output.push_str(&indent_str);
                        self.column += indent_str.len();
                    }
                    let text = token.to_string(&Dialect::default());
                    output.push_str(&text);
                    self.column += text.len();
                }
                FormatElement::Space => {
                    output.push(' ');
                    self.column += 1;
                }
                FormatElement::HardBreak => {
                    output.push('\n');
                    self.column = 0;
                }
                FormatElement::LineGap => {
                    output.push_str("\n\n");
                    self.column = 0;
                }
                // These shouldn't appear in the final flat list, but handle them just in case
                FormatElement::Indent => {
                    current_indent += 1;
                }
                FormatElement::Dedent => {
                    current_indent = current_indent.saturating_sub(1);
                }
                _ => {
                    // Group, SoftBreak, etc. should already be resolved
                }
            }
        }

        output
    }

    fn normalize_final_format_list(&self, elements: &[FormatElement]) -> String {
        let mut output = String::new();
        let mut format_vec: Vec<FormatElement> = Vec::new();
        let mut current_indent = 0isize;
        for element in elements {
            println!(" {:?} ", element.clone());
            match element {
                FormatElement::Token(token) => {
                    if format_vec.is_empty() {
                        output.push_str(&token.to_string(&Dialect::default()));
                        continue;
                    }
                    let (indent_change, resolved_el) = self.normalize_format_sequence(&format_vec);
                    current_indent += indent_change;
                    if current_indent < 0 {
            current_indent = 0
          }
                    println!("{current_indent} {indent_change} {:?} ", resolved_el.clone());
                    format_vec.clear();
                    if let Some(resolved) = resolved_el {
                        match resolved {
                            FormatElement::Space => output.push(' '),
                            FormatElement::HardBreak => {
                                output.push('\n');
                                // Apply indentation if we have any
                                if current_indent > 0 {
                                    let indent_str = self.make_indent_(current_indent as usize);
                                    output.push_str(&indent_str);
                                }
                            }
                            FormatElement::LineGap => {
                                output.push_str("\n\n");
                                // Apply indentation if we have any
                                if current_indent > 0 {
                                    let indent_str = self.make_indent_(current_indent as usize);
                                    output.push_str(&indent_str);
                                }
                            }
                            _ => {}
                        }
                    }
                    output.push_str(&token.to_string(&Dialect::default()));
                }
                _ => {
                    format_vec.push(element.clone());
                }
            }
        }
        output
    }

    fn normalize_format_sequence(
        &self,
        elements: &[FormatElement],
    ) -> (isize, Option<FormatElement>) {
        let mut element: Option<FormatElement> = None;
        let mut indent_change: isize = 0;
        for el in elements {
            match el {
                FormatElement::Indent => indent_change = indent_change + 1,
                FormatElement::Dedent => indent_change = indent_change - 1,
                _ => {
                    // Only consider non-indent elements for priority selection
                    if let Some(e) = element.clone() {
                        if el.prio() < e.prio() {
                            element = Some(el.clone());
                        }
                    } else {
                        element = Some(el.clone());
                    }
                }
            }
        }
        return (indent_change, element);
    }

    fn render_element(
        &mut self,
        element: &FormatElement,
        output: &mut String,
        flat_format_list: &mut Vec<FormatElement>,
    ) {
        match element {
            FormatElement::Token(token) => {
                self.render_token(token, output);
                flat_format_list.push(element.clone());
            }
            FormatElement::Space => {
                output.push(' ');
                flat_format_list.push(FormatElement::Space);
                self.column += 1;
            }
            FormatElement::HardBreak => {
                output.push('\n');
                flat_format_list.push(FormatElement::HardBreak);
                self.column = 0;
            }
            FormatElement::LineGap => {
                output.push_str("\n\n");
                flat_format_list.push(FormatElement::LineGap);
                self.column = 0;
            }
            FormatElement::Indent => {
                flat_format_list.push(FormatElement::Indent);
                self.indent_level += 1;
            }
            FormatElement::Dedent => {
                flat_format_list.push(FormatElement::Dedent);
                self.indent_level = self.indent_level.saturating_sub(1);
            }
            FormatElement::Group(group_elements) => {
                self.render_group(group_elements, output, flat_format_list);
            }
            FormatElement::SoftBreak | FormatElement::ShortBreak | FormatElement::SoftLine => {
                // These should be resolved by groups, but if they appear outside groups, treat as breaks
                output.push('\n');
                flat_format_list.push(FormatElement::HardBreak);
                self.column = 0;
            }
        }
    }

    fn render_token(&mut self, token: &Token, output: &mut String) {
        // Apply indentation if we're at line start
        if self.column == 0 && self.indent_level > 0 {
            let indent_str = self.make_indent();
            output.push_str(&indent_str);
            self.column += indent_str.len();
        }

        let text = token.to_string(&Dialect::default());
        output.push_str(&text);
        self.column += text.len();
    }

    fn render_group(
        &mut self,
        elements: &[FormatElement],
        output: &mut String,
        flat_format_list: &mut Vec<FormatElement>,
    ) {
        // Wadler's algorithm: try flat first, then break if too wide
        if let Some((flat_text, flat_elements)) = self.try_render_flat(elements) {
            let would_fit = self.column + flat_text.len() <= self.settings.line_width;

            if would_fit {
                output.push_str(&flat_text);
                flat_format_list.extend(flat_elements);
                self.column += flat_text.len();
                return;
            }
        }

        self.render_group_with_breaks(elements, output, flat_format_list);
    }

    fn try_render_flat(&self, elements: &[FormatElement]) -> Option<(String, Vec<FormatElement>)> {
        if self.should_fail_flat_due_to_short_break(elements) {
            return None;
        }

        let mut flat_output = String::new();
        let mut flat_format_list: Vec<FormatElement> = Vec::new();
        let mut format_vec: Vec<FormatElement> = Vec::new();

        for element in elements {
            match element {
                FormatElement::Group(group) => {
                    if group.len() == 0 {
                        continue;
                    }
                }
                _ => {}
            }

            match element {
                FormatElement::Group(_) | FormatElement::Token(_) => {
                    if format_vec.len() > 0 {
                        let (_, resolved_el) = self.normalize_format_sequence(&format_vec);
                        format_vec.clear();
                        match resolved_el {
                            None => return None,
                            Some(FormatElement::HardBreak) | Some(FormatElement::LineGap) => {
                                return None; // Can't flatten hard breaks
                            }
                            Some(FormatElement::SoftBreak) | Some(FormatElement::ShortBreak) => {
                                flat_output.push(' ');
                                flat_format_list.push(FormatElement::Space);
                            }
                            Some(FormatElement::Space) => {
                                flat_output.push(' ');
                                flat_format_list.push(FormatElement::Space);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
            match element {
                FormatElement::Group(nested_group) => {
                    if let Some((nested_flat, more_flat)) = self.try_render_flat(nested_group) {
                        flat_output.push_str(&nested_flat);
                        flat_format_list.extend(more_flat);
                    } else {
                        return None;
                    }
                }
                FormatElement::Token(token) => {
                    flat_output.push_str(&token.to_string(&Dialect::default()));
                    flat_format_list.push(element.clone());
                }
                format_el => {
                    format_vec.push(format_el.clone());
                }
            }
        }
        if format_vec.len() > 0 {
            let (_, resolved_el) = self.normalize_format_sequence(&format_vec);
            format_vec.clear();
            match resolved_el {
                None => return None,
                Some(FormatElement::HardBreak) | Some(FormatElement::LineGap) => {
                    return None; // Can't flatten hard breaks
                }
                Some(FormatElement::SoftBreak) | Some(FormatElement::ShortBreak) => {
                    flat_format_list.push(FormatElement::Space);
                    flat_output.push(' ');
                }
                Some(FormatElement::Space) => {
                    flat_format_list.push(FormatElement::Space);
                    flat_output.push(' ');
                }
                _ => {}
            }
        }
        Some((flat_output, flat_format_list))
    }

    fn delta_indent(&mut self, indent_change: isize) {
        if indent_change == 0 {
            return;
        }
        if indent_change > 0 {
            self.indent_level += indent_change as usize;
            return;
        }
        self.indent_level = self
            .indent_level
            .saturating_sub(indent_change.abs() as usize);
    }

    fn render_group_with_breaks(
        &mut self,
        elements: &[FormatElement],
        output: &mut String,
        flat_format_list: &mut Vec<FormatElement>,
    ) {
        let mut format_vec: Vec<FormatElement> = Vec::new();

        for element in elements {
            match element {
                FormatElement::Group(group) => {
                    if group.len() == 0 {
                        continue;
                    }
                }
                _ => {}
            };
            match element {
                FormatElement::Group(_) | FormatElement::Token(_) => {
                    if !format_vec.is_empty() {
                        let (indent_change, resolved_el) =
                            self.normalize_format_sequence(&format_vec);
                        format_vec.clear();
                        self.delta_indent(indent_change);
                        
                        // Add the indent/dedent elements to flat_format_list
                        if indent_change > 0 {
                            for _ in 0..indent_change {
                                flat_format_list.push(FormatElement::Indent);
                            }
                        } else if indent_change < 0 {
                            for _ in 0..(-indent_change) {
                                flat_format_list.push(FormatElement::Dedent);
                            }
                        }
                        
                        match resolved_el {
                            Some(FormatElement::HardBreak) => {
                                output.push('\n');
                                flat_format_list.push(FormatElement::HardBreak);
                                self.column = 0
                            }
                            Some(FormatElement::LineGap) => {
                                output.push_str("\n\n");
                                flat_format_list.push(FormatElement::LineGap);
                                self.column = 0;
                            }
                            Some(FormatElement::SoftBreak)
                            | Some(FormatElement::ShortBreak)
                            | Some(FormatElement::SoftLine) => {
                                output.push('\n');
                                flat_format_list.push(FormatElement::HardBreak);
                                self.column = 0;
                            }
                            Some(FormatElement::Space) => {
                                output.push(' ');
                                flat_format_list.push(FormatElement::Space);
                                self.column += 1;
                            }
                            _ => {}
                        }
                    }
                    self.render_element(element, output, flat_format_list);
                }
                _ => {
                    format_vec.push(element.clone());
                }
            }
        }

        if format_vec.is_empty() {
            return;
        }
        let (indent_change, resolved_el) = self.normalize_format_sequence(&format_vec);
        self.delta_indent(indent_change);
        
        // Add the indent/dedent elements to flat_format_list
        if indent_change > 0 {
            for _ in 0..indent_change {
                flat_format_list.push(FormatElement::Indent);
            }
        } else if indent_change < 0 {
            for _ in 0..(-indent_change) {
                flat_format_list.push(FormatElement::Dedent);
            }
        }
        
        match resolved_el {
            Some(FormatElement::HardBreak) => {
                output.push('\n');
                flat_format_list.push(FormatElement::HardBreak);
                self.column = 0
            }
            Some(FormatElement::LineGap) => {
                output.push_str("\n\n");
                flat_format_list.push(FormatElement::LineGap);
                self.column = 0;
            }
            Some(FormatElement::SoftBreak)
            | Some(FormatElement::ShortBreak)
            | Some(FormatElement::SoftLine) => {
                output.push('\n');
                flat_format_list.push(FormatElement::HardBreak);
                self.column = 0;
            }
            Some(FormatElement::Space) => {
                output.push(' ');
                flat_format_list.push(FormatElement::Space);
                self.column += 1;
            }
            _ => {}
        }
    }

    fn should_fail_flat_due_to_short_break(&self, elements: &[FormatElement]) -> bool {
        if !elements.contains(&FormatElement::ShortBreak) {
            return false;
        }
        if elements.len() > 4 {
            return true;
        }

        for element in elements {
            if let FormatElement::Token(token) = element {
                if matches!(token.token_type, TokenType::Comma) {
                    return true; // Force break mode if there's ShortBreak + comma
                }
            }
        }
        false // Don't fail flat if no comma
    }

    fn make_indent(&self) -> String {
        self.make_indent_(self.indent_level)
    }
    fn make_indent_(&self, indent_level: usize) -> String {
        match self.settings.indent_style {
            IndentStyle::Spaces => " ".repeat(indent_level * self.settings.indent_size),
            IndentStyle::Tabs => "\t".repeat(indent_level),
        }
    }
}

impl Formatter {
    pub fn new(settings: FormatSettings) -> Self {
        Self {
            settings,
            dialect: Dialect::default(),
        }
    }

    pub fn with_dialect(settings: FormatSettings, dialect: Dialect) -> Self {
        Self { settings, dialect }
    }

    pub fn format_tokens(&self, tokens: &[Token]) -> Result<String, FormatterError> {
        // Phase 1: Build FormatAst from tokens
        let ast_builder = FormatAstBuilder::new(tokens.to_vec(), self.dialect.clone());
        let format_ast = ast_builder.build();

        // Phase 2: Render FormatAst to String
        let mut renderer = FormatRenderer::new(&self.settings, format_ast);
        let (output, _flat_elements) = renderer.render();
        Ok(output)
    }

    pub fn format_tokens_with_elements(
        &self,
        tokens: &[Token],
    ) -> Result<(String, Vec<FormatElement>), FormatterError> {
        // Phase 1: Build FormatAst from tokens
        let ast_builder = FormatAstBuilder::new(tokens.to_vec(), self.dialect.clone());
        let format_ast = ast_builder.build();

        // Phase 2: Render FormatAst to String and flattened elements
        let mut renderer = FormatRenderer::new(&self.settings, format_ast);
        Ok(renderer.render())
    }
}

#[derive(Debug)]
pub enum FormatterError {
    InvalidToken { message: String },
}

impl std::fmt::Display for FormatterError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FormatterError::InvalidToken { message } => write!(f, "Invalid token: {message}"),
        }
    }
}

impl std::error::Error for FormatterError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::Tokenizer;
    use serde::Deserialize;
    use std::fs;

    #[derive(Debug, Deserialize)]
    struct FormatterTestCase {
        name: String,
        input: String,
        settings: Option<TestSettings>,
        expected: String,
    }

    #[derive(Debug, Deserialize)]
    struct FormatterTestFile {
        file: Vec<FormatterTestCase>,
    }

    fn load_formatter_test_file(path: &str) -> FormatterTestFile {
        let content = fs::read_to_string(path).expect("Failed to read formatter test file");
        serde_yaml::from_str(&content).expect("Failed to parse formatter YAML")
    }

    fn pprint_format_ast(elements: &[FormatElement], indent: usize, item: usize) {
        for (i, element) in elements.iter().enumerate() {
            let index = i + item;
            let indent_value = " ".repeat(indent);
            match element {
                FormatElement::Group(subelements) => {
                    let size_ = subelements.len();
                    if size_ == 0 {
                        continue;
                    }
                    println!("{indent_value}[{index}] - Group s {size_} -");
                    pprint_format_ast(subelements, indent + 1, index);
                }
                _ => {
                    println!("{indent_value}[{index}] {element:?}");
                }
            }
        }
    }

    fn create_visual_diff(expected: &str, actual: &str) -> String {
        let expected_lines: Vec<&str> = expected.split('\n').collect();
        let actual_lines: Vec<&str> = actual.split('\n').collect();
        let max_lines = expected_lines.len().max(actual_lines.len());

        const GREEN: &str = "\x1b[32m";
        const RED: &str = "\x1b[31m";
        const RESET: &str = "\x1b[0m";

        let mut output = String::new();
        output.push_str("\nExpected vs Actual (marked lines differ):\n");

        for i in 0..max_lines {
            let expected_line = expected_lines.get(i).unwrap_or(&"");
            let actual_line = actual_lines.get(i).unwrap_or(&"");
            let line_num = i + 1;

            if expected_line == actual_line {
                output.push_str(&format!(
                    "{GREEN}✓{:3}{RESET} │ {}\n",
                    line_num,
                    expected_line.replace(' ', "·")
                ));
            } else {
                output.push_str(&format!(
                    "{RED}✗{:3}{RESET} │ Expected: {}\n",
                    line_num,
                    expected_line.replace(' ', "·")
                ));
                output.push_str(&format!(
                    "{RED}   {RESET} │ Actual:   {}\n",
                    actual_line.replace(' ', "·")
                ));
                output.push_str(&format!(
                    "{RED}   {RESET} │ {RED}{}{}\n",
                    "─".repeat(20),
                    RESET
                ));
            }
        }

        output
    }

    fn run_formatter_tests(test_file_path: &str) {
        let test_file = load_formatter_test_file(test_file_path);

        for test_case in test_file.file {
            // Tokenize input
            let mut tokenizer = Tokenizer::new(&test_case.input);
            let tokenizer_result = tokenizer.tokenize().expect("Tokenization should succeed");

            // Create formatter with settings
            let format_settings = test_case
                .settings
                .as_ref()
                .map(FormatSettings::from)
                .unwrap_or_default();

            let formatter = Formatter::new(format_settings);

            // Format tokens
            let actual_output = formatter
                .format_tokens(&tokenizer_result.tokens)
                .expect("Formatting should succeed");

            // Compare with expected (trim trailing whitespace for comparison)
            let expected = test_case.expected.trim();
            let actual = actual_output.trim();

            if expected != actual {
                println!("FormatAst for failing test '{}':", test_case.name);
                let ast_builder =
                    FormatAstBuilder::new(tokenizer_result.tokens.clone(), Dialect::default());
                let format_ast = ast_builder.build();
                pprint_format_ast(format_ast.elements(), 0, 0);
                let diff_output = create_visual_diff(expected, actual);
                panic!(
                    "Formatter test '{}' failed:\n{}",
                    test_case.name, diff_output
                );
            }
        }
    }

    #[test]
    fn test_basic_formatting() {
        run_formatter_tests("test/fixtures/formatter/basic.yml");
    }

    #[test]
    fn test_group_by_only() {
        let test_file_path = "test/fixtures/formatter/basic.yml";
        let test_file = load_formatter_test_file(test_file_path);

        // Find just the group_by_multiple_columns test
        let test_case = test_file
            .file
            .iter()
            .find(|tc| tc.name == "group_by_multiple_columns")
            .expect("group_by_multiple_columns test not found");

        // Tokenize input
        let mut tokenizer = Tokenizer::new(&test_case.input);
        let tokenizer_result = tokenizer.tokenize().expect("Tokenization should succeed");

        // Create formatter with settings
        let format_settings = test_case
            .settings
            .as_ref()
            .map(FormatSettings::from)
            .unwrap_or_default();

        let formatter = Formatter::new(format_settings);

        // Format tokens
        let actual_output = formatter
            .format_tokens(&tokenizer_result.tokens)
            .expect("Formatting should succeed");

        // Compare with expected (trim trailing whitespace for comparison)
        let expected = test_case.expected.trim();
        let actual = actual_output.trim();

        if expected != actual {
            println!("FormatAst for failing test '{}':", test_case.name);
            let ast_builder =
                FormatAstBuilder::new(tokenizer_result.tokens.clone(), Dialect::default());
            let format_ast = ast_builder.build();
            pprint_format_ast(format_ast.elements(), 0, 0);
            let diff_output = create_visual_diff(expected, actual);
            panic!(
                "Formatter test '{}' failed:\n{}",
                test_case.name, diff_output
            );
        }
    }

    // #[test]
    // fn test_complex_formatting() {
    //     run_formatter_tests("test/fixtures/formatter/complex.yml");
    // }
    //
    #[test]
    fn test_medium_sql_formatting() {
        run_formatter_tests("test/fixtures/formatter/medium_sql.yml");
    }

    #[test]
    fn test_case_statement_formatting() {
        run_formatter_tests("test/fixtures/formatter/case_test.yml");
    }

    #[test]
    fn test_subquery_formatting() {
        run_formatter_tests("test/fixtures/formatter/subquery_test.yml");
    }

    #[test]
    fn test_comprehensive_case_statements() {
        run_formatter_tests("test/fixtures/formatter/case_statements.yml");
    }

    #[test]
    fn test_universal_groups() {
        run_formatter_tests("test/fixtures/formatter/universal_groups.yml");
    }

    #[test]
    fn test_break_behavior() {
        run_formatter_tests("test/fixtures/formatter/break_behavior.yml");
    }
}

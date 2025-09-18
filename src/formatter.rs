use crate::dialect::Dialect;
use crate::tokenizer::{CommentType, Token, TokenType};
use serde::Deserialize;
use std::collections::VecDeque;
use std::slice;

// Depth-first iterator over FormatAst
pub struct WalkIter<'a> {
    stack: Vec<slice::Iter<'a, FormatElement>>,
}

impl<'a> WalkIter<'a> {
    fn new(ast: &'a FormatAst) -> Self {
        Self {
            stack: vec![ast.elements.iter()],
        }
    }
}

impl<'a> Iterator for WalkIter<'a> {
    type Item = &'a FormatElement;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let iter = self.stack.last_mut()?;
            if let Some(elem) = iter.next() {
                match elem {
                    FormatElement::Group(children) => {
                        // yield the group itself *first*, then descend
                        self.stack.push(children.iter());
                        return Some(elem);
                    }
                    _ => return Some(elem),
                }
            } else {
                // finished this level, pop and continue
                self.stack.pop();
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
    pub fn walk(&self) -> WalkIter<'_> {
        WalkIter::new(self)
    }
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
    Root,
    WhenThen,
    When,
    Then,
    Else,
}

#[derive(Debug, Clone, PartialEq)]
enum ParenType {
    WindowOver,
    SubQuery,
    CTESubQuery,
    Function,
    DataType,
    Other,
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
    SelectBlockClause(SqlClause),
    CaseStatement(CaseClause),
    WithClause,
    Parens(ParenType),
    JinjaBlock,
}

struct FormatAstBuilder {
    tokens: VecDeque<Token>,
    ast: FormatAst,
    context_stack: Vec<SqlContext>,
    group_stack: Vec<Vec<FormatElement>>,
    format_mode: bool,
}

impl FormatAstBuilder {
    fn new(tokens: Vec<Token>, _dialect: Dialect) -> Self {
        let token_queue = VecDeque::from(tokens);

        Self {
            format_mode: true,
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

    fn end_contexts<F>(&mut self, stop_fn: F, stop_type: StackPopType)
    where
        F: Fn(&SqlContext) -> bool,
    {
        let mut stopper: Option<StackPopType> = None;
        while let Some(ctx_) = self.context_stack.last() {
            let ctx = ctx_.clone();
            if stop_fn(&ctx) {
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
                    self.push(FormatElement::Dedent);
                    self.push(FormatElement::SoftBreak);
                    self.group_end();
                }
                SqlContext::SelectBlockClause(_) => {
                    self.push(FormatElement::Dedent);
                    self.push(FormatElement::SoftBreak);
                    self.group_end();
                }
                SqlContext::Parens(_) => {
                    self.push(FormatElement::Dedent);
                    self.push(FormatElement::SoftLine);
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
        self.push_context(ctx);
        self.group_stack.push(Vec::new());
    }

    fn group_end(&mut self) {
        let latest_group = self.group_stack.pop().unwrap_or_default();
        self.push(FormatElement::Group(latest_group));
        self.pop_context();
    }

    fn push(&mut self, element: FormatElement) {
        if !self.format_mode
            && !matches!(element, FormatElement::Token(_) | FormatElement::Group(_))
        {
            return;
        }
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
                    self.group_end();
                }
                self.push(FormatElement::Token(token));
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
                    self.push(FormatElement::SoftBreak);
                    self.push(FormatElement::Token(token));
                    self.push(FormatElement::Space);
                    return;
                }
                let token_type = token.token_type.clone();
                self.handle_whereish(token, &token_type);
            }
            TokenType::Over => {
                self.push(FormatElement::Space);
                self.push(FormatElement::Token(token));
                self.push(FormatElement::Space);
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
                self.push(FormatElement::Token(token));
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
                    self.push(FormatElement::Token(token));
                    return;
                }
                self.push(FormatElement::SoftBreak);
                self.push(FormatElement::Token(token));
                self.push(FormatElement::Space);
            }
            TokenType::And | TokenType::Or => {
                self.push(FormatElement::SoftBreak);
                self.push(FormatElement::Token(token));
                self.push(FormatElement::Space);
            }
            _ => {
                if let Some(next_token) = self.peek() {
                    match next_token.token_type {
                        TokenType::LeftParen => {
                            // Function call - we'll let left paren handling take care of grouping
                            self.push(FormatElement::Token(token));
                        }
                        _ => {
                            self.push(FormatElement::Token(token));
                            self.push(FormatElement::Space);
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
        if self.current_context() == &SqlContext::WithClause {
            self.group_end();
            self.push(FormatElement::LineGap);
        }
        self.group_start(SqlContext::SelectBlockClause(SqlClause::SelectFields));
        self.push(FormatElement::Token(token));
        if let Some(next_token) = self.peek() {
            if matches!(next_token.token_type, TokenType::Keyword)
                && next_token.value().to_uppercase() == "DISTINCT"
            {
                self.push(FormatElement::Space);
                let distinct = self.advance().unwrap();
                self.push(FormatElement::Token(distinct));
            }
        }
        self.push(FormatElement::ShortBreak);
        self.push(FormatElement::Indent);
    }

    fn handle_from(&mut self, token: Token) {
        let ctx = SqlContext::SelectBlockClause(SqlClause::From);
        self.terminate_contexts_for(ctx.clone());
        self.group_start(ctx);
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Space);
    }

    fn handle_join(&mut self, token: Token) {
        let ctx = SqlContext::SelectBlockClause(SqlClause::Join);
        self.terminate_contexts_for(ctx.clone());
        self.push(FormatElement::HardBreak);
        self.group_start(ctx);
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Space);
        self.push(FormatElement::Indent);
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
        self.push(FormatElement::HardBreak);
        self.group_start(ctx);
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Space);
        self.push(FormatElement::Indent);
        self.push(FormatElement::Space);
    }

    fn handle_partition_by(&mut self, token: Token) {
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Space);
    }

    fn handle_case(&mut self, token: Token) {
        self.group_start(SqlContext::CaseStatement(CaseClause::Root));
        self.push(FormatElement::Token(token));
        self.push(FormatElement::SoftBreak);
        self.push(FormatElement::Indent);
    }

    fn handle_when(&mut self, token: Token) {
        self.move_to_case_root();
        self.push(FormatElement::SoftBreak);
        self.group_start(SqlContext::CaseStatement(CaseClause::WhenThen));

        self.group_start(SqlContext::CaseStatement(CaseClause::When));
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Indent);
        self.push(FormatElement::Space);
    }

    fn handle_then(&mut self, token: Token) {
        self.push(FormatElement::Dedent);
        self.push(FormatElement::SoftBreak);
        self.group_end();
        self.push(FormatElement::Indent);
        self.push(FormatElement::SoftBreak);

        self.group_start(SqlContext::CaseStatement(CaseClause::Then));
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Indent);
        self.push(FormatElement::Space);
    }

    fn handle_else(&mut self, token: Token) {
        self.move_to_case_root();
        self.push(FormatElement::SoftBreak);

        self.group_start(SqlContext::CaseStatement(CaseClause::Else));
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Indent);
        self.push(FormatElement::Space);
    }

    fn handle_end(&mut self, token: Token) {
        self.move_to_case_root();
        self.push(FormatElement::Dedent);
        self.push(FormatElement::SoftBreak);
        self.push(FormatElement::Token(token));
        self.group_end();
        self.push(FormatElement::Space);
    }

    fn move_to_case_root(&mut self) {
        self.end_contexts(
            |ctx| matches!(ctx, SqlContext::CaseStatement(CaseClause::Root)),
            StackPopType::Before,
        );
    }

    fn handle_with(&mut self, token: Token) {
        self.group_start(SqlContext::WithClause);
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Space);
    }

    fn handle_keyword(&mut self, token: Token) {
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Space);
    }

    fn handle_value(&mut self, token: Token) {
        let curr_type = token.token_type.clone();
        self.push(FormatElement::Token(token));

        if let Some(next) = self.peek() {
            match next.token_type {
                TokenType::Comma | TokenType::Dot => {}
                TokenType::LeftParen => {
                    if curr_type == TokenType::Identifier {
                        let p_token = self.advance().unwrap();
                        self.handle_left_paren(p_token, ParenType::Function);
                        return;
                    }
                    self.push(FormatElement::Space);
                }
                _ => {
                    self.push(FormatElement::Space);
                }
            }
        }
    }

    fn handle_comma(&mut self, token: Token) {
        self.push(FormatElement::Token(token));
        match self.current_context() {
            SqlContext::WithClause => {
                self.push(FormatElement::LineGap);
            }
            _ => {
                self.push(FormatElement::SoftBreak);
            }
        }
    }

    fn handle_dot(&mut self, token: Token) {
        self.push(FormatElement::Token(token));
    }

    fn handle_operator(&mut self, token: Token) {
        match token.value().as_str() {
            "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=" => {
                self.push(FormatElement::Space);
                self.push(FormatElement::Token(token));
                self.push(FormatElement::Space);
            }
            _ => {
                self.push(FormatElement::SoftBreak);
                self.push(FormatElement::Token(token));
                self.push(FormatElement::Space);
            }
        }
    }

    fn handle_left_paren(&mut self, token: Token, paren_type: ParenType) {
        assert!(matches!(token.token_type, TokenType::LeftParen));

        if matches!(paren_type, ParenType::CTESubQuery) {
            self.group_start(SqlContext::Parens(paren_type));
            self.push(FormatElement::Token(token));
            self.push(FormatElement::SoftLine);
            self.push(FormatElement::Indent);
            return;
        }

        let peek_type = self.peek_type();
        match peek_type {
            Some(TokenType::Star) => {
                let next_t = FormatElement::Token(self.advance().unwrap());
                self.group_start(SqlContext::Parens(ParenType::Other));
                self.push(FormatElement::Token(token));
                self.push(next_t);
            }
            Some(TokenType::Select) => {
                self.group_start(SqlContext::Parens(ParenType::SubQuery));
                self.push(FormatElement::Token(token));
                self.push(FormatElement::SoftLine);
                self.push(FormatElement::Indent);
            }
            _ => {
                self.group_start(SqlContext::Parens(paren_type));
                self.push(FormatElement::Token(token));
                self.push(FormatElement::SoftLine);
                self.push(FormatElement::Indent);
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
            StackPopType::After,
        );
        self.push(FormatElement::Token(token));
        if let Some(next_token) = self.peek() {
            if next_token.token_type == TokenType::Comma {
                return;
            }
        };
        self.push(FormatElement::Space);
    }

    fn handle_jinja_template(&mut self, token: Token) {
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Space);
    }

    fn handle_jinja_start(&mut self, token: Token) {
        self.group_start(SqlContext::JinjaBlock);
        self.push(FormatElement::HardBreak);
        self.push(FormatElement::Token(token));
        self.push(FormatElement::Indent);
        self.push(FormatElement::HardBreak);
    }

    fn close_jinja_block(&mut self) {
        // Close any SQL clauses that are still open within the Jinja block
        self.end_contexts(
            |ctx| matches!(ctx, SqlContext::JinjaBlock),
            StackPopType::Before,
        );
        // Close the current Jinja content with dedent
        self.push(FormatElement::Dedent);
        self.push(FormatElement::HardBreak);
        self.group_end();
    }

    fn handle_jinja_transition(&mut self, token: Token) {
        self.close_jinja_block();
        self.handle_jinja_start(token);
    }

    fn handle_jinja_end(&mut self, token: Token) {
        self.close_jinja_block();
        self.push(FormatElement::HardBreak);
        self.push(FormatElement::Token(token));
        self.push(FormatElement::HardBreak);
    }
}

struct FormatRenderer<'a> {
    settings: &'a FormatSettings,
    dialect: Dialect,
    ast: FormatAst,
    indent_level: usize,
    column: usize,
}

impl<'a> FormatRenderer<'a> {
    fn new(dialect: Dialect, settings: &'a FormatSettings, ast: FormatAst) -> Self {
        Self {
            settings,
            dialect,
            ast,
            indent_level: 0,
            column: 0,
        }
    }

    fn render(&mut self) -> (String, Vec<FormatElement>) {
        let elements = self.ast.elements().to_vec(); // Clone to avoid borrow checker issues
        let mut flat_format_list: Vec<FormatElement> = Vec::new();
        let mut temp_output = String::new();
        self.render_group(&elements, &mut temp_output, &mut flat_format_list);
        let final_output = self.normalize_final_format_list(&flat_format_list);
        (final_output, flat_format_list)
    }

    fn line_(
        &self,
        output: &mut String,
        line_str: &mut String,
        comments: &mut Vec<(bool, Token)>,
        current_indent: isize,
    ) {
        if comments.is_empty() {
            output.push_str(line_str);
            line_str.clear();
            return;
        };

        let indent_str = if current_indent > 0 {
            self.make_indent_(current_indent as usize)
        } else {
            String::new()
        };
        for (is_first, el) in comments.iter() {
            match (is_first, el.token_type.clone()) {
                (_, TokenType::Comment(CommentType::DoubleDashLine)) => {
                    line_str.push_str(&indent_str);
                    line_str.push_str("-- ");
                    line_str.push_str(el.value().trim());
                }
                (_, TokenType::Comment(CommentType::DoubleDash)) => {
                    line_str.push_str("  -- ");
                    line_str.push_str(el.value().trim());
                }
                (_, TokenType::Comment(CommentType::MultiLine)) => {
                    let comment_value = el.value();
                    let comment_lines: Vec<&str> = comment_value.trim().lines().collect();
                    if comment_lines.len() == 1 {
                        line_str.push('\n');
                        line_str.push_str(&indent_str);
                        line_str.push_str("/* ");
                        line_str.push_str(comment_lines[0].trim());
                        line_str.push_str(" */");
                        continue;
                    }

                    line_str.push('\n');
                    line_str.push_str(&indent_str);
                    line_str.push_str("/*\n");
                    for line in comment_lines {
                        line_str.push_str(&indent_str);
                        line_str.push_str(&indent_str);
                        line_str.push_str(line.trim());
                        line_str.push('\n');
                    }
                    line_str.push_str(&indent_str);
                    line_str.push_str("*/");
                }
                _ => {}
            }
        }
        output.push_str(line_str);
        comments.clear();
        line_str.clear();
    }

    fn normalize_final_format_list(&self, elements: &[FormatElement]) -> String {
        let mut output = String::new();
        let mut line_str = String::new();
        let mut comments: Vec<(bool, Token)> = Vec::new();
        let mut format_vec: Vec<FormatElement> = Vec::new();
        let mut current_indent = 0isize;
        let mut is_after_newline = true;
        for element in elements {
            match element {
                FormatElement::Token(token) => {
                    if !token.comments.is_empty() {
                        comments.extend(
                            token
                                .comments
                                .clone()
                                .into_iter()
                                .map(|c| (is_after_newline, c)),
                        );
                    }
                    is_after_newline = false;
                    if matches!(token.token_type, TokenType::EOF) {
                        line_str = line_str.trim_end().to_string();
                        break;
                    }
                    if output.is_empty() && line_str.is_empty() && !comments.is_empty() {
                        self.line_(&mut output, &mut line_str, &mut comments, current_indent);
                        output.push('\n')
                    }
                    if format_vec.is_empty() {
                        line_str.push_str(&token.to_string(&self.dialect));
                        continue;
                    }
                    let (indent_change, resolved_el) = self.normalize_format_sequence(&format_vec);
                    current_indent += indent_change;
                    if current_indent < 0 {
                        current_indent = 0
                    }
                    format_vec.clear();
                    if let Some(resolved) = resolved_el {
                        match resolved {
                            FormatElement::Space => line_str.push(' '),
                            FormatElement::HardBreak => {
                                self.line_(
                                    &mut output,
                                    &mut line_str,
                                    &mut comments,
                                    current_indent,
                                );
                                line_str.push('\n');
                                // Apply indentation if we have any
                                if current_indent > 0 {
                                    let indent_str = self.make_indent_(current_indent as usize);
                                    line_str.push_str(&indent_str);
                                }
                            }
                            FormatElement::LineGap => {
                                self.line_(
                                    &mut output,
                                    &mut line_str,
                                    &mut comments,
                                    current_indent,
                                );
                                line_str.push_str("\n\n");
                                // Apply indentation if we have any
                                if current_indent > 0 {
                                    let indent_str = self.make_indent_(current_indent as usize);
                                    line_str.push_str(&indent_str);
                                }
                            }
                            _ => {}
                        }
                    }
                    line_str.push_str(&token.to_string(&self.dialect));
                }
                FormatElement::HardBreak => {
                    format_vec.push(element.clone());
                    is_after_newline = true;
                }
                _ => {
                    format_vec.push(element.clone());
                }
            }
        }

        self.line_(&mut output, &mut line_str, &mut comments, current_indent);
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
                FormatElement::Indent => indent_change += 1,
                FormatElement::Dedent => indent_change -= 1,
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
        (indent_change, element)
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

        let text = token.to_string(&self.dialect);
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
            if let FormatElement::Group(group) = element {
                if group.is_empty() {
                    continue;
                }
            }

            match element {
                FormatElement::Group(_) | FormatElement::Token(_) => {
                    if !format_vec.is_empty() {
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
                    flat_output.push_str(&token.to_string(&self.dialect));
                    flat_format_list.push(element.clone());
                }
                format_el => {
                    format_vec.push(format_el.clone());
                }
            }
        }
        if !format_vec.is_empty() {
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
            .saturating_sub(indent_change.unsigned_abs());
    }

    fn render_group_with_breaks(
        &mut self,
        elements: &[FormatElement],
        output: &mut String,
        flat_format_list: &mut Vec<FormatElement>,
    ) {
        let mut format_vec: Vec<FormatElement> = Vec::new();

        for element in elements {
            if let FormatElement::Group(group) = element {
                if group.is_empty() {
                    continue;
                }
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

    pub fn format_tokens(
        &self,
        tokens: &[Token],
    ) -> Result<(String, Vec<FormatElement>), FormatterError> {
        let ast_builder = FormatAstBuilder::new(tokens.to_vec(), self.dialect.clone());
        let format_ast = ast_builder.build();
        let mut renderer = FormatRenderer::new(self.dialect.clone(), &self.settings, format_ast);
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
            let indent_value = " ".repeat(indent * 4);
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
            let (actual_output, format_elements) = formatter
                .format_tokens(&tokenizer_result.tokens)
                .expect("Formatting should succeed");

            // Compare with expected (trim trailing whitespace for comparison)
            let expected = test_case.expected.trim();
            let actual = actual_output.trim();

            if expected == actual {
                continue;
            }
            println!("FormatAst for failing test '{}':", test_case.name);
            println!("\n");
            let mut ast_b =
                FormatAstBuilder::new(tokenizer_result.tokens.clone(), formatter.dialect.clone());
            ast_b.format_mode = false;
            let format_ = ast_b.build();
            pprint_format_ast(format_.elements(), 0, 0);
            for token in format_.walk() {
                println!("{token:?}");
            }
            let ast_builder =
                FormatAstBuilder::new(tokenizer_result.tokens.clone(), Dialect::default());
            let format_ast = ast_builder.build();
            pprint_format_ast(format_ast.elements(), 0, 0);
            println!("\n");
            for element in format_elements {
                println!("{element:?}");
            }
            println!("\n");
            let diff_output = create_visual_diff(expected, &actual);
            panic!(
                "Formatter test '{}' failed:\n{}",
                test_case.name, diff_output
            );
        }
    }

    #[test]
    fn test_complex_formatting() {
        run_formatter_tests("test/fixtures/formatter/complex.yml");
    }

    #[test]
    fn test_basic_formatting() {
        run_formatter_tests("test/fixtures/formatter/basic.yml");
    }

    #[test]
    fn test_medium_sql_formatting() {
        run_formatter_tests("test/fixtures/formatter/medium_sql.yml");
    }

    #[test]
    fn test_universal_groups() {
        run_formatter_tests("test/fixtures/formatter/universal_groups.yml");
    }

    #[test]
    fn test_comments_formatting() {
        run_formatter_tests("test/fixtures/formatter/comments.yml");
    }
}

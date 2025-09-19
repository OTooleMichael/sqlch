use crate::dialect::Dialect;
use crate::loose_parser::{
    BetweenClause, CaseClause, LooseAst, LooseAstBuilder, LooseAstElement, ParenType, SqlClause,
    SqlContext,
};
use crate::tokenizer::{CommentType, Token, TokenType};
use serde::Deserialize;
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
                    FormatElement::Group(group) => {
                        self.stack.push(group.elements.iter());
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
pub struct Group {
    pub context: SqlContext,
    pub elements: Vec<FormatElement>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FormatElement {
    Token(Token), // Keep token info for renderer decisions
    Space,
    NoSpace,
    HardBreak, // SQL structural line breaks
    SoftBreak, // Can become space or break depending on line width
    ShortBreak,
    SoftLine, // Can become nothing (flat) or break depending on line width
    LineGap,  // A new line, and a full empty line
    Indent,
    Dedent,
    Group(Group), // Universal grouping - any () creates a group
}

impl FormatElement {
    fn prio(&self) -> u8 {
        match self {
            FormatElement::LineGap => 0,
            FormatElement::HardBreak => 1,
            FormatElement::SoftBreak => 2,
            FormatElement::SoftLine => 3,
            FormatElement::NoSpace => 4,
            FormatElement::Space => 5,
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

    pub fn get_elements(self) -> Vec<FormatElement> {
        self.elements
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

struct FormatAstBuilder;

fn convert_loose_to_format_elements(element: &LooseAstElement) -> FormatElement {
    match element {
        LooseAstElement::Token(token) => FormatElement::Token(token.clone()),
        LooseAstElement::Group(loose_group) => {
            let elements: Vec<FormatElement> = loose_group
                .elements
                .iter()
                .map(convert_loose_to_format_elements)
                .collect();
            let converted_group = Group {
                context: loose_group.context.clone(),
                elements,
            };
            FormatElement::Group(converted_group)
        }
    }
}

impl FormatAstBuilder {
    fn new() -> Self {
        Self
    }

    fn inject_format_elements(&self, structural_ast: LooseAst) -> FormatAst {
        let mut result = FormatAst::default();
        let group = convert_loose_to_format_elements(&structural_ast.element.unwrap());
        match group {
            FormatElement::Group(group) => {
                let mut context_stack = vec![group.context.clone()];
                let new_group = self.process_group(&group, &mut context_stack);
                for el in new_group {
                    result.push(el)
                }
                result
            }
            _ => panic!("Not reachable"),
        }
    }

    fn process_token(
        &self,
        token: &Token,
        next_token: Option<&FormatElement>,
        context_stack: &[SqlContext],
    ) -> Vec<FormatElement> {
        let mut result = Vec::new();

        match token.token_type {
            TokenType::SemiColon => {
                return vec![
                    FormatElement::HardBreak,
                    FormatElement::Token(token.clone()),
                    FormatElement::LineGap,
                ]
            }
            TokenType::RightParen => {
                result.push(FormatElement::SoftLine);
                result.push(FormatElement::Dedent);
                result.push(FormatElement::Token(token.clone()));
            }
            TokenType::Colon | TokenType::DoubleColon => {
                result.push(FormatElement::NoSpace);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::NoSpace);
            }
            TokenType::Comma => {
                result.push(FormatElement::NoSpace);
                result.push(FormatElement::Token(token.clone()));
                if let Some(SqlContext::WithClause) = context_stack.last() {
                    result.push(FormatElement::LineGap);
                    return result;
                }
                result.push(FormatElement::SoftBreak);
            }
            TokenType::As => {
                result.push(FormatElement::Space);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::And => {
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Or => {
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Operator => match token.value().as_str() {
                "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=" => {
                    result.push(FormatElement::Space);
                    result.push(FormatElement::Token(token.clone()));
                    result.push(FormatElement::Space);
                }
                _ => {
                    result.push(FormatElement::SoftBreak);
                    result.push(FormatElement::Token(token.clone()));
                    result.push(FormatElement::Space);
                }
            },
            TokenType::Star => {
                if let Some(FormatElement::Token(next)) = next_token {
                    if matches!(next.token_type, TokenType::RightParen) {
                        result.push(FormatElement::Token(token.clone()));
                        return result;
                    }
                }
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Dot => {
                result.push(FormatElement::Token(token.clone()));
            }
            TokenType::Identifier => {
                result.push(FormatElement::Token(token.clone()));

                match next_token {
                    Some(FormatElement::Token(next)) => {
                        if matches!(next.token_type, TokenType::Comma | TokenType::Dot) {
                            return result;
                        }
                    }
                    Some(FormatElement::Group(group)) => {
                        if matches!(group.context, SqlContext::Parens(_)) {
                            return result;
                        }
                    }
                    _ => {}
                }
                result.push(FormatElement::Space);
            }
            TokenType::Number | TokenType::StringLiteral => {
                result.push(FormatElement::Token(token.clone()));
                if let Some(FormatElement::Token(next)) = next_token {
                    if matches!(next.token_type, TokenType::Comma | TokenType::Dot) {
                        return result;
                    }
                }
                result.push(FormatElement::Space);
            }
            TokenType::In | TokenType::Keyword => {
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Over => {
                result.push(FormatElement::Space);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Varchar | TokenType::Decimal => {
                result.push(FormatElement::Token(token.clone()));
            }
            TokenType::PartitionBy => {
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::OrderBy => {
                // OrderBy within window functions should also create a line break
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Case => {
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::When => {
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Then => {
                result.push(FormatElement::Space);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Else => {
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::End => {
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Dedent);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::With => {
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::InnerJoin
            | TokenType::LeftJoin
            | TokenType::RightJoin
            | TokenType::FullJoin
            | TokenType::LeftOuterJoin
            | TokenType::RightOuterJoin
            | TokenType::FullOuterJoin
            | TokenType::CrossJoin
            | TokenType::NaturalJoin
            | TokenType::Join => {
                result.push(FormatElement::SoftBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::LeftParen => {
                result.push(FormatElement::Token(token.clone()));
            }
            TokenType::JinjaIf
            | TokenType::JinjaFor
            | TokenType::JinjaElif
            | TokenType::JinjaElse => {
                result.push(FormatElement::Token(token.clone()));
            }
            TokenType::JinjaEndif | TokenType::JinjaEndfor => {
                result.push(FormatElement::HardBreak);
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::HardBreak);
            }
            TokenType::TemplateVariable | TokenType::TemplateBlock => {
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
            TokenType::Extract => {
                result.push(FormatElement::Token(token.clone()));
                // Don't add space if next element is Parens(Extract)
                match next_token {
                    Some(FormatElement::Group(group)) => {
                        if matches!(group.context, SqlContext::Parens(ParenType::Extract)) {
                            return result;
                        }
                    }
                    _ => {}
                }
                result.push(FormatElement::Space);
            }
            TokenType::EOF => {
                result.push(FormatElement::Token(token.clone()));
            }
            _ => {
                // Default case for remaining token types
                result.push(FormatElement::Token(token.clone()));
                result.push(FormatElement::Space);
            }
        }

        result
    }

    fn process_group_elements(
        &self,
        elements: &[FormatElement],
        skip: usize,
        new_elements: &mut Vec<FormatElement>,
        context_stack: &mut Vec<SqlContext>,
    ) {
        let elements_to_process: Vec<_> = elements.iter().skip(skip).collect();

        for (i, element) in elements_to_process.iter().enumerate() {
            match element {
                FormatElement::Token(token) => {
                    let next_token = elements_to_process.get(i + 1).copied();
                    let mut processed = self.process_token(token, next_token, context_stack);
                    new_elements.append(&mut processed);
                }
                FormatElement::Group(nested_group) => {
                    let mut processed_nested = self.process_group(nested_group, context_stack);
                    new_elements.append(&mut processed_nested);
                }
                _ => {
                    let el = *element;
                    new_elements.push(el.clone());
                }
            }
        }
    }

    fn process_group(
        &self,
        group: &Group,
        context_stack: &mut Vec<SqlContext>,
    ) -> Vec<FormatElement> {
        context_stack.push(group.context.clone());
        let mut results = Vec::new();
        let mut new_elements = Vec::new();
        match &group.context {
            SqlContext::BetweenClause(BetweenClause::Root) => {
                new_elements.push(FormatElement::Indent);
                self.process_group_elements(&group.elements, 0, &mut new_elements, context_stack);
                new_elements.push(FormatElement::Dedent);
                new_elements.push(FormatElement::SoftBreak);
            }
            SqlContext::BetweenClause(BetweenClause::Val1) => {
                if let Some(FormatElement::Token(token)) = group.elements.first() {
                    if token.token_type == TokenType::Between {
                        new_elements.push(FormatElement::Token(token.clone()));
                        new_elements.push(FormatElement::Space);
                        self.process_group_elements(
                            &group.elements,
                            1,
                            &mut new_elements,
                            context_stack,
                        );
                    }
                }
            }
            SqlContext::BetweenClause(_) => {
                self.process_group_elements(&group.elements, 0, &mut new_elements, context_stack);
            }
            SqlContext::SelectBlockClause(SqlClause::SelectFields) => {
                if let Some(FormatElement::Token(token)) = group.elements.first() {
                    if token.token_type == TokenType::Select {
                        new_elements.push(FormatElement::Token(token.clone()));

                        // Check for DISTINCT after SELECT
                        let mut skip_count = 1;
                        if let Some(FormatElement::Token(second_token)) = group.elements.get(1) {
                            if second_token.token_type == TokenType::Keyword
                                && second_token.value().as_str().to_uppercase() == "DISTINCT"
                            {
                                new_elements.push(FormatElement::Space);
                                new_elements.push(FormatElement::Token(second_token.clone()));
                                skip_count = 2;
                            }
                        }

                        new_elements.push(FormatElement::ShortBreak);
                        new_elements.push(FormatElement::Indent);
                        self.process_group_elements(
                            &group.elements,
                            skip_count,
                            &mut new_elements,
                            context_stack,
                        );
                        new_elements.push(FormatElement::Dedent);
                        new_elements.push(FormatElement::SoftBreak);
                    }
                }
            }

            SqlContext::SelectBlockClause(SqlClause::From) => {
                if let Some(FormatElement::Token(token)) = group.elements.first() {
                    if token.token_type == TokenType::From {
                        new_elements.push(FormatElement::Token(token.clone()));
                        new_elements.push(FormatElement::Space);
                        self.process_group_elements(
                            &group.elements,
                            1,
                            &mut new_elements,
                            context_stack,
                        );
                        new_elements.push(FormatElement::Dedent);
                        new_elements.push(FormatElement::SoftBreak);
                    }
                }
            }

            SqlContext::SelectBlockClause(_) => {
                // Handle keyword token first (WHERE, HAVING, etc.)
                if let Some(FormatElement::Token(token)) = group.elements.first() {
                    new_elements.push(FormatElement::Token(token.clone()));
                    new_elements.push(FormatElement::Space);
                    new_elements.push(FormatElement::Indent);
                    new_elements.push(FormatElement::Space);

                    self.process_group_elements(
                        &group.elements,
                        1,
                        &mut new_elements,
                        context_stack,
                    );
                    new_elements.push(FormatElement::Dedent);
                    results.push(FormatElement::HardBreak);
                    results.push(FormatElement::Group(Group {
                        context: group.context.clone(),
                        elements: new_elements,
                    }));
                    return results;
                }
            }

            SqlContext::Parens(_) => {
                // Handle LeftParen token first
                if let Some(FormatElement::Token(token)) = group.elements.first() {
                    if token.token_type == TokenType::LeftParen {
                        new_elements.push(FormatElement::Token(token.clone()));
                        new_elements.push(FormatElement::SoftLine);
                        new_elements.push(FormatElement::Indent);

                        self.process_group_elements(
                            &group.elements,
                            1,
                            &mut new_elements,
                            context_stack,
                        );
                        new_elements.push(FormatElement::Space);
                    }
                }
            }

            SqlContext::JinjaBlock => {
                // Jinja blocks: handle start/end tokens, process body
                if let Some(FormatElement::Token(first_token)) = group.elements.first() {
                    match first_token.token_type {
                        TokenType::JinjaIf
                        | TokenType::JinjaFor
                        | TokenType::JinjaElif
                        | TokenType::JinjaElse => {
                            new_elements.push(FormatElement::HardBreak);
                            new_elements.push(FormatElement::Token(first_token.clone()));
                            new_elements.push(FormatElement::Indent);
                            new_elements.push(FormatElement::HardBreak);

                            self.process_group_elements(
                                &group.elements,
                                1,
                                &mut new_elements,
                                context_stack,
                            );

                            new_elements.push(FormatElement::Dedent);
                        }
                        _ => {
                            self.process_group_elements(
                                &group.elements,
                                0,
                                &mut new_elements,
                                context_stack,
                            );
                        }
                    }
                }
            }
            SqlContext::WithClause => {
                self.process_group_elements(&group.elements, 0, &mut new_elements, context_stack);
                new_elements.push(FormatElement::LineGap);
            }

            SqlContext::CaseStatement(case_clause) => {
                match case_clause {
                    CaseClause::Root => {
                        // Root CASE statement - handle CASE keyword or END
                        if let Some(FormatElement::Token(first_token)) = group.elements.first() {
                            if first_token.token_type == TokenType::Case {
                                new_elements.push(FormatElement::Token(first_token.clone()));
                                new_elements.push(FormatElement::SoftBreak);
                                new_elements.push(FormatElement::Indent);
                                new_elements.push(FormatElement::SoftBreak);
                                self.process_group_elements(
                                    &group.elements,
                                    1,
                                    &mut new_elements,
                                    context_stack,
                                );
                            }
                        }
                    }
                    CaseClause::When => {
                        // WHEN clause
                        if let Some(FormatElement::Token(token)) = group.elements.first() {
                            if token.token_type == TokenType::When {
                                new_elements.push(FormatElement::Token(token.clone()));
                                new_elements.push(FormatElement::Indent);
                                new_elements.push(FormatElement::Space);
                                self.process_group_elements(
                                    &group.elements,
                                    1,
                                    &mut new_elements,
                                    context_stack,
                                );
                                new_elements.push(FormatElement::Dedent);
                                new_elements.push(FormatElement::SoftBreak);
                            }
                        }
                    }
                    CaseClause::Then => {
                        // THEN clause
                        if let Some(FormatElement::Token(token)) = group.elements.first() {
                            if token.token_type == TokenType::Then {
                                new_elements.push(FormatElement::SoftBreak);
                                new_elements.push(FormatElement::Indent);
                                new_elements.push(FormatElement::Token(token.clone()));
                                new_elements.push(FormatElement::Space);

                                self.process_group_elements(
                                    &group.elements,
                                    1,
                                    &mut new_elements,
                                    context_stack,
                                );
                                new_elements.push(FormatElement::Dedent);
                                new_elements.push(FormatElement::SoftBreak);
                                results.push(FormatElement::Indent);
                                results.push(FormatElement::SoftBreak);
                                results.push(FormatElement::Group(Group {
                                    context: group.context.clone(),
                                    elements: new_elements,
                                }));
                                return results;
                            }
                        }
                    }
                    CaseClause::WhenThen => {
                        self.process_group_elements(
                            &group.elements,
                            0,
                            &mut new_elements,
                            context_stack,
                        );
                        new_elements.push(FormatElement::Dedent);
                        new_elements.push(FormatElement::SoftBreak);
                        results.push(FormatElement::Group(Group {
                            context: group.context.clone(),
                            elements: new_elements,
                        }));
                        results.push(FormatElement::SoftBreak);
                        return results;
                    }
                    CaseClause::Else => {
                        // ELSE clause
                        if let Some(FormatElement::Token(token)) = group.elements.first() {
                            if token.token_type == TokenType::Else {
                                new_elements.push(FormatElement::SoftBreak);
                                new_elements.push(FormatElement::Indent);
                                new_elements.push(FormatElement::Token(token.clone()));
                                new_elements.push(FormatElement::Space);

                                self.process_group_elements(
                                    &group.elements,
                                    1,
                                    &mut new_elements,
                                    context_stack,
                                );
                                new_elements.push(FormatElement::Dedent);
                                new_elements.push(FormatElement::SoftBreak);
                            }
                        }
                    }
                }
            }

            SqlContext::BetweenClause(_) => {
                // Simple passthrough for now
                self.process_group_elements(&group.elements, 0, &mut new_elements, context_stack);
            }

            SqlContext::Root => {
                self.process_group_elements(&group.elements, 0, &mut new_elements, context_stack);
            }
            SqlContext::Stmt => {
                self.process_group_elements(&group.elements, 0, &mut new_elements, context_stack);
            }
        }

        context_stack.pop();
        results.push(FormatElement::Group(Group {
            context: group.context.clone(),
            elements: new_elements,
        }));
        results
    }
}

struct FormatRenderer<'a> {
    settings: &'a FormatSettings,
    dialect: Dialect,
    indent_level: usize,
    column: usize,
}

impl<'a> FormatRenderer<'a> {
    fn new(dialect: Dialect, settings: &'a FormatSettings) -> Self {
        Self {
            settings,
            dialect,
            indent_level: 0,
            column: 0,
        }
    }

    fn render(&mut self, ast: FormatAst) -> (String, Vec<FormatElement>) {
        let mut flat_format_list: Vec<FormatElement> = Vec::new();
        let mut temp_output = String::new();
        let elements = ast.get_elements();
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
            if output.is_empty() {
                output.push_str(line_str.trim_start());
            } else {
                output.push_str(line_str);
            }
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
        if output.is_empty() {
            output.push_str(line_str.trim_start());
        } else {
            output.push_str(line_str);
        }
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
            FormatElement::NoSpace => {
                flat_format_list.push(FormatElement::NoSpace);
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
            FormatElement::Group(group) => {
                self.render_group(&group.elements, output, flat_format_list);
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
                if group.elements.is_empty() {
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
                            Some(FormatElement::NoSpace) => {
                                flat_format_list.push(FormatElement::NoSpace);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
            match element {
                FormatElement::Group(nested_group) => {
                    if let Some((nested_flat, more_flat)) =
                        self.try_render_flat(&nested_group.elements)
                    {
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
                if group.elements.is_empty() {
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
                            Some(FormatElement::NoSpace) => {
                                flat_format_list.push(FormatElement::NoSpace);
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
            Some(FormatElement::NoSpace) => {
                flat_format_list.push(FormatElement::NoSpace);
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

pub fn format_tokens(
    settings: &FormatSettings,
    dialect: &Dialect,
    tokens: &[Token],
) -> Result<(String, Vec<FormatElement>), FormatterError> {
    let ast_builder = LooseAstBuilder::new(tokens.to_vec(), dialect.clone());
    let format_ast = FormatAstBuilder::new().inject_format_elements(ast_builder.build());
    let mut renderer = FormatRenderer::new(dialect.clone(), settings);
    Ok(renderer.render(format_ast))
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

    fn pprint_format_ast(element: &FormatElement, indent: usize, item: usize) {
        match element {
            FormatElement::Token(token) => {
                let index = item;
                let indent_value = " ".repeat(indent * 4);
                if let Some(str_) = &token.value {
                    let value = str_.to_owned();
                    let t_ = &token.token_type;
                    println!("{indent_value}[{index}] {value:?} {t_:?}");
                } else {
                    let value = token.value();
                    println!("{indent_value}[{index}] {value:?}");
                };
            }
            FormatElement::Group(group) => {
                let size_ = group.elements.len();
                let ctx = &group.context;
                if size_ == 0 {
                    return;
                }
                let index = item;
                let indent_value = " ".repeat(indent * 4);
                println!("{indent_value}[{index}] - {ctx:?}  G{size_} -");
                for (i, element) in group.elements.iter().enumerate() {
                    let index = i + item;
                    pprint_format_ast(element, indent + 1, index);
                }
            }
            el => {
                let index = item;
                let indent_value = " ".repeat(indent * 4);
                println!("{indent_value}[{index}] {el:?}");
            }
        }
    }
    fn pprint_loose_ast(element: &LooseAstElement, indent: usize, item: usize) {
        match element {
            LooseAstElement::Token(token) => {
                let index = item;
                let indent_value = " ".repeat(indent * 4);
                if let Some(str_) = &token.value {
                    let value = str_.to_owned();
                    let t_ = &token.token_type;
                    println!("{indent_value}[{index}] {value:?} {t_:?}");
                } else {
                    let value = token.value();
                    println!("{indent_value}[{index}] {value:?}");
                };
            }
            LooseAstElement::Group(group) => {
                let size_ = group.elements.len();
                let ctx = &group.context;
                if size_ == 0 {
                    return;
                }
                for (i, element) in group.elements.iter().enumerate() {
                    let index = i + item;
                    let indent_value = " ".repeat(indent * 4);
                    println!("{indent_value}[{index}] - {ctx:?}  G{size_} -");
                    pprint_loose_ast(&element, indent + 1, index);
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
        run_formatter_tests_(test_file_path);
    }

    fn run_formatter_tests_(test_file_path: &str) {
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

            let dialect = Dialect::default();
            let (actual_output, format_elements) =
                format_tokens(&format_settings, &dialect, &tokenizer_result.tokens.clone())
                    .expect("Formatting should succeed");

            let expected = test_case.expected.trim();
            let actual = &actual_output;

            if expected == actual {
                continue;
            }

            // Compare old vs new FormatASTs directly
            println!("\n");
            let ast_builder =
                LooseAstBuilder::new(tokenizer_result.tokens.clone(), Dialect::default());
            let loose_ast = ast_builder.build();
            pprint_loose_ast(loose_ast.element.as_ref().unwrap(), 0, 0);
            let format_ast = FormatAstBuilder::new().inject_format_elements(loose_ast);
            pprint_format_ast(&format_ast.get_elements()[0], 0, 0);
            println!("FormatAst for failing test '{}':", test_case.name);
            println!("\n");
            for element in format_elements {
                println!("{element:?}");
            }
            println!("\n");
            let diff_output = create_visual_diff(expected, actual);
            panic!(
                "Formatter test  '{}' failed:\n{}",
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

    #[test]
    fn test_interesting_formatting() {
        run_formatter_tests("test/fixtures/formatter/interesting.yml");
    }
}

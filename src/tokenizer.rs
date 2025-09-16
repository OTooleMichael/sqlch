use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq)]
pub enum TokenType {
    Keyword,
    Identifier,
    StringLiteral,
    Number,
    Operator,
    Punctuation,
    Comma,
    Dot,
    As,
    On,
    Over,
    LeftParen,
    RightParen,
    LeftBrace,
    RightBrace,
    LeftBracket,
    RightBracket,
    Comment,
    TemplateVariable,
    TemplateBlock,
    // Jinja control blocks
    JinjaIf,
    JinjaElif,
    JinjaElse,
    JinjaEndif,
    JinjaFor,
    JinjaEndfor,
    // SQL pairable constructs
    Select,
    From,
    Case,
    End,
    And,
    Or,
    Star,
    // Additional SQL keywords as first-class tokens
    Where,
    Having,
    When,
    Then,
    Else,
    With,
    WithRecursive,
    Qualify,
    // Keywords that can appear standalone or in multi-word constructs
    Join,
    Union,
    Not,
    Is,
    Current,
    Natural,
    Cross,
    Partition,
    // Internal tokens for multi-word processing (not exposed as standalone)
    Inner,
    Left,
    Right,
    Full,
    Outer,
    Group,
    By,
    Order,
    All,
    Null,
    Exists,
    In,
    Row,
    Limit,
    // Multi-word SQL constructs
    OrderBy,
    GroupBy,
    PartitionBy,
    LeftOuterJoin,
    LeftJoin,
    RightOuterJoin,
    RightJoin,
    FullOuterJoin,
    FullJoin,
    InnerJoin,
    CrossJoin,
    NaturalJoin,
    UnionAll,
    IsNull,
    IsNotNull,
    NotExists,
    NotIn,
    CurrentRow,
    EOF,
}

impl TokenType {
    pub fn default_value(&self) -> Option<&'static str> {
        match self {
            // SQL keywords with fixed values
            TokenType::Select => Some("SELECT"),
            TokenType::From => Some("FROM"),
            TokenType::Where => Some("WHERE"),
            TokenType::Having => Some("HAVING"),
            TokenType::When => Some("WHEN"),
            TokenType::Then => Some("THEN"),
            TokenType::Else => Some("ELSE"),
            TokenType::With => Some("WITH"),
            TokenType::WithRecursive => Some("WITH RECURSIVE"),
            TokenType::Case => Some("CASE"),
            TokenType::End => Some("END"),
            TokenType::Qualify => Some("QUALIFY"),

            TokenType::And => Some("AND"),
            TokenType::Or => Some("OR"),
            TokenType::Star => Some("*"),
            // Multi-word SQL constructs
            TokenType::GroupBy => Some("GROUP BY"),
            TokenType::OrderBy => Some("ORDER BY"),
            TokenType::PartitionBy => Some("PARTITION BY"),
            TokenType::InnerJoin => Some("INNER JOIN"),
            TokenType::LeftJoin => Some("LEFT JOIN"),
            TokenType::RightJoin => Some("RIGHT JOIN"),
            TokenType::FullJoin => Some("FULL JOIN"),
            TokenType::LeftOuterJoin => Some("LEFT OUTER JOIN"),
            TokenType::RightOuterJoin => Some("RIGHT OUTER JOIN"),
            TokenType::FullOuterJoin => Some("FULL OUTER JOIN"),
            TokenType::CrossJoin => Some("CROSS JOIN"),
            TokenType::NaturalJoin => Some("NATURAL JOIN"),
            TokenType::UnionAll => Some("UNION ALL"),
            TokenType::IsNull => Some("IS NULL"),
            TokenType::IsNotNull => Some("IS NOT NULL"),
            TokenType::NotExists => Some("NOT EXISTS"),
            TokenType::NotIn => Some("NOT IN"),
            TokenType::CurrentRow => Some("CURRENT ROW"),

            // Standalone keywords that can appear in multi-word contexts
            TokenType::Join => Some("JOIN"),
            TokenType::Union => Some("UNION"),
            TokenType::Not => Some("NOT"),
            TokenType::Is => Some("IS"),
            TokenType::Current => Some("CURRENT"),
            TokenType::Natural => Some("NATURAL"),
            TokenType::Cross => Some("CROSS"),
            TokenType::Partition => Some("PARTITION"),
            TokenType::All => Some("ALL"),
            TokenType::Null => Some("NULL"),
            TokenType::Exists => Some("EXISTS"),
            TokenType::Row => Some("ROW"),
            TokenType::Limit => Some("LIMIT"),

            // Internal tokens (usually not rendered standalone)
            TokenType::Inner => Some("INNER"),
            TokenType::Left => Some("LEFT"),
            TokenType::Right => Some("RIGHT"),
            TokenType::Full => Some("FULL"),
            TokenType::Outer => Some("OUTER"),
            TokenType::Group => Some("GROUP"),
            TokenType::By => Some("BY"),
            TokenType::Order => Some("ORDER"),
            TokenType::In => Some("IN"),

            // Punctuation with fixed values
            TokenType::Comma => Some(","),
            TokenType::Dot => Some("."),
            TokenType::As => Some("AS"),
            TokenType::On => Some("ON"),
            TokenType::Over => Some("OVER"),
            TokenType::LeftParen => Some("("),
            TokenType::RightParen => Some(")"),
            TokenType::LeftBrace => Some("{"),
            TokenType::RightBrace => Some("}"),
            TokenType::LeftBracket => Some("["),
            TokenType::RightBracket => Some("]"),

            // Variable content tokens - must provide value
            TokenType::Keyword
            | TokenType::Identifier
            | TokenType::StringLiteral
            | TokenType::Number
            | TokenType::Operator
            | TokenType::Punctuation
            | TokenType::Comment
            | TokenType::TemplateVariable
            | TokenType::TemplateBlock
            | TokenType::JinjaIf
            | TokenType::JinjaElif
            | TokenType::JinjaElse
            | TokenType::JinjaEndif
            | TokenType::JinjaFor
            | TokenType::JinjaEndfor => None,

            TokenType::EOF => Some(""),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenizerError {
    UnmatchedToken {
        expected: String,
        found: String,
        position: usize,
    },
    InvalidSyntax {
        message: String,
        position: usize,
    },
    UnterminatedString {
        position: usize,
    },
    UnterminatedComment {
        position: usize,
    },
    UnterminatedTemplate {
        template_type: String,
        position: usize,
    },
}

impl std::fmt::Display for TokenizerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TokenizerError::UnmatchedToken {
                expected,
                found,
                position,
            } => {
                write!(
                    f,
                    "Unmatched token at position {position}: expected {expected}, found {found}"
                )
            }
            TokenizerError::InvalidSyntax { message, position } => {
                write!(f, "Invalid syntax at position {position}: {message}")
            }
            TokenizerError::UnterminatedString { position } => {
                write!(f, "Unterminated string literal at position {position}")
            }
            TokenizerError::UnterminatedComment { position } => {
                write!(f, "Unterminated comment at position {position}")
            }
            TokenizerError::UnterminatedTemplate {
                template_type,
                position,
            } => {
                write!(
                    f,
                    "Unterminated {template_type} template at position {position}"
                )
            }
        }
    }
}

impl std::error::Error for TokenizerError {}
#[derive(Debug, Clone, PartialEq)]
pub struct TokenPair {
    pub token_type: TokenType,
    pub loc: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub value: Option<String>,
    pub start: usize,
    pub pair: Option<TokenPair>,
    pub comments: Vec<Token>,
}

impl Token {
    pub fn new(token_type: TokenType, value: Option<String>, start: usize) -> Self {
        Self {
            token_type,
            value,
            start,
            pair: None,
            comments: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        if let Some(val) = self.token_type.default_value() {
            return val.len();
        }
        let dialect = crate::dialect::Dialect::default();
        self.to_string(&dialect).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_string(&self, dialect: &crate::dialect::Dialect) -> String {
        // Use provided value if available, otherwise use token type's default
        let base_value = match &self.value {
            Some(v) => v.clone(),
            None => match self.token_type.default_value() {
                Some(default) => default.to_string(),
                None => panic!(
                    "Token {:?} requires a value but none was provided",
                    self.token_type
                ),
            },
        };

        match self.token_type {
            TokenType::StringLiteral => format!(
                "{}{}{}",
                dialect.string_char, base_value, dialect.string_char
            ),
            TokenType::Identifier => {
                // Only quote identifiers if they contain special characters or are SQL keywords
                if base_value.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    base_value
                } else {
                    format!(
                        "{}{}{}",
                        dialect.identifier_char, base_value, dialect.identifier_char
                    )
                }
            }
            _ => base_value,
        }
    }

    pub fn value(&self) -> String {
        match &self.value {
            Some(v) => v.clone(),
            None => match self.token_type.default_value() {
                Some(default) => default.to_string(),
                None => panic!(
                    "Token {:?} requires a value but none was provided",
                    self.token_type
                ),
            },
        }
    }
}

#[derive(Debug)]
pub struct TokenizerResult {
    pub tokens: Vec<Token>,
    pub hash: u64,
}

#[derive(Debug)]
pub struct Tokenizer<'a> {
    input: &'a str,
    chars_iter: std::str::CharIndices<'a>,
    current_pos: Option<(usize, char)>,
    keyword_buf: Vec<Token>,
    comment_buf: Vec<Token>,
    keywords: HashMap<&'a str, bool>,
    pair_stack: Vec<TokenPair>,
    tokens: Vec<Token>,
    hasher: DefaultHasher,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        let mut keywords = HashMap::new();
        // Add SQL keywords
        for kw in &[
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
            "DISTINCT", "TRUE", "FALSE", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS", "ON",
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "UNION", "GROUP", "BY", "ORDER",
            "HAVING", "LIMIT", "OFFSET", "WITH", "CTE", "CASE", "WHEN", "THEN", "ELSE", "END",
            "COPY", "INTO", "VALUES", "SET", "OVER",
        ] {
            keywords.insert(*kw, true);
        }

        let mut chars_iter = input.char_indices();
        let current_pos = chars_iter.next();

        let mut hasher = DefaultHasher::new();
        input.hash(&mut hasher); // Hash the input string, not individual tokens

        Self {
            input,
            chars_iter,
            current_pos,
            keyword_buf: Vec::new(),
            comment_buf: Vec::new(),
            keywords,
            pair_stack: Vec::new(),
            tokens: Vec::new(),
            hasher,
        }
    }
    fn add_token(&mut self, mut token: Token) -> Result<(), TokenizerError> {
        // Attach any buffered comments to this token
        if !self.comment_buf.is_empty() {
            token.comments.append(&mut self.comment_buf);
        }

        let token_i = self.tokens.len();

        match &token.token_type {
            // Opening tokens that need pairing
            TokenType::LeftParen
            | TokenType::LeftBrace
            | TokenType::LeftBracket
            | TokenType::Select
            | TokenType::Case
            | TokenType::JinjaIf
            | TokenType::JinjaFor => {
                let pair = TokenPair {
                    token_type: token.token_type.clone(),
                    loc: token_i,
                };
                self.pair_stack.push(pair);
                self.tokens.push(token);
                Ok(())
            }

            // Transition tokens that close previous and open new blocks
            TokenType::JinjaElif => self.close_and_reopen_chain(&token, "if/elif", token_i),
            TokenType::JinjaElse => self.close_and_reopen_chain(&token, "if/elif", token_i),

            // Closing tokens that complete pairs
            TokenType::RightParen => self.close_pair(&token, TokenType::LeftParen, "(", token_i),
            TokenType::RightBrace => self.close_pair(&token, TokenType::LeftBrace, "{", token_i),
            TokenType::RightBracket => {
                self.close_pair(&token, TokenType::LeftBracket, "[", token_i)
            }
            TokenType::From => {
                self.close_optional_pair(&token, TokenType::Select, "SELECT", token_i)
            }
            TokenType::End => self.close_pair(&token, TokenType::Case, "CASE", token_i),
            TokenType::JinjaEndif => self.close_jinja_chain(&token, token_i),
            TokenType::JinjaEndfor => self.close_pair(&token, TokenType::JinjaFor, "for", token_i),

            // All other tokens are just added
            _ => {
                self.tokens.push(token);
                Ok(())
            }
        }
    }

    fn close_pair(
        &mut self,
        token: &Token,
        expected_type: TokenType,
        expected_desc: &str,
        token_i: usize,
    ) -> Result<(), TokenizerError> {
        if let Some(opening_pair) = self.pair_stack.last() {
            if opening_pair.token_type == expected_type {
                let opening_pair = self.pair_stack.pop().unwrap();
                let open_token = &mut self.tokens[opening_pair.loc];
                let token_with_pair = Token {
                    pair: Some(opening_pair.clone()),
                    ..token.clone()
                };
                open_token.pair = Some(TokenPair {
                    token_type: token.token_type.clone(),
                    loc: token_i,
                });
                self.tokens.push(token_with_pair);
                Ok(())
            } else {
                Err(TokenizerError::UnmatchedToken {
                    expected: expected_desc.to_string(),
                    found: token.value(),
                    position: token.start,
                })
            }
        } else {
            Err(TokenizerError::UnmatchedToken {
                expected: expected_desc.to_string(),
                found: token.value(),
                position: token.start,
            })
        }
    }

    fn close_optional_pair(
        &mut self,
        token: &Token,
        expected_type: TokenType,
        _expected_desc: &str,
        token_i: usize,
    ) -> Result<(), TokenizerError> {
        if let Some(opening_pair) = self.pair_stack.last() {
            if opening_pair.token_type == expected_type {
                let opening_pair = self.pair_stack.pop().unwrap();
                let open_token = &mut self.tokens[opening_pair.loc];
                let token_with_pair = Token {
                    pair: Some(opening_pair.clone()),
                    ..token.clone()
                };
                open_token.pair = Some(TokenPair {
                    token_type: token.token_type.clone(),
                    loc: token_i,
                });
                self.tokens.push(token_with_pair);
                return Ok(());
            }
        }
        // If no matching pair on stack, just add the token normally
        self.tokens.push(token.clone());
        Ok(())
    }

    fn close_and_reopen_chain(
        &mut self,
        token: &Token,
        expected_desc: &str,
        token_i: usize,
    ) -> Result<(), TokenizerError> {
        // Check if there's a valid previous condition to close
        if let Some(opening_pair) = self.pair_stack.last() {
            if matches!(
                opening_pair.token_type,
                TokenType::JinjaIf | TokenType::JinjaElif | TokenType::JinjaElse
            ) {
                // Close the previous condition block
                let opening_pair = self.pair_stack.pop().unwrap();
                let open_token = &mut self.tokens[opening_pair.loc];
                let token_with_pair = Token {
                    pair: Some(opening_pair.clone()),
                    ..token.clone()
                };
                open_token.pair = Some(TokenPair {
                    token_type: token.token_type.clone(),
                    loc: token_i,
                });

                // Now open a new condition block for this elif/else
                let new_pair = TokenPair {
                    token_type: token.token_type.clone(),
                    loc: token_i,
                };
                self.pair_stack.push(new_pair);
                self.tokens.push(token_with_pair);
                Ok(())
            } else {
                Err(TokenizerError::UnmatchedToken {
                    expected: expected_desc.to_string(),
                    found: token.value(),
                    position: token.start,
                })
            }
        } else {
            Err(TokenizerError::UnmatchedToken {
                expected: expected_desc.to_string(),
                found: token.value(),
                position: token.start,
            })
        }
    }

    fn close_jinja_chain(&mut self, token: &Token, token_i: usize) -> Result<(), TokenizerError> {
        // endif should close whatever Jinja condition is currently open (if, elif, or else)
        if let Some(opening_pair) = self.pair_stack.last() {
            if matches!(
                opening_pair.token_type,
                TokenType::JinjaIf | TokenType::JinjaElif | TokenType::JinjaElse
            ) {
                let opening_pair = self.pair_stack.pop().unwrap();
                let open_token = &mut self.tokens[opening_pair.loc];
                let token_with_pair = Token {
                    pair: Some(opening_pair.clone()),
                    ..token.clone()
                };
                open_token.pair = Some(TokenPair {
                    token_type: token.token_type.clone(),
                    loc: token_i,
                });
                self.tokens.push(token_with_pair);
                Ok(())
            } else {
                Err(TokenizerError::UnmatchedToken {
                    expected: "if/elif/else".to_string(),
                    found: token.value(),
                    position: token.start,
                })
            }
        } else {
            Err(TokenizerError::UnmatchedToken {
                expected: "if/elif/else".to_string(),
                found: token.value(),
                position: token.start,
            })
        }
    }

    pub fn tokenize(&mut self) -> Result<TokenizerResult, TokenizerError> {
        while self.current_pos.is_some() {
            match self.current_char() {
                Some('{') if self.peek() == Some('{') => {
                    let token = self.tokenize_template_variable()?;
                    self.add_token(token)?;
                }
                Some('{') if self.peek() == Some('%') => {
                    let token = self.tokenize_template_block()?;
                    self.add_token(token)?;
                }
                Some(c) if c.is_whitespace() => {
                    // Skip whitespace entirely
                    self.advance();
                }
                Some(c) if c.is_alphabetic() || c == '_' => {
                    self.tokenize_identifier_or_keyword()?;
                }
                Some(c) if c.is_ascii_digit() => {
                    let token = self.tokenize_number()?;
                    self.add_token(token)?;
                }
                Some('"') => {
                    let token = self.tokenize_string(TokenType::Identifier)?;
                    self.add_token(token)?;
                }
                Some('\'') => {
                    let token = self.tokenize_string(TokenType::StringLiteral)?;
                    self.add_token(token)?;
                }
                Some('-') if self.peek() == Some('-') => {
                    let token = self.tokenize_comment()?;
                    self.add_token(token)?;
                }
                Some('/') if self.peek() == Some('*') => {
                    let token = self.tokenize_multiline_comment()?;
                    self.add_token(token)?;
                }
                Some(':') if self.peek() == Some(':') => {
                    let token = self.tokenize_operator()?;
                    self.add_token(token)?;
                }
                Some(':') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::Punctuation,
                        value: Some(":".to_string()),
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some(c) if "+-*/=<>!@$".contains(c) => {
                    let token = self.tokenize_operator()?;
                    self.add_token(token)?;
                }
                Some('(') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::LeftParen,
                        value: None,
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some(')') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::RightParen,
                        value: None,
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some('{') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::LeftBrace,
                        value: None,
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some('}') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::RightBrace,
                        value: None,
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some('[') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::LeftBracket,
                        value: None,
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some(']') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::RightBracket,
                        value: None,
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some(',') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::Comma,
                        value: None,
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some('.') => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::Dot,
                        value: None,
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                Some(c) if ";".contains(c) => {
                    let start = self.current_pos.map(|(b, _)| b).unwrap();
                    let ch = c.to_string();
                    self.advance();
                    let _end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
                    self.add_token(Token {
                        token_type: TokenType::Punctuation,
                        value: Some(ch),
                        start,
                        pair: None,
                        comments: Vec::new(),
                    })?;
                }
                _ => {
                    // Unknown token, skip or error
                    self.advance();
                }
            }
        }

        // Flush any remaining tokens in the buffer
        self.flush_keyword_buffer()?;

        let eof_start = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        self.add_token(Token {
            token_type: TokenType::EOF,
            value: None,
            start: eof_start,
            pair: None,
            comments: Vec::new(),
        })?;
        let final_hash = self.hasher.finish();
        Ok(TokenizerResult {
            tokens: self.tokens.clone(),
            hash: final_hash,
        })
    }

    fn current_char(&self) -> Option<char> {
        self.current_pos.map(|(_, c)| c)
    }

    fn peek(&self) -> Option<char> {
        self.chars_iter.clone().next().map(|(_, c)| c)
    }

    fn advance(&mut self) {
        self.current_pos = self.chars_iter.next();
    }

    fn tokenize_identifier_or_keyword(&mut self) -> Result<(), TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        while self
            .current_char()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            self.advance();
        }
        let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        let ident = &self.input[start..end];

        // Check for special pairable tokens first
        let ident_upper = ident.to_uppercase();
        let token_type = match ident_upper.as_str() {
            "SELECT" => TokenType::Select,
            "FROM" => TokenType::From,
            "AND" => TokenType::And,
            "OR" => TokenType::Or,
            "CASE" => TokenType::Case,
            "END" => TokenType::End,
            "WHERE" => TokenType::Where,
            "HAVING" => TokenType::Having,
            "QUALIFY" => TokenType::Qualify,
            "WHEN" => TokenType::When,
            "THEN" => TokenType::Then,
            "ELSE" => TokenType::Else,
            "WITH" => TokenType::With,
            "AS" => TokenType::As,
            "OVER" => TokenType::Over,
            // Tokens that can be standalone or part of multi-word constructs
            "JOIN" => TokenType::Join,
            "UNION" => TokenType::Union,
            "NOT" => TokenType::Not,
            "IS" => TokenType::Is,
            "CURRENT" => TokenType::Current,
            "NATURAL" => TokenType::Natural,
            "CROSS" => TokenType::Cross,
            "PARTITION" => TokenType::Partition,
            // Internal tokens for multi-word processing
            "INNER" => TokenType::Inner,
            "LEFT" => TokenType::Left,
            "RIGHT" => TokenType::Right,
            "FULL" => TokenType::Full,
            "OUTER" => TokenType::Outer,
            "GROUP" => TokenType::Group,
            "BY" => TokenType::By,
            "ORDER" => TokenType::Order,
            "ALL" => TokenType::All,
            "NULL" => TokenType::Null,
            "EXISTS" => TokenType::Exists,
            "IN" => TokenType::In,
            "ROW" => TokenType::Row,
            "LIMIT" => TokenType::Limit,
            _ if self.keywords.contains_key(ident_upper.as_str()) => TokenType::Keyword,
            _ => TokenType::Identifier,
        };

        let token = Token {
            token_type: token_type.clone(),
            value: if token_type.default_value().is_some() {
                None
            } else {
                Some(ident.to_string())
            },
            start,
            pair: None,
            comments: Vec::new(),
        };

        // Handle multi-word keyword combinations
        self.handle_multi_word_token(token)
    }

    fn handle_multi_word_token(&mut self, token: Token) -> Result<(), TokenizerError> {
        match token.token_type {
            // Keywords that might start multi-word combinations - buffer them
            TokenType::Inner
            | TokenType::Left
            | TokenType::Right
            | TokenType::Full
            | TokenType::Group
            | TokenType::Order
            | TokenType::Partition
            | TokenType::Union
            | TokenType::Current
            | TokenType::Natural
            | TokenType::Cross => {
                self.flush_keyword_buffer()?;
                self.keyword_buf.push(token);
                Ok(())
            }

            // Special handling for NOT and IS which can form complex combinations
            TokenType::Not => {
                // NOT can combine with IS to form IS NOT, or standalone for NOT EXISTS/NOT IN
                if let Some(prev_token) = self.keyword_buf.last() {
                    if prev_token.token_type == TokenType::Is {
                        // Combine IS + NOT
                        let mut is_token = self.keyword_buf.pop().unwrap();
                        is_token.value = Some(format!("{} {}", is_token.value(), token.value()));
                        self.keyword_buf.push(is_token);
                        Ok(())
                    } else {
                        self.flush_keyword_buffer()?;
                        self.keyword_buf.push(token);
                        Ok(())
                    }
                } else {
                    self.keyword_buf.push(token);
                    Ok(())
                }
            }

            TokenType::Is => {
                self.flush_keyword_buffer()?;
                self.keyword_buf.push(token);
                Ok(())
            }

            // OUTER can follow LEFT/RIGHT/FULL
            TokenType::Outer => {
                if let Some(mut prev_token) = self.keyword_buf.pop() {
                    match prev_token.token_type {
                        TokenType::Left | TokenType::Right | TokenType::Full => {
                            // Combine with OUTER and put back in buffer
                            prev_token.value =
                                Some(format!("{} {}", prev_token.value(), token.value()));
                            self.keyword_buf.push(prev_token);
                            Ok(())
                        }
                        _ => {
                            // Not a valid OUTER combination
                            self.add_token(prev_token)?;
                            self.add_token(token)?;
                            Ok(())
                        }
                    }
                } else {
                    // No previous token, OUTER standalone
                    self.add_token(token)?;
                    Ok(())
                }
            }

            // JOIN completes join combinations
            TokenType::Join => {
                if let Some(modifier_token) = self.keyword_buf.pop() {
                    let join_type = match modifier_token.token_type {
                        TokenType::Inner => TokenType::InnerJoin,
                        TokenType::Left => {
                            if modifier_token.value().contains("OUTER") {
                                TokenType::LeftOuterJoin
                            } else {
                                TokenType::LeftJoin
                            }
                        }
                        TokenType::Right => {
                            if modifier_token.value().contains("OUTER") {
                                TokenType::RightOuterJoin
                            } else {
                                TokenType::RightJoin
                            }
                        }
                        TokenType::Full => {
                            if modifier_token.value().contains("OUTER") {
                                TokenType::FullOuterJoin
                            } else {
                                TokenType::FullJoin
                            }
                        }
                        TokenType::Cross => TokenType::CrossJoin,
                        TokenType::Natural => TokenType::NaturalJoin,
                        _ => {
                            // Not a valid join combination
                            self.add_token(modifier_token)?;
                            return self.add_token(token);
                        }
                    };

                    self.add_token(Token {
                        token_type: join_type,
                        value: None,
                        start: modifier_token.start,
                        pair: None,
                        comments: Vec::new(),
                    })
                } else {
                    // No modifier, standalone JOIN
                    self.add_token(token)
                }
            }

            // BY completes GROUP BY, ORDER BY, PARTITION BY
            TokenType::By => {
                if let Some(modifier_token) = self.keyword_buf.pop() {
                    let by_type = match modifier_token.token_type {
                        TokenType::Group => TokenType::GroupBy,
                        TokenType::Order => TokenType::OrderBy,
                        TokenType::Partition => TokenType::PartitionBy,
                        _ => {
                            // Not a valid BY combination
                            self.add_token(modifier_token)?;
                            return self.add_token(token);
                        }
                    };

                    self.add_token(Token {
                        token_type: by_type,
                        value: None,
                        start: modifier_token.start,
                        pair: None,
                        comments: Vec::new(),
                    })
                } else {
                    // Standalone BY
                    self.add_token(token)
                }
            }

            // ALL can complete UNION ALL
            TokenType::All => {
                if let Some(modifier_token) = self.keyword_buf.pop() {
                    match modifier_token.token_type {
                        TokenType::Union => self.add_token(Token {
                            token_type: TokenType::UnionAll,
                            value: None,
                            start: modifier_token.start,
                            pair: None,
                            comments: Vec::new(),
                        }),
                        _ => {
                            // Not UNION ALL
                            self.add_token(modifier_token)?;
                            self.add_token(token)
                        }
                    }
                } else {
                    // Standalone ALL
                    self.add_token(token)
                }
            }

            // NULL can complete IS NULL or check for IS NOT NULL pattern
            TokenType::Null => {
                if let Some(modifier_token) = self.keyword_buf.pop() {
                    match modifier_token.token_type {
                        TokenType::Is => self.add_token(Token {
                            token_type: TokenType::IsNull,
                            value: None,
                            start: modifier_token.start,
                            pair: None,
                            comments: Vec::new(),
                        }),
                        _ => {
                            // Check if this could be IS NOT NULL (modifier_token could be NOT from IS NOT)
                            if modifier_token.value().contains("IS NOT") {
                                self.add_token(Token {
                                    token_type: TokenType::IsNotNull,
                                    value: None,
                                    start: modifier_token.start,
                                    pair: None,
                                    comments: Vec::new(),
                                })
                            } else {
                                // Not IS NULL related
                                self.add_token(modifier_token)?;
                                self.add_token(token)
                            }
                        }
                    }
                } else {
                    // Standalone NULL
                    self.add_token(token)
                }
            }

            // EXISTS can complete NOT EXISTS
            TokenType::Exists => {
                if let Some(modifier_token) = self.keyword_buf.pop() {
                    match modifier_token.token_type {
                        TokenType::Not => self.add_token(Token {
                            token_type: TokenType::NotExists,
                            value: None,
                            start: modifier_token.start,
                            pair: None,
                            comments: Vec::new(),
                        }),
                        _ => {
                            // Not NOT EXISTS
                            self.add_token(modifier_token)?;
                            self.add_token(token)
                        }
                    }
                } else {
                    // Standalone EXISTS
                    self.add_token(token)
                }
            }

            // IN can complete NOT IN
            TokenType::In => {
                if let Some(modifier_token) = self.keyword_buf.pop() {
                    match modifier_token.token_type {
                        TokenType::Not => self.add_token(Token {
                            token_type: TokenType::NotIn,
                            value: None,
                            start: modifier_token.start,
                            pair: None,
                            comments: Vec::new(),
                        }),
                        _ => {
                            // Not NOT IN
                            self.add_token(modifier_token)?;
                            self.add_token(token)
                        }
                    }
                } else {
                    // Standalone IN
                    self.add_token(token)
                }
            }

            // ROW can complete CURRENT ROW
            TokenType::Row => {
                if let Some(modifier_token) = self.keyword_buf.pop() {
                    match modifier_token.token_type {
                        TokenType::Current => self.add_token(Token {
                            token_type: TokenType::CurrentRow,
                            value: None,
                            start: modifier_token.start,
                            pair: None,
                            comments: Vec::new(),
                        }),
                        _ => {
                            // Not CURRENT ROW
                            self.add_token(modifier_token)?;
                            self.add_token(token)
                        }
                    }
                } else {
                    // Standalone ROW
                    self.add_token(token)
                }
            }

            // For any other token, flush buffer and add this token
            _ => {
                self.flush_keyword_buffer()?;
                self.add_token(token)
            }
        }
    }

    fn flush_keyword_buffer(&mut self) -> Result<(), TokenizerError> {
        while let Some(token) = self.keyword_buf.pop() {
            self.add_token(token)?;
        }
        Ok(())
    }

    fn tokenize_number(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        while self
            .current_char()
            .is_some_and(|c| c.is_ascii_digit() || c == '.')
        {
            self.advance();
        }
        let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        Ok(Token::new(
            TokenType::Number,
            Some(self.input[start..end].to_string()),
            start,
        ))
    }

    fn tokenize_string(&mut self, token_type: TokenType) -> Result<Token, TokenizerError> {
        let quote = match self.current_char() {
            Some(q) => q,
            None => {
                return Err(TokenizerError::UnterminatedString {
                    position: self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len()),
                })
            }
        };
        self.advance(); // consume opening quote
        let start = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());

        while let Some(c) = self.current_char() {
            if c == quote {
                let end = self.current_pos.map(|(b, _)| b).unwrap();
                self.advance(); // consume closing quote
                return Ok(Token::new(
                    token_type,
                    Some(self.input[start..end].to_string()),
                    start,
                ));
            }
            self.advance();
        }

        // If we reach here, we didn't find the closing quote
        Err(TokenizerError::UnterminatedString { position: start })
    }

    fn tokenize_operator(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        while self
            .current_char()
            .is_some_and(|c| "+-*/=<>!@$:".contains(c))
        {
            self.advance();
        }
        let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        let value = self.input[start..end].to_string();
        if value == "*" {
            return Ok(Token::new(TokenType::Star, None, start));
        }
        Ok(Token::new(
            TokenType::Operator,
            Some(self.input[start..end].to_string()),
            start,
        ))
    }

    fn tokenize_comment(&mut self) -> Result<Token, TokenizerError> {
        self.advance(); // consume first -
        self.advance(); // consume second -
        let start = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        while self.current_char().is_some_and(|c| c != '\n') {
            self.advance();
        }
        let end = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());
        Ok(Token::new(
            TokenType::Comment,
            Some(self.input[start..end].to_string()),
            start,
        ))
    }

    fn tokenize_multiline_comment(&mut self) -> Result<Token, TokenizerError> {
        self.advance(); // consume /
        self.advance(); // consume *
        let start = self.current_pos.map(|(b, _)| b).unwrap_or(self.input.len());

        loop {
            if self.current_char() == Some('*') && self.peek() == Some('/') {
                let end = self.current_pos.map(|(b, _)| b).unwrap();
                self.advance(); // consume *
                self.advance(); // consume /
                return Ok(Token::new(
                    TokenType::Comment,
                    Some(self.input[start..end].to_string()),
                    start,
                ));
            }
            if self.current_pos.is_none() {
                return Err(TokenizerError::UnterminatedComment { position: start });
            }
            self.advance();
        }
    }

    fn tokenize_template_variable(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        self.advance(); // consume first {
        self.advance(); // consume second {

        while let Some(c) = self.current_char() {
            if c == '}' && self.peek() == Some('}') {
                let end = self.current_pos.map(|(b, _)| b + 2).unwrap();
                self.advance(); // consume first }
                self.advance(); // consume second }
                return Ok(Token::new(
                    TokenType::TemplateVariable,
                    Some(self.input[start..end].to_string()),
                    start,
                ));
            }
            self.advance();
        }

        // If we reach here, we didn't find the closing }}
        Err(TokenizerError::UnterminatedTemplate {
            template_type: "variable".to_string(),
            position: start,
        })
    }

    fn tokenize_template_block(&mut self) -> Result<Token, TokenizerError> {
        let start = self.current_pos.map(|(b, _)| b).unwrap();
        self.advance(); // consume first {
        self.advance(); // consume %

        // Skip whitespace after {%
        while let Some(c) = self.current_char() {
            if !c.is_whitespace() {
                break;
            }
            self.advance();
        }

        // Extract the content inside the template block for analysis
        let content_start = self.current_pos.map(|(b, _)| b).unwrap_or(start);
        while let Some(c) = self.current_char() {
            if c == '%' && self.peek() == Some('}') {
                let content_end = self.current_pos.map(|(b, _)| b).unwrap();
                let end = content_end + 2;
                self.advance(); // consume %
                self.advance(); // consume }

                let full_content = &self.input[start..end];
                let inner_content = &self.input[content_start..content_end].trim();

                // Determine token type based on Jinja control keywords
                let token_type = match inner_content.to_lowercase().as_str() {
                    s if s.starts_with("if ") => TokenType::JinjaIf,
                    s if s.starts_with("elif ") => TokenType::JinjaElif,
                    "else" => TokenType::JinjaElse,
                    "endif" => TokenType::JinjaEndif,
                    s if s.starts_with("for ") => TokenType::JinjaFor,
                    "endfor" => TokenType::JinjaEndfor,
                    _ => TokenType::TemplateBlock,
                };

                return Ok(Token::new(
                    token_type,
                    Some(full_content.to_string()),
                    start,
                ));
            }
            self.advance();
        }

        // If we reach here, we didn't find the closing %}
        Err(TokenizerError::UnterminatedTemplate {
            template_type: "block".to_string(),
            position: start,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::fs;

    #[derive(Debug, Deserialize)]
    struct TestCase {
        name: String,
        sql: String,
        tokens: Vec<TestToken>,
    }

    #[derive(Debug, Deserialize)]
    struct TestToken {
        #[serde(rename = "type")]
        token_type: Option<String>,
        value: Option<String>,
        position: Option<usize>,
    }

    #[derive(Debug, Deserialize)]
    struct TestFile {
        file: Vec<TestCase>,
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
            let result = tokenizer
                .tokenize()
                .expect("Tokenization should succeed in tests");
            let tokens = &result.tokens;

            // Filter out EOF tokens for comparison (unless explicitly expected)
            let actual_tokens: Vec<&Token> = tokens
                .iter()
                .filter(|t| t.token_type != TokenType::EOF)
                .collect();

            // Check each expected token
            for (i, expected) in test_case.tokens.iter().enumerate() {
                if i >= actual_tokens.len() {
                    panic!(
                        "Test '{}' failed: Expected {} tokens, got {}",
                        test_case.name,
                        test_case.tokens.len(),
                        actual_tokens.len()
                    );
                }

                let actual = &actual_tokens[i];

                // Check token type if specified
                if let Some(ref expected_type) = expected.token_type {
                    let actual_type = match actual.token_type {
                        TokenType::Keyword => "Keyword",
                        TokenType::Or => "OR",
                        TokenType::And => "AND",
                        TokenType::Star => "*",
                        TokenType::Identifier => "Identifier",
                        TokenType::StringLiteral => "StringLiteral",
                        TokenType::Number => "Number",
                        TokenType::Operator => "Operator",
                        TokenType::Punctuation => "Punctuation",
                        TokenType::Comma => "Comma",
                        TokenType::Dot => "Dot",
                        TokenType::LeftParen => "LeftParen",
                        TokenType::RightParen => "RightParen",
                        TokenType::LeftBrace => "LeftBrace",
                        TokenType::RightBrace => "RightBrace",
                        TokenType::LeftBracket => "LeftBracket",
                        TokenType::RightBracket => "RightBracket",
                        TokenType::Comment => "Comment",
                        TokenType::TemplateVariable => "TemplateVariable",
                        TokenType::TemplateBlock => "TemplateBlock",
                        TokenType::JinjaIf => "JinjaIf",
                        TokenType::JinjaElif => "JinjaElif",
                        TokenType::JinjaElse => "JinjaElse",
                        TokenType::JinjaEndif => "JinjaEndif",
                        TokenType::JinjaFor => "JinjaFor",
                        TokenType::JinjaEndfor => "JinjaEndfor",
                        TokenType::Select => "Select",
                        TokenType::From => "From",
                        TokenType::Case => "Case",
                        TokenType::End => "End",
                        TokenType::Inner => "Inner",
                        TokenType::Left => "Left",
                        TokenType::Right => "Right",
                        TokenType::Full => "Full",
                        TokenType::Join => "Join",
                        TokenType::Where => "Where",
                        TokenType::Having => "Having",
                        TokenType::Group => "Group",
                        TokenType::By => "By",
                        TokenType::Order => "Order",
                        TokenType::Qualify => "Qualify",
                        TokenType::When => "When",
                        TokenType::Then => "Then",
                        TokenType::Else => "Else",
                        TokenType::With => "With",
                        TokenType::WithRecursive => "WithRecursive",
                        TokenType::Union => "Union",
                        TokenType::Not => "Not",
                        TokenType::Is => "Is",
                        TokenType::Natural => "Natural",
                        TokenType::Cross => "Cross",
                        TokenType::Partition => "Partition",
                        TokenType::Outer => "Outer",
                        TokenType::All => "All",
                        TokenType::Null => "Null",
                        TokenType::Exists => "Exists",
                        TokenType::In => "In",
                        TokenType::Current => "Current",
                        TokenType::Row => "Row",
                        TokenType::Limit => "Limit",
                        TokenType::As => "As",
                        TokenType::On => "On",
                        TokenType::Over => "Over",
                        TokenType::OrderBy => "OrderBy",
                        TokenType::GroupBy => "GroupBy",
                        TokenType::PartitionBy => "PartitionBy",
                        TokenType::LeftOuterJoin => "LeftOuterJoin",
                        TokenType::LeftJoin => "LeftJoin",
                        TokenType::RightOuterJoin => "RightOuterJoin",
                        TokenType::RightJoin => "RightJoin",
                        TokenType::FullOuterJoin => "FullOuterJoin",
                        TokenType::FullJoin => "FullJoin",
                        TokenType::InnerJoin => "InnerJoin",
                        TokenType::CrossJoin => "CrossJoin",
                        TokenType::NaturalJoin => "NaturalJoin",
                        TokenType::UnionAll => "UnionAll",
                        TokenType::IsNull => "IsNull",
                        TokenType::IsNotNull => "IsNotNull",
                        TokenType::NotExists => "NotExists",
                        TokenType::NotIn => "NotIn",
                        TokenType::CurrentRow => "CurrentRow",
                        TokenType::EOF => "EOF",
                    };

                    if expected_type != actual_type {
                        panic!(
                            "Test '{}' failed at token {}: Expected type '{}', got '{}'",
                            test_case.name, i, expected_type, actual_type
                        );
                    }
                }

                // Check value if specified
                if let Some(ref expected_value) = expected.value {
                    if expected_value != &actual.value() {
                        panic!(
                            "Test '{}' failed at token {}: Expected value '{}', got '{}'",
                            test_case.name,
                            i,
                            expected_value,
                            actual.value()
                        );
                    }
                }

                // Check position if specified
                if let Some(expected_pos) = expected.position {
                    if expected_pos != actual.start {
                        panic!(
                            "Test '{}' failed at token {}: Expected position {}, got {}",
                            test_case.name, i, expected_pos, actual.start
                        );
                    }
                }
            }

            println!("  ✓ Passed");
        }
    }

    #[test]
    fn test_basic_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/basic.yml");
    }

    #[test]
    fn test_ansi_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/ansi.yml");
    }

    #[test]
    fn test_jinja_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/jinja.yml");
    }

    #[test]
    fn test_snowflake_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/snowflake.yml");
    }

    #[test]
    fn test_joins_tokenization() {
        run_yaml_tests("test/fixtures/tokenizer/joins.yml");
    }

    #[test]
    fn test_jinja_chain_pairing() {
        // Test 1: Simple if/endif
        let mut tokenizer = Tokenizer::new("{% if condition %}content{% endif %}");
        let result = tokenizer.tokenize().expect("Should tokenize successfully");
        let tokens = &result.tokens;

        // Find the if and endif tokens
        let if_token = tokens
            .iter()
            .find(|t| t.token_type == TokenType::JinjaIf)
            .unwrap();
        let endif_token = tokens
            .iter()
            .find(|t| t.token_type == TokenType::JinjaEndif)
            .unwrap();

        // Verify they are paired
        assert!(if_token.pair.is_some(), "JinjaIf should have a pair");
        assert!(endif_token.pair.is_some(), "JinjaEndif should have a pair");

        // Test 2: if/else/endif chain
        let mut tokenizer = Tokenizer::new("{% if condition %}true{% else %}false{% endif %}");
        let result = tokenizer.tokenize().expect("Should tokenize successfully");
        let tokens = &result.tokens;

        let if_token = tokens
            .iter()
            .find(|t| t.token_type == TokenType::JinjaIf)
            .unwrap();
        let else_token = tokens
            .iter()
            .find(|t| t.token_type == TokenType::JinjaElse)
            .unwrap();
        let endif_token = tokens
            .iter()
            .find(|t| t.token_type == TokenType::JinjaEndif)
            .unwrap();

        // Verify the chain: if pairs with else, else pairs with endif
        assert!(if_token.pair.is_some(), "JinjaIf should have a pair");
        assert!(else_token.pair.is_some(), "JinjaElse should have a pair");
        assert!(endif_token.pair.is_some(), "JinjaEndif should have a pair");

        println!("Chain pairing test passed!");
    }
}

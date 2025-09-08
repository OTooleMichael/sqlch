# SQL Formatter & Linter

A fast, extensible SQL formatter and linter written in Rust, inspired by [sqlfluff](https://github.com/sqlfluff/sqlfluff). Supports multiple SQL dialects, Jinja/DBT templating, and provides both CLI and Language Server Protocol (LSP) interfaces.

## Features

- **Multi-dialect Support**: PostgreSQL, MySQL, SQLite, SQL Server, and more
- **Jinja/DBT Templating**: Handle templated SQL with variable substitution
- **Dual AST/CST Parsing**: Maintains both abstract and concrete syntax trees for flexible processing
- **Extensible Rule System**: Pluggable linting rules for code quality enforcement
- **CLI Interface**: Command-line tools for formatting and linting
- **LSP Integration**: Editor integration with diagnostics, hover, and completion
- **Fast & Memory Efficient**: Written in Rust for high performance

## Architecture

The project is structured around several key components:

- **Tokenizer**: Lexical analysis of SQL source code
- **Parser**: Generates dual AST/CST representations
- **Rules Engine**: Applies linting and formatting rules
- **Dialects**: Dialect-specific configurations and behavior
- **Templating**: Jinja/DBT variable extraction and replacement
- **CLI**: Command-line interface
- **LSP**: Language Server Protocol implementation

## Installation

### Prerequisites

- Rust 1.70+ (2021 edition)
- Cargo package manager

### Building from Source

```bash
git clone <repository-url>
cd sql_formatter
cargo build --release
```

## Usage

### CLI Commands

#### Format SQL Files

```bash
cargo run -- format input.sql --output formatted.sql --dialect postgresql
```

#### Lint SQL Files

```bash
cargo run -- lint input.sql --dialect mysql
```

#### Start LSP Server

```bash
cargo run -- lsp
```

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
sql_formatter = { path = "path/to/sql_formatter" }
```

```rust
use sql_formatter::{parser::Parser, rules::RuleEngine};

let sql = "SELECT * FROM users WHERE id = 1";
let mut parser = Parser::new(sql);
match parser.parse() {
    Ok((ast, cst)) => {
        // Process AST/CST
        println!("{:?}", ast);
    }
    Err(e) => eprintln!("Parse error: {}", e),
}
```

## Supported SQL Dialects

- PostgreSQL
- MySQL
- SQLite
- SQL Server
- Generic SQL (fallback)

## Templating Support

The formatter supports Jinja-style and DBT-style templating:

```sql
-- Jinja-style
SELECT * FROM {{ table_name }} WHERE id = {{ user_id }}

-- DBT-style
SELECT * FROM {{ ref('users') }} WHERE status = '{{ var('active_status') }}'
```

Variables can be extracted and replaced programmatically:

```rust
use sql_formatter::templating::Templater;

let templater = Templater::new();
let sql = "SELECT * FROM {{ table }}";
let variables = templater.extract_variables(sql);
// variables = ["{{ table }}"]

let mut replacements = std::collections::HashMap::new();
replacements.insert("{{ table }}".to_string(), "users".to_string());
let processed = templater.replace_variables(sql, &replacements);
// processed = "SELECT * FROM users"
```

## Development

### Running Tests

```bash
cargo test
```

### Running Lints

```bash
cargo clippy
```

### Formatting Code

```bash
cargo fmt
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/your-username/sql_formatter.git`
3. Create a feature branch: `git checkout -b feature/your-feature`
4. Make your changes and add tests
5. Run the test suite: `cargo test`
6. Format your code: `cargo fmt`
7. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by [sqlfluff](https://github.com/sqlfluff/sqlfluff)
- Built with [Rust](https://www.rust-lang.org/)
- LSP implementation using [tower-lsp](https://github.com/ebanner/pyright/tree/main/packages/tower-lsp)
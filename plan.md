# SQL Formatter & Linter - Development Plan

## Current Status ✅

### Completed Components

- **✅ Project Initialization**
  - Rust project scaffold created with Cargo
  - Basic dependencies configured (clap, nom, tower-lsp, regex, etc.)
  - sqlfluff reference repository cloned and gitignored (major point of reference for rules, architecture, and implementation)

- **✅ Core Architecture**
  - **Tokenizer**: Basic implementation with support for keywords, identifiers, literals, operators, punctuation, whitespace, and comments
  - **Parser**: Dual AST/CST generation for SELECT statements
  - **AST/CST**: Abstract and Concrete Syntax Tree definitions
  - **Rules Engine**: Framework for pluggable linting rules with violation detection
  - **Dialects**: Configuration structure for multiple SQL dialects (PostgreSQL, MySQL, SQLite, SQL Server)
  - **CLI**: Command-line interface with format, lint, and LSP commands
  - **LSP Server**: Basic Language Server Protocol implementation with hover and completion stubs
  - **Templating**: Jinja/DBT-style variable extraction and replacement

- **✅ Basic Functionality**
  - Compiles successfully with no errors
  - Can parse basic SELECT statements
  - CLI commands functional
  - LSP server starts and responds to basic requests

## Roadmap 🚀

### Phase 1: Parser Enhancement (High Priority)

- [ ] **Expand SQL Grammar Support**
  - [ ] INSERT, UPDATE, DELETE statements
  - [ ] JOIN clauses (INNER, LEFT, RIGHT, FULL)
  - [ ] WHERE conditions with complex expressions
  - [ ] GROUP BY and HAVING clauses
  - [ ] ORDER BY and LIMIT clauses
  - [ ] Subqueries
  - [ ] CTEs (Common Table Expressions)
  - [ ] Window functions

- [ ] **Parser Improvements**
  - [ ] Better error handling and recovery
  - [ ] Position tracking for accurate error reporting
  - [ ] Support for comments in various positions
  - [ ] Handling of SQL string concatenation

### Phase 2: Rule System Expansion (High Priority)

- [ ] **Layout Rules** (inspired by sqlfluff LT rules)
  - [ ] LT01: Trailing whitespace
  - [ ] LT02: Indentation consistency
  - [ ] LT03: Line length limits
  - [ ] LT04: Comma placement
  - [ ] LT05: Operator spacing

- [ ] **Capitalization Rules** (inspired by sqlfluff CP rules)
  - [ ] CP01: Keyword capitalization
  - [ ] CP02: Function name capitalization
  - [ ] CP03: Object name capitalization

- [ ] **Structure Rules** (inspired by sqlfluff ST rules)
  - [ ] ST01: Semicolon placement
  - [ ] ST02: Single table alias per query
  - [ ] ST03: CTE structure
  - [ ] ST04: Column order consistency

- [ ] **Convention Rules** (inspired by sqlfluff CV rules)
  - [ ] CV01: Consistent column aliasing
  - [ ] CV02: Consistent table aliasing
  - [ ] CV03: Consistent use of quotes

### Phase 3: Dialect Integration (Medium Priority)

- [ ] **Dialect-Specific Tokenization**
  - [ ] PostgreSQL-specific keywords and functions
  - [ ] MySQL-specific syntax elements
  - [ ] SQLite-specific features
  - [ ] SQL Server T-SQL extensions

- [ ] **Dialect-Specific Rules**
  - [ ] PostgreSQL-specific best practices
  - [ ] MySQL-specific optimizations
  - [ ] SQLite-specific constraints

- [ ] **Dialect-Aware Parsing**
  - [ ] Conditional parsing based on dialect
  - [ ] Dialect-specific AST nodes

### Phase 4: Formatting Engine (Medium Priority)

- [ ] **Pretty Printing**
  - [ ] Configurable indentation (spaces vs tabs)
  - [ ] Line wrapping strategies
  - [ ] Consistent formatting rules

- [ ] **Format Configuration**
  - [ ] YAML/JSON configuration files
  - [ ] Per-project settings
  - [ ] Editor integration settings

- [ ] **Output Formats**
  - [ ] Standard SQL formatting
  - [ ] Compact formatting
  - [ ] Custom formatting rules

### Phase 5: LSP Enhancement (Medium Priority)

- [ ] **Diagnostics**
  - [ ] Real-time linting feedback
  - [ ] Syntax error highlighting
  - [ ] Rule violation reporting

- [ ] **Code Actions**
  - [ ] Quick fixes for rule violations
  - [ ] Auto-formatting on save
  - [ ] Refactoring suggestions

- [ ] **Document Symbols**
  - [ ] Table and column navigation
  - [ ] Function and variable references
  - [ ] Outline view support

- [ ] **Hover Information**
  - [ ] Keyword documentation
  - [ ] Function signatures
  - [ ] Variable information

### Phase 6: Advanced Features (Low Priority)

- [ ] **Performance Optimization**
  - [ ] Parallel processing for large files
  - [ ] Incremental parsing
  - [ ] Caching for repeated operations

- [ ] **Integration Features**
  - [ ] Git hooks integration
  - [ ] CI/CD pipeline integration
  - [ ] Database connection validation

- [ ] **Extensibility**
  - [ ] Plugin system for custom rules
  - [ ] Custom dialect support
  - [ ] Third-party integrations

## Testing Strategy

- [ ] **Unit Tests**
  - [ ] Tokenizer tests for various SQL constructs
  - [ ] Parser tests for AST/CST generation
  - [ ] Rule tests for violation detection and fixing

- [ ] **Integration Tests**
  - [ ] End-to-end CLI testing
  - [ ] LSP protocol testing
  - [ ] Multi-file project testing

- [ ] **Benchmarking**
  - [ ] Performance comparisons with sqlfluff
  - [ ] Memory usage optimization
  - [ ] Large file handling

## Success Metrics

- **Functionality**: Parse and format 95% of common SQL patterns
- **Performance**: Faster than sqlfluff for equivalent operations
- **Compatibility**: Drop-in replacement for sqlfluff in common use cases
- **Extensibility**: Easy to add new dialects and rules
- **Reliability**: Comprehensive test coverage with CI/CD

## Timeline

- **Phase 1**: 2-3 weeks (Parser enhancement)
- **Phase 2**: 3-4 weeks (Rule system)
- **Phase 3**: 2-3 weeks (Dialect integration)
- **Phase 4**: 2-3 weeks (Formatting engine)
- **Phase 5**: 3-4 weeks (LSP enhancement)
- **Phase 6**: Ongoing (Advanced features)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines and contribution process.

## Related Documents

- [README.md](README.md) - Project overview and usage
- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture documentation
- [RULES.md](RULES.md) - Available rules and their implementation
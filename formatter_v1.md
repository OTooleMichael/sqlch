# SQL Formatter V1: Current Implementation and Lessons Learned

## Overview

This document describes the current state of the SQL formatter implementation, the architectural decisions made, lessons learned, and recommendations for future development.

## Current Architecture

### Format Pipeline
1. **Tokenizer** → Tokens (with first-class `Dot` and `Comma` tokens)
2. **Format Stream Builder** (`tokens_to_format_stream`) → `Vec<FormatElement>`
3. **Wadler Renderer** (`render_format_stream`) → Formatted string

### Format Elements
```rust
enum FormatElement {
    Text(String),
    Space,
    HardBreak,      // Always break line
    SoftBreak,      // Break only if needed
    Indent,         // Increase indent level
    Dedent,         // Decrease indent level  
    Group(Vec<FormatElement>),  // Wadler group for line-fitting decisions
}
```

### Current State
- ✅ Basic SELECT/FROM/WHERE formatting works
- ✅ First-class `Dot` tokens improve spacing around `table.column`
- ✅ Wadler renderer infrastructure in place
- ✅ Simple test cases pass (`indent_spaces_2`)
- ❌ Complex nested indentation fails (`break_list_on_80_plus`)
- ❌ Double indentation issues in IN clauses

## Key Issues Discovered

### 1. **Manual Indent/Dedent vs. Automatic Grouping Conflict**

**Problem:** We're mixing two incompatible approaches:
- **Manual approach:** Explicitly adding `Indent`/`Dedent` elements
- **Wadler approach:** Groups that should automatically handle indentation

**Example of the conflict:**
```rust
// Manual indentation for AND keyword
stream.push(FormatElement::Indent);

// Then IN list adds its own indentation  
stream.push(FormatElement::Indent); 

// Result: Double indentation (6 spaces instead of 4)
```

**Root cause:** We're **fighting against** the Wadler algorithm instead of **using** it properly.

### 2. **Wadler Implementation is Incomplete**

**Current Wadler renderer issues:**
- Groups don't automatically indent when breaking (missing core Wadler behavior)
- No differentiation between structural vs semantic groups
- Line-width decisions work, but indentation logic is manual

**What's missing:**
```rust
// Should happen automatically when group breaks:
fn render_group_broken(&self, elements: &[FormatElement], current_indent: usize + 1) {
    // Auto-indent group contents when breaking
}
```

### 3. **Complex Nesting Cannot Be Manually Balanced**

**The test case that reveals the issue:**
```sql
WHERE active = TRUE
  AND lemon IN (
    1,    -- Should be 4 spaces 
    4,    -- Should be 4 spaces
    ...
  )     -- Should be 2 spaces
```

**Current output:**
```sql
WHERE active = TRUE  
  AND lemon IN (
      1,  -- 6 spaces (wrong)
        4,  -- 8 spaces (wrong)
    )     -- 4 spaces (wrong)
```

**Why manual balancing fails:**
- AND adds `Indent` → level 1
- IN list adds `Indent` → level 2  
- Comma processing adds more context → level 3
- No clear point to `Dedent` back to correct levels

## Lessons Learned

### 1. **Wadler Algorithm Requires Semantic Understanding**

**Insight:** Not all groups should auto-indent. We need:

- **Structural groups:** Lists, field collections (don't auto-indent)
- **Semantic groups:** AND conditions, nested expressions (do auto-indent)

**Proper approach:**
```rust
enum GroupType {
    Structural,  // For line-fitting only
    Semantic,    // For line-fitting + auto-indentation
}

struct Group {
    elements: Vec<FormatElement>,
    group_type: GroupType,
}
```

### 2. **SQL Structure Doesn't Map Cleanly to Pure Wadler**

**SQL formatting requirements:**
- **Hierarchical indentation** (SELECT > fields, WHERE > conditions)
- **Context-sensitive spacing** (before/after operators, punctuation)
- **Domain-specific rules** (JOIN alignment, IN list formatting)

**Pure Wadler limitations:**
- Designed for expression languages (Lisp, ML)
- Assumes uniform indentation rules
- Less suited for statement-based languages with complex precedence

### 3. **First-Class Tokens Are Valuable**

**Success story:** Making `Dot` and `Comma` first-class tokens solved:
- Spacing issues around `table.column` references
- Cleaner tokenization logic
- Better control over punctuation formatting

**Recommendation:** Continue expanding first-class tokens for `(`, `)`, `;`, etc.

## Technical Debt

### Current Workarounds
1. **Complex manual grouping logic** in WHERE clause processing
2. **Inconsistent indentation handling** between different SQL clauses  
3. **Mixing manual and automatic approaches** throughout the codebase

### Code Quality Issues
1. **Duplicate logic** for handling parentheses in different contexts
2. **Hard-coded spacing rules** scattered throughout format stream building
3. **No clear separation** between structural formatting and content formatting

## Recommendations for V2

### 1. **Choose a Clear Architecture**

**Option A: Pure Wadler with Typed Groups**
```rust
enum SqlGroup {
    SelectClause { fields: Vec<FormatElement> },
    WhereCondition { operator: String, operands: Vec<FormatElement> },
    InList { items: Vec<FormatElement> },
}
```

**Option B: Hybrid with Clear Separation**
- Use manual `Indent`/`Dedent` for SQL clause structure
- Use `Group` only for line-fitting decisions within clauses
- Never mix the two approaches in the same context

**Option C: SQL-Specific Pretty Printer**
- Design custom algorithm optimized for SQL's hierarchical structure
- Abandon Wadler altogether in favor of SQL-domain rules

### 2. **Implement Proper Wadler Algorithm**

If continuing with Wadler:
```rust
fn render_group(&self, group: &SqlGroup, current_indent: usize, remaining_width: usize) -> String {
    let flat_result = self.try_flat_render(&group.elements, current_indent);
    
    if flat_result.len() <= remaining_width && group.can_be_flat() {
        flat_result
    } else {
        self.render_group_broken(&group.elements, current_indent + group.indent_delta())
    }
}
```

### 3. **Add Comprehensive Testing**

**Current gap:** Only 2 formatter tests, both basic cases.

**Needed test coverage:**
- Complex nested structures (IN clauses, subqueries)
- Edge cases (empty lists, single items)
- Different indentation styles (tabs vs spaces, different sizes)
- Line width variations (40, 80, 120 characters)
- Mixed SQL constructs (JOINs, CTEs, CASE statements)

### 4. **Separate Concerns Cleanly**

```rust
// Phase 1: Parse SQL structure
struct SqlAst { clauses: Vec<SqlClause> }

// Phase 2: Apply formatting rules  
struct FormatRules { indent_style: IndentStyle, line_width: usize }

// Phase 3: Render to text
struct TextRenderer { rules: FormatRules }
```

## Current Status

**Working features:**
- Basic SELECT/FROM/WHERE formatting
- First-class token handling
- Wadler infrastructure (partial)
- Simple test cases

**Failing features:**
- Complex nested indentation (IN clauses)
- Balanced indent/dedent in WHERE conditions
- Consistent spacing in all contexts

**Test results:** 5/6 tests passing (1 formatter test still failing)

## Conclusion

The V1 implementation successfully demonstrates:
1. **Wadler algorithm feasibility** for SQL formatting
2. **Value of first-class tokens** for better spacing control
3. **Complexity of SQL formatting** compared to expression languages

However, it also reveals fundamental architectural issues that require a V2 redesign to properly handle complex nested structures.

The core insight: **SQL formatting needs either a fully implemented Wadler algorithm with semantic group types, or a custom pretty-printer designed specifically for SQL's hierarchical statement structure.**
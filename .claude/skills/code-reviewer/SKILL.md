---
name: code-reviewer
description: Analyze code in the src folder for bugs, correctness issues, and design problems. Use this whenever the user asks you to review code, analyze code quality, find issues in their src folder, or provide feedback on code structure and patterns. This skill is especially effective for PySpark projects. Trigger on requests like "review this code", "analyze the src folder", "code review please", "find bugs in my code", or similar code analysis requests.
compatibility: Requires Python files to analyze; works with any text-based code
---

# Code Reviewer Skill

## Overview

This skill analyzes code for correctness, design patterns, and potential bugs. It's optimized for PySpark projects but works with any Python code. It produces a structured report with severity levels, specific line references, and actionable suggestions.

## How to Use

When invoked with code files, this skill will:

1. **Scan for Issues** — Look for:
   - Logic errors and edge case handling
   - Incorrect API usage (especially PySpark DataFrame operations)
   - Design pattern violations and maintainability concerns
   - Performance anti-patterns
   - Missing error handling

2. **Categorize by Severity**:
   - 🔴 **Critical**: Runtime failures, data correctness issues, security holes
   - 🟠 **High**: Design flaws, inefficient patterns that cause production problems
   - 🟡 **Medium**: Code quality issues, maintainability concerns
   - 🟢 **Low**: Style suggestions, minor improvements

3. **Provide Actionable Feedback**:
   - Specific file and line references
   - Clear explanation of the issue
   - Concrete suggestion for improvement (with code example if helpful)

## Input Format

The skill accepts code files in any of these forms:
- Single Python file passed as context
- Multiple files from the src folder
- Code snippets in the conversation
- File paths (it will read them if available)

## Output Format

### Report Structure

```
# Code Review Report: [filename or summary]

## Summary
- Total issues found: N
- Critical: X | High: Y | Medium: Z | Low: W
- Overall assessment: [brief overview]

## Issues by Severity

### 🔴 Critical Issues (if any)
**Issue 1: [Title]**
- Location: file.py:LINE
- Problem: [explanation]
- Impact: [why this matters]
- Fix: [specific suggestion with example]

### 🟠 High Issues (if any)
[same format as critical]

### 🟡 Medium Issues (if any)
[same format as critical]

### 🟢 Low Issues (if any)
[same format as critical]

## Patterns & Recommendations
- [Overall observations about code structure]
- [Strengths worth highlighting]
- [Key areas for improvement]

## Next Steps
- Priority actions if critical/high issues exist
- Suggested refactoring approach
```

## Special Handling for PySpark Code

When reviewing PySpark code, pay special attention to:

- **DataFrame Operations**: Check for wide transformations, unnecessary broadcasts, or missing partition optimization
- **Lazy Evaluation**: Verify actions are intentional and not duplicated
- **Join Operations**: Look for cartesian products, incorrect join keys, skewed data handling
- **Serialization**: Check for closure issues, non-serializable objects
- **Caching Strategy**: Identify unnecessary cache() calls or cache with wrong storage level
- **UDF Performance**: Flag Python UDFs that could be built-in Spark functions
- **Streaming Specifics**: Watermarking, trigger modes, checkpoint handling, stateful operations

## Example Interaction

**User**: "Review the streaming code in src/claude_pyspark_proj/streaming/"

**Skill Output**:
```
# Code Review Report: streaming module

## Summary
- Total issues found: 3
- Critical: 1 | High: 1 | Medium: 1 | Low: 0
- Overall assessment: Functional but has critical security and design issues

## Issues by Severity

### 🔴 Critical Issues

**Issue 1: Hardcoded credentials in config**
- Location: get_data_from_event_hubs.py:43-46
- Problem: Connection string with SharedAccessKey is exposed in source code
- Impact: Security vulnerability; anyone with code access has Kafka credentials
- Fix: Move to environment variables or Databricks secrets:
  ```python
  connection_string = dbutils.secrets.get("scope=kafka", "connection_string")
  ```

### 🟠 High Issues
[continues...]
```

## Testing & Iteration

The skill learns from feedback. If reviews consistently miss certain issues or are too verbose, we'll refine the focus and output format.

## Notes

- This skill reads code as written and doesn't execute it
- It focuses on static analysis and pattern recognition
- For runtime issues, consider also running unit tests or profiling
- Context matters: if you have specific coding standards or frameworks, mention them in your request

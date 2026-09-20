# SQL Debugger

This is a Python package managed using uv. Below are some rules for working with it:

## Stack

- uv for package management and running commands
- basedpyright for type checking
- pytest for unit testing
- ruff for linting

## Code Conventions

### Style

- Unused results of non-`None` returning methods should be assigned to `_` instead of ignored
- Comments explain why, not what. Only use comments to explain irregular choices
- Do not use section header comments

### Naming

- Functions start with a verb, its name should complete "When called, the function will..."

## Workflow

- When writing code, do the following:
    - run `uv run pytest`, all must pass after your changes unless we wrote tests first for something
    - run `uv run basedpyright` on files that were modified; fix warnings and errors

# SQL Debugger

This is a Python package managed using uv. Below are some rules for working with it:

## Stack

- uv for package management and running commands
- basedpyright for type checking
- pytest for unit testing
- ruff for linting

## Code Style

- Unused results of non-`None` returning methods should be assigned to `_` instead of ignored

## Workflow

- When writing code, do the following:
    - always run `basedpyright` on modified files via `uv` and fix warnings and errors

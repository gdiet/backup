# Agent Guidelines And Best Practices For This Project

## Project Overview

The goal of this project is a deduplicating backup application, implemented in Go.

## Verification Of Changes

When making changes to the codebase, always verify your changes before finishing and suggesting commits:

Verify that the code compiles. Avoid creating binary files that could accidentally be committed to version control: `go build ./...`

Run `go fmt` and `go vet`.

Run all tests that are not classified as long-running: `go test ./...`

Suggest an English semantic commit message following the Conventional Commits format.

Only commit changes when explicitly asked to do so by the user. Even if you've made changes and suggested a commit message, wait for explicit permission before running `git commit`.

If the user has not explicitly approved the commit message, use git diff and similar to check what will actually be committed and suggest a commit message for approval, but don't commit.

## Dependencies

Suggest required or helpful dependencies to the user, but do not add them to the project without explicit permission. When adding new dependencies:

- `go get <package>`
- `go mod tidy`

## Code Quality

- Follow Go idioms and conventions
- Keep functions focused and testable
- Write self-documenting code with clear variable names
- If possible, avoid complex logic and not obvious code. If unavoidable, add explaining comments

# Contributing

## Before you submit

- Search the open and closed issues and pull requests for the same change, and link the related ones.
- Read the issue you work on, including its comments, because later comments may replace earlier plans.

## Titles

Write issue and pull request titles as natural sentences that say what changes, such as "Reconnect after the server restarts".

## Commits

Use Conventional Commits. `cliff.toml` groups release notes by commit type.

- Use the folder or file containing the change as the commit scope, such as `manager` or `eioc`. Use `deps` for dependency changes and `agents` for agent instruction changes.
- Mark a public API break with both `!` and a `BREAKING CHANGE:` footer, including a dependency bump whose types appear in the public API.

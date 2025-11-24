### Running The Filesystem Directly

    go run src/*.go <mount-point>

### Development Tools

- Use VSCode with the Go extension for code editing and debugging.
- Use the Workspace Default Settings extension https://github.com/dangmai/vscode-workspace-default-settings for consistent VSCode settings that can be modified locally without affecting the repository.
- Use the Git Graph 2 extension https://github.com/hansu/vscode-git-graph for visualizing git history.
- Use the SonarQube for IDE extension https://github.com/SonarSource/sonarlint-vscode.git for code quality and security analysis.

### Packages and Architecture

```mermaid
graph TD
  fuse --> repo
  repo --> meta
  repo --> data
  data --> cache
  data --> store
  fuse[
    ------ fuse --------
    FUSE bindings
    no synchronization
  ]
  repo[
    ------ repo --------
    technical entrypoint
    no synchronization
  ]
  meta[
    ------ meta --------
    metadata management
    bbolt transactions
  ]
  data[
    ------ data --------
    file data access
    synchronized
  ]
  cache[
    ------ cache -------
    temp. data storage
    no synchronization
  ]
  store[
    ------ store -------
    file data storage
    synchronized
  ]
  classDef default font-family:monospace,text-align:left;
```

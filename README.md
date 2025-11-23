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
  main --> repo
  repo --> metadata
  repo --> handles
  handles --> cache
  handles --> store
  main[
    ------ main --------
    FUSE bindings
    no sync
  ]
  repo[
    ---- repository ----
    technical entrypoint
    no sync
  ]
  metadata[
    ----- metadata -----
    metadata management
    bbolt transactions
  ]
  handles[
    ----- handles ------
    file data access
    synchronized
  ]
  cache[
    ------ cache -------
    temp. data storage
    no sync
  ]
  store[
    ------ store -------
    file data storage
    synchronized
  ]
  classDef default font-family:monospace,text-align:left;
```

#### repository

The `repository` implements FUSE operations. It delegates tasks to internal packages and translates results and errors. No synchronization at this level. Implemented methods:

**Tree operations:**

- Mkdir
- Readdir
- Rmdir
- Rename

**Attribute operations:**

- Getattr
- Utimens

**File operations:**

- Create
- Open
- ??? Read
- ??? Write
- ??? Truncate
- ??? Release
- ??? Unlink

**Other operations:**

- NewFS
- ??? Statfs
- Destroy

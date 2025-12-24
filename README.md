### Starting The Filesystem

- Linux: The mount point must be an existing empty directory.
- Windows: The mount point must be either a non-existing entry in an existing directory or a drive letter (e.g., `X:`).

Relative paths are supported for the mount point.

```
go run ./src/. <mount-point>
```

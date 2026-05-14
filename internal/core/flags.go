package core

// FIXME can be private for backup?
type BackupFlags struct {
	CreateDirs   bool // -p, --create-dirs     default false
	TargetExists bool // -t, --target-exists   default false
	Concurrency  uint // -c, --concurrency     default 4
}

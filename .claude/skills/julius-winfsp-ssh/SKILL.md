---
name: julius-winfsp-ssh
description: SSH into a separate physical/VM Windows machine (nicknamed "julius") to run real WinFSP mount tests that neither the primary Windows checkout nor a WSL clone can perform. Use when reaching a genuinely separate remote Windows machine over SSH for this project - distinct from the wsl-windows-sync skill, which covers a single machine's dual Windows/WSL environment.
---

# Working With A Separate Windows Machine Over SSH

Distinct from `wsl-windows-sync` (same machine, dual environment) - this is about reaching a
genuinely separate physical/VM Windows machine (in practice, one nicknamed "julius") over SSH to
run real WinFSP tests that neither the primary checkout nor WSL can perform. Gotchas learned the
hard way across several sessions:

- **SSH-Exec-Sessions (Win32-OpenSSH) tie spawned child processes to a Job Object** - when the SSH
  connection ends, every child process dies with it, even ones started with `start /b`. Workaround:
  launch via Task Scheduler instead (`schtasks /create ... /sc once /st 23:59 /f` then
  `schtasks /run /tn ...`) - runs independently of the SSH session's Job Object.
- **Drive-letter and directory-based WinFSP mounts are bound to the logon session that created
  them** - a mount started from one SSH session isn't visible from a different SSH session (a
  different logon session), even though the mount process is confirmed still running. Keep
  mounting and observing/testing in the same script/process/session.
- `powershell -File ...` is blocked by the default execution policy - pass
  `-ExecutionPolicy Bypass`.
- Multi-layer shell escaping (bash → ssh → cmd → powershell) is error-prone for anything
  non-trivial - prefer staging a script on the remote machine (`scp`) and executing that, over
  inlining complex commands through every layer.
- The hostname itself may not resolve from WSL2 (no NetBIOS/mDNS there) - `ping <host>` in `cmd.exe`
  may only return a link-local IPv6 address unreachable from WSL2's own virtual network;
  `ping /4 <host>` returns a usable IPv4 LAN address instead.
- A WinFSP mountpoint can't itself be an SMB share root (`ERROR_SHARING_VIOLATION`) - share the
  parent directory instead; the mount is then reachable as a subfolder of that share.

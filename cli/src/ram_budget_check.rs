//! Shared startup guard for any `--*-mb`-style RAM budget flag (`mount
//! --write-cache-mb`, `store --chunk-buffer-mb`) - factored out of
//! `mount.rs` once `store` needed the exact same check for its own new
//! chunk-buffer budget (see `docs/plans/implemented/bounded-memory-io-pipeline.md`).

/// Refuses to start (unless `allow_swap_risk` downgrades this to a
/// warning) if `budget_mb` is large enough, relative to *currently
/// available* (not total) system RAM, that actually using it could push
/// the system into swapping. Deliberately checks available rather than
/// total memory - this process isn't the only thing running on the
/// machine, and total memory says nothing about what's actually free
/// right now. A hard error by default even though the budget itself
/// degrades gracefully to disk spillover if ever actually exhausted (see
/// `write_cache::WriteCache`): swapping is a machine-wide problem this
/// process would be causing for *everything else* running on it, not
/// just a local slowdown for itself, so it defaults to refusing rather
/// than merely warning.
///
/// `flag_name` (without its leading `--`) is used only to phrase the
/// error/warning message for whichever command called this.
pub(crate) fn check_ram_budget(
    flag_name: &str,
    budget_mb: u64,
    allow_swap_risk: bool,
) -> Result<(), String> {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let available_mb = sys.available_memory() / (1024 * 1024);
    let reason = if budget_mb > available_mb {
        Some(format!(
            "--{flag_name}={budget_mb} exceeds currently available RAM \
             (~{available_mb} MB) - this may cause swapping"
        ))
    } else if budget_mb * 2 > available_mb {
        Some(format!(
            "--{flag_name}={budget_mb} is more than half of currently \
             available RAM (~{available_mb} MB) - heavy concurrent use may cause swapping"
        ))
    } else {
        None
    };
    match reason {
        None => Ok(()),
        Some(reason) if allow_swap_risk => {
            eprintln!("warning: {reason}; continuing because --allow-swap-risk was given");
            Ok(())
        }
        Some(reason) => Err(format!(
            "{reason}; pass --allow-swap-risk to start anyway, or lower --{flag_name}"
        )),
    }
}

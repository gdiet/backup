//! The one-line warning a command prints at start when it was explicitly given an option that can
//! make it behave in a way the operator might not expect to be permanent or safe. The warning
//! names the option and states its consequence. The option's own help text and the manual
//! (`docs/manual.md`) explain it in more detail.

/// Writes the warning for `option` to stderr.
pub fn warn(option: &str, consequence: &str) {
    eprintln!("{}", line(option, consequence));
}

fn line(option: &str, consequence: &str) -> String {
    format!("warning: {option}: {consequence}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_the_option_and_states_the_consequence() {
        assert_eq!(
            line("--best-effort", "something happens"),
            "warning: --best-effort: something happens"
        );
    }
}

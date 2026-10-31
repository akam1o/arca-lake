//! arca-lake command-line entry point.
//!
//! Phase 0 stub: prints the planned subcommands and exits. Real argument
//! parsing is added when the first subcommand has something to run.

use std::process::ExitCode;

const USAGE: &str = "\
arca-lake (design phase)

Planned subcommands:
  gateway        run an S3 gateway
  storage-node   run a storage node
  controller     run the control plane
  admin          administrative commands

No subcommand is implemented yet. See docs/roadmap/phase-0.md.
";

fn main() -> ExitCode {
    eprint!("{USAGE}");
    ExitCode::from(2)
}

//! The metric registry (RFC-013 §7, Phase 3.4 §11): every exported metric
//! family with its type, process, unit, labels and help, in
//! `metrics-registry.tsv`. Processes take their `# HELP` lines from it
//! ([`annotate`]); [`validate`] checks an exposition against it (tests).

use std::sync::OnceLock;

/// The registry file.
pub const REGISTRY_TSV: &str = include_str!("metrics-registry.tsv");

/// Allowed values of a label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelValues {
    /// A closed set.
    Closed(Vec<String>),
    /// An S3 operation name (bounded by the API; `unknown`, `other`).
    S3Op,
}

/// One metric family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Family {
    /// Name.
    pub name: String,
    /// `counter`, `gauge` or `histogram`.
    pub kind: String,
    /// `gateway`, `node` or `controller`.
    pub process: String,
    /// Unit.
    pub unit: String,
    /// Labels (a histogram's `le` is implicit).
    pub labels: Vec<(String, LabelValues)>,
    /// Help text.
    pub help: String,
}

/// The parsed registry.
///
/// # Panics
///
/// When the embedded file is malformed (caught by the tests).
pub fn registry() -> &'static [Family] {
    static R: OnceLock<Vec<Family>> = OnceLock::new();
    R.get_or_init(|| {
        REGISTRY_TSV
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| {
                let c: Vec<&str> = l.split('\t').collect();
                assert_eq!(c.len(), 6, "registry row: {l}");
                let labels = if c[4] == "-" {
                    Vec::new()
                } else {
                    c[4].split(',')
                        .map(|kv| {
                            let (k, v) = kv.split_once('=').expect("label=values");
                            let v = if v == "~s3op" {
                                LabelValues::S3Op
                            } else {
                                LabelValues::Closed(v.split('|').map(str::to_string).collect())
                            };
                            (k.to_string(), v)
                        })
                        .collect()
                };
                Family {
                    name: c[0].into(),
                    kind: c[1].into(),
                    process: c[2].into(),
                    unit: c[3].into(),
                    labels,
                    help: c[5].into(),
                }
            })
            .collect()
    })
}

/// The family of `name`, if registered.
#[must_use]
pub fn family(name: &str) -> Option<&'static Family> {
    registry().iter().find(|f| f.name == name)
}

/// Replace every `# HELP` line of an exposition with the registry's help,
/// placed before the family's `# TYPE` line.
#[must_use]
pub fn annotate(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 4096);
    for line in text.lines() {
        if line.starts_with("# HELP ") {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ")
            && let Some(f) = rest.split_whitespace().next().and_then(family)
        {
            out.push_str("# HELP ");
            out.push_str(&f.name);
            out.push(' ');
            out.push_str(&f.help);
            out.push('\n');
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn parse_labels(s: &str) -> Option<Vec<(String, String)>> {
    let mut out = Vec::new();
    let mut rest = s;
    while !rest.is_empty() {
        let (k, r) = rest.split_once("=\"")?;
        let end = r.find('"')?;
        out.push((k.trim_start_matches(',').to_string(), r[..end].to_string()));
        rest = &r[end + 1..];
    }
    Some(out)
}

fn plausible_s3_op(v: &str) -> bool {
    v == "unknown"
        || v == "other"
        || (v.len() <= 64
            && v.starts_with(|c: char| c.is_ascii_uppercase())
            && v.chars().all(|c| c.is_ascii_alphanumeric()))
}

/// Problems of `process`'s exposition `text` against the registry: an
/// unknown family, a wrong type or process, a family split or interleaved,
/// a series outside its family, an undeclared label or a value outside its
/// set, a value that looks like an address, a path or a key, a missing
/// `# HELP`, a registered family that is not exported.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn validate(process: &str, text: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let mut seen: Vec<&str> = Vec::new();
    let mut current: Option<&Family> = None;
    let mut helped: Option<String> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            helped = rest.split_whitespace().next().map(str::to_string);
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut it = rest.split_whitespace();
            let (Some(name), Some(kind)) = (it.next(), it.next()) else {
                problems.push(format!("malformed TYPE line: {line}"));
                continue;
            };
            let Some(f) = family(name) else {
                problems.push(format!("{process}: {name} is not in the registry"));
                current = None;
                continue;
            };
            if f.kind != kind {
                problems.push(format!(
                    "{name}: exported as {kind}, registered as {}",
                    f.kind
                ));
            }
            if f.process != process {
                problems.push(format!(
                    "{name}: exported by {process}, registered for {}",
                    f.process
                ));
            }
            if helped.as_deref() != Some(name) {
                problems.push(format!("{name}: no # HELP line"));
            }
            if seen.contains(&name) {
                problems.push(format!("{name}: family declared twice (split)"));
            }
            seen.push(&f.name);
            current = Some(f);
            continue;
        }
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let (series, _value) = line.rsplit_once(' ').unwrap_or((line, ""));
        let (name, labels) = match series.split_once('{') {
            Some((n, l)) => (n, l.strip_suffix('}').unwrap_or(l)),
            None => (series, ""),
        };
        let Some(f) = current else {
            problems.push(format!(
                "{process}: series {name} outside a declared family"
            ));
            continue;
        };
        let suffix_ok = if f.kind == "histogram" {
            ["_bucket", "_sum", "_count"]
                .iter()
                .any(|s| name.strip_suffix(s) == Some(f.name.as_str()))
        } else {
            name == f.name
        };
        if !suffix_ok {
            problems.push(format!(
                "{name}: not a series of the current family {}",
                f.name
            ));
            continue;
        }
        let Some(pairs) = parse_labels(labels) else {
            problems.push(format!("{name}: malformed labels {labels}"));
            continue;
        };
        for (k, v) in &pairs {
            if k == "le" && f.kind == "histogram" && name.ends_with("_bucket") {
                continue;
            }
            match f.labels.iter().find(|(l, _)| l == k) {
                None => problems.push(format!("{name}: undeclared label {k}")),
                Some((_, LabelValues::Closed(set))) if !set.contains(v) => {
                    problems.push(format!("{name}: {k}=\"{v}\" is outside its closed set"));
                }
                Some((_, LabelValues::S3Op)) if !plausible_s3_op(v) => {
                    problems.push(format!("{name}: {k}=\"{v}\" is not an S3 operation name"));
                }
                _ => {}
            }
            if v.contains(['/', ':', '@', ' ', '.']) && k != "le" {
                problems.push(format!(
                    "{name}: {k}=\"{v}\" looks like an address, a path or a key"
                ));
            }
        }
        let declared: Vec<&str> = f.labels.iter().map(|(l, _)| l.as_str()).collect();
        let mut got: Vec<&str> = pairs
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| *k != "le")
            .collect();
        got.sort_unstable();
        let mut want = declared.clone();
        want.sort_unstable();
        if got != want {
            problems.push(format!("{name}: labels {got:?}, registered {want:?}"));
        }
    }
    for f in registry().iter().filter(|f| f.process == process) {
        if !seen.contains(&f.name.as_str()) {
            problems.push(format!(
                "{}: registered for {process} but not exported",
                f.name
            ));
        }
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_registry_parses_and_names_are_unique_and_well_formed() {
        let r = registry();
        assert!(r.len() > 100);
        let mut names: Vec<&str> = r.iter().map(|f| f.name.as_str()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "duplicate rows");
        for f in r {
            assert!(
                f.name.starts_with(&format!("arca_{}_", f.process)),
                "{}",
                f.name
            );
            assert!(
                matches!(f.kind.as_str(), "counter" | "gauge" | "histogram"),
                "{}",
                f.name
            );
            assert_eq!(
                f.kind == "counter",
                f.name.ends_with("_total"),
                "{}: counters end in _total",
                f.name
            );
            assert!(!f.help.is_empty() && !f.unit.is_empty(), "{}", f.name);
            if f.kind == "histogram" || f.unit == "seconds" {
                assert!(f.name.ends_with("_seconds"), "{}", f.name);
            }
        }
    }

    #[test]
    fn validation_catches_what_it_should() {
        let ok = "# TYPE arca_node_put_in_flight gauge\narca_node_put_in_flight 0\n";
        let p = validate("node", &annotate(ok));
        assert!(p.iter().all(|x| x.contains("not exported")), "{p:?}");
        for (bad, want) in [
            (
                "# TYPE arca_node_nope gauge\narca_node_nope 1\n",
                "not in the registry",
            ),
            (
                "# TYPE arca_node_put_in_flight counter\narca_node_put_in_flight 1\n",
                "registered as gauge",
            ),
            (
                "# TYPE arca_node_put_in_flight gauge\narca_node_put_in_flight{host=\"a\"} 1\n",
                "undeclared label",
            ),
            (
                "# TYPE arca_node_phase_seconds histogram\narca_node_phase_seconds_count{phase=\"10.0.0.1:9\"} 1\n",
                "outside its closed set",
            ),
            ("arca_node_put_in_flight 1\n", "outside a declared family"),
        ] {
            let p = validate("node", &annotate(bad));
            assert!(p.iter().any(|x| x.contains(want)), "{bad}: {p:?}");
        }
        let p = validate(
            "node",
            "# TYPE arca_node_put_in_flight gauge\narca_node_put_in_flight 0\n",
        );
        assert!(p.iter().any(|x| x.contains("no # HELP")), "{p:?}");
    }
}

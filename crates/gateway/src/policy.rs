//! Bucket policy subset (RFC-012 §5.3.3): strict parsing of the supported
//! AWS policy JSON grammar and evaluation.
//!
//! The parser rejects every element it does not fully understand, so the
//! evaluator never meets an unknown element (rule 5). Evaluation is a pure
//! function of the policy and the request; `Deny` is monotonic: adding a
//! statement can never turn a `Deny` into an `Allow` (rule 2).

use std::net::IpAddr;

use serde_json::Value;

/// Actions the policy grammar accepts (and the gateway can enforce).
pub const SUPPORTED_ACTIONS: &[&str] = &[
    "s3:GetObject",
    "s3:GetObjectVersion",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
    "s3:ListBucket",
    "s3:ListBucketVersions",
    "s3:ListBucketMultipartUploads",
    "s3:ListMultipartUploadParts",
    "s3:AbortMultipartUpload",
    "s3:GetObjectAttributes",
    "s3:GetBucketVersioning",
    "s3:PutBucketVersioning",
    "s3:GetBucketPolicy",
    "s3:PutBucketPolicy",
    "s3:DeleteBucketPolicy",
    "s3:GetBucketLocation",
    "s3:DeleteBucket",
];

/// Condition keys the grammar accepts, with their operator family.
const STRING_KEYS: &[&str] = &[
    "s3:prefix",
    "s3:delimiter",
    "s3:x-amz-metadata-directive",
    "s3:x-amz-copy-source",
];
const IP_KEYS: &[&str] = &["aws:SourceIp"];
const BOOL_KEYS: &[&str] = &["aws:SecureTransport"];

/// A rejected policy document (the message names the offending element).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyError(pub String);

impl std::error::Error for PolicyError {}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn err<T>(msg: impl Into<String>) -> Result<T, PolicyError> {
    Err(PolicyError(msg.into()))
}

/// `Effect`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// Allow.
    Allow,
    /// Deny.
    Deny,
}

/// `Principal`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Principals {
    Any,
    Names(Vec<String>),
}

/// A string pattern with `*` wildcards (`?` is not supported by this
/// subset and is rejected at parse time).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pattern(String);

impl Pattern {
    fn matches(&self, s: &str) -> bool {
        glob_match(&self.0, s)
    }
}

/// `*`-only glob match (case-sensitive).
fn glob_match(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if i == 0 {
            if !s.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            return s.len() >= pos && s[pos..].ends_with(part);
        } else if let Some(found) = s[pos..].find(part) {
            pos += found + part.len();
        } else {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Resource {
    /// `*`.
    Any,
    /// `arn:aws:s3:::bucket` (the policy's bucket).
    Bucket,
    /// `arn:aws:s3:::bucket/<pattern>`.
    Objects(Pattern),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Condition {
    StringEquals(String, Vec<String>),
    StringNotEquals(String, Vec<String>),
    StringLike(String, Vec<Pattern>),
    StringNotLike(String, Vec<Pattern>),
    IpAddress(Vec<Cidr>),
    NotIpAddress(Vec<Cidr>),
    Bool(String, bool),
}

/// An IP network in CIDR notation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    fn parse(s: &str) -> Option<Self> {
        let (a, p) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let addr: IpAddr = a.parse().ok()?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match p {
            Some(p) => p.parse().ok().filter(|n| *n <= max)?,
            None => max,
        };
        Some(Self { addr, prefix })
    }

    fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - u32::from(self.prefix))
                };
                u32::from(n) & mask == u32::from(a) & mask
            }
            (IpAddr::V6(n), IpAddr::V6(a)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - u32::from(self.prefix))
                };
                u128::from(n) & mask == u128::from(a) & mask
            }
            _ => false,
        }
    }
}

/// One statement.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Statement {
    sid: Option<String>,
    effect: Effect,
    principals: Principals,
    actions: Vec<Pattern>,
    resources: Vec<Resource>,
    conditions: Vec<Condition>,
}

/// A validated bucket policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    bucket: String,
    statements: Vec<Statement>,
}

/// What a request looks like to the evaluator.
#[derive(Debug, Clone, Default)]
pub struct Request<'a> {
    /// Principal name, or `"anonymous"`.
    pub principal: &'a str,
    /// Action, e.g. `s3:GetObject`.
    pub action: &'a str,
    /// Object key, for object-scoped actions.
    pub key: Option<&'a str>,
    /// `prefix` query parameter of a listing.
    pub prefix: Option<&'a str>,
    /// `delimiter` query parameter of a listing.
    pub delimiter: Option<&'a str>,
    /// Client address.
    pub source_ip: Option<IpAddr>,
    /// Whether the request arrived over TLS.
    pub secure_transport: bool,
    /// `x-amz-metadata-directive` of a copy.
    pub metadata_directive: Option<&'a str>,
    /// `x-amz-copy-source` of a copy.
    pub copy_source: Option<&'a str>,
}

/// Outcome of evaluating a policy against a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// A `Deny` statement matched (`Sid` index for the audit record).
    Deny(usize),
    /// No `Deny` matched and an `Allow` statement did.
    Allow(usize),
    /// No statement matched.
    NoMatch,
}

/// The principal name used for unauthenticated requests.
pub const ANONYMOUS: &str = "anonymous";

fn strings(v: &Value, what: &str) -> Result<Vec<String>, PolicyError> {
    match v {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Array(a) => a
            .iter()
            .map(|x| match x {
                Value::String(s) => Ok(s.clone()),
                _ => err(format!("{what}: expected a string or a list of strings")),
            })
            .collect(),
        _ => err(format!("{what}: expected a string or a list of strings")),
    }
}

fn check_pattern(p: &str, what: &str) -> Result<Pattern, PolicyError> {
    if p.contains('?') {
        return err(format!("{what}: the '?' wildcard is not supported"));
    }
    Ok(Pattern(p.to_string()))
}

impl Policy {
    /// Parse and validate a policy document for `bucket`. Every unsupported
    /// element is rejected with a message naming it (RFC-012 §5.3.2 rule 5).
    ///
    /// # Errors
    ///
    /// [`PolicyError`] naming the first offending element.
    #[allow(clippy::too_many_lines)]
    pub fn parse(bucket: &str, json: &str) -> Result<Self, PolicyError> {
        if json.len() > 1 << 20 {
            return err("policy document larger than 1 MiB");
        }
        let doc: Value =
            serde_json::from_str(json).map_err(|e| PolicyError(format!("invalid JSON: {e}")))?;
        let Value::Object(top) = &doc else {
            return err("policy must be a JSON object");
        };
        for k in top.keys() {
            if !matches!(k.as_str(), "Version" | "Statement" | "Id") {
                return err(format!("unsupported top-level element '{k}'"));
            }
        }
        match top.get("Version") {
            Some(Value::String(v)) if v == "2012-10-17" => {}
            Some(_) => return err("Version: only \"2012-10-17\" is supported"),
            None => return err("Version is required"),
        }
        let statements = match top.get("Statement") {
            Some(Value::Array(a)) => a.clone(),
            Some(o @ Value::Object(_)) => vec![o.clone()],
            _ => return err("Statement must be an object or a list of objects"),
        };
        let mut out = Vec::with_capacity(statements.len());
        let mut sids = std::collections::HashSet::new();
        for (i, st) in statements.iter().enumerate() {
            let Value::Object(st) = st else {
                return err(format!("Statement[{i}]: expected an object"));
            };
            let at = |e: &str| format!("Statement[{i}].{e}");
            for k in st.keys() {
                if !matches!(
                    k.as_str(),
                    "Sid" | "Effect" | "Principal" | "Action" | "Resource" | "Condition"
                ) {
                    return err(format!("{}: unsupported element", at(k)));
                }
            }
            let sid = match st.get("Sid") {
                None => None,
                Some(Value::String(s)) => {
                    if !sids.insert(s.clone()) {
                        return err(format!("{}: duplicate Sid '{s}'", at("Sid")));
                    }
                    Some(s.clone())
                }
                Some(_) => return err(format!("{}: expected a string", at("Sid"))),
            };
            let effect = match st.get("Effect") {
                Some(Value::String(s)) if s == "Allow" => Effect::Allow,
                Some(Value::String(s)) if s == "Deny" => Effect::Deny,
                _ => return err(format!("{}: must be \"Allow\" or \"Deny\"", at("Effect"))),
            };
            let principals = match st.get("Principal") {
                Some(Value::String(s)) if s == "*" => Principals::Any,
                Some(Value::Object(p)) => {
                    for k in p.keys() {
                        if k != "AWS" {
                            return err(format!(
                                "{}: unsupported principal type '{k}'",
                                at("Principal")
                            ));
                        }
                    }
                    let Some(aws) = p.get("AWS") else {
                        return err(format!("{}: expected {{\"AWS\": ...}}", at("Principal")));
                    };
                    let names = strings(aws, &at("Principal.AWS"))?;
                    if names.iter().any(|n| n == "*") {
                        if names.len() != 1 {
                            return err(format!(
                                "{}: \"*\" cannot be combined with names",
                                at("Principal.AWS")
                            ));
                        }
                        Principals::Any
                    } else {
                        let mut out = Vec::new();
                        for n in names {
                            // The IAM ARN form is accepted; its last path
                            // element is the principal name.
                            let name = if let Some(rest) = n.strip_prefix("arn:aws:iam::") {
                                rest.rsplit('/').next().unwrap_or(rest).to_string()
                            } else {
                                n
                            };
                            if name.is_empty() || name.contains('*') {
                                return err(format!(
                                    "{}: invalid principal name",
                                    at("Principal.AWS")
                                ));
                            }
                            out.push(name);
                        }
                        Principals::Names(out)
                    }
                }
                Some(_) => {
                    return err(format!(
                        "{}: expected \"*\" or {{\"AWS\": ...}}",
                        at("Principal")
                    ));
                }
                None => return err(format!("{}: Principal is required", at("Principal"))),
            };
            let mut actions = Vec::new();
            for a in strings(
                st.get("Action").ok_or_else(|| {
                    PolicyError(format!(
                        "{}: Action is required (NotAction is not supported)",
                        at("Action")
                    ))
                })?,
                &at("Action"),
            )? {
                let p = check_pattern(&a, &at("Action"))?;
                if !SUPPORTED_ACTIONS.iter().any(|s| p.matches(s)) {
                    return err(format!("{}: unsupported action '{a}'", at("Action")));
                }
                actions.push(p);
            }
            let mut resources = Vec::new();
            for r in strings(
                st.get("Resource").ok_or_else(|| {
                    PolicyError(format!(
                        "{}: Resource is required (NotResource is not supported)",
                        at("Resource")
                    ))
                })?,
                &at("Resource"),
            )? {
                if r == "*" {
                    resources.push(Resource::Any);
                    continue;
                }
                let Some(rest) = r.strip_prefix("arn:aws:s3:::") else {
                    return err(format!("{}: unsupported resource '{r}'", at("Resource")));
                };
                match rest.split_once('/') {
                    None if rest == bucket => resources.push(Resource::Bucket),
                    Some((b, pat)) if b == bucket => {
                        resources.push(Resource::Objects(check_pattern(pat, &at("Resource"))?));
                    }
                    _ => {
                        return err(format!(
                            "{}: resource '{r}' is not in bucket '{bucket}'",
                            at("Resource")
                        ));
                    }
                }
            }
            let mut conditions = Vec::new();
            if let Some(c) = st.get("Condition") {
                let Value::Object(c) = c else {
                    return err(format!("{}: expected an object", at("Condition")));
                };
                for (op, keys) in c {
                    let Value::Object(keys) = keys else {
                        return err(format!(
                            "{}: expected an object",
                            at(&format!("Condition.{op}"))
                        ));
                    };
                    for (key, vals) in keys {
                        let where_ = at(&format!("Condition.{op}.{key}"));
                        let cond = match op.as_str() {
                            "StringEquals" | "StringNotEquals" | "StringLike" | "StringNotLike" => {
                                if !STRING_KEYS.contains(&key.as_str()) {
                                    return err(format!("{where_}: unsupported condition key"));
                                }
                                let vs = strings(vals, &where_)?;
                                match op.as_str() {
                                    "StringEquals" => Condition::StringEquals(key.clone(), vs),
                                    "StringNotEquals" => {
                                        Condition::StringNotEquals(key.clone(), vs)
                                    }
                                    "StringLike" => Condition::StringLike(
                                        key.clone(),
                                        vs.iter()
                                            .map(|v| check_pattern(v, &where_))
                                            .collect::<Result<_, _>>()?,
                                    ),
                                    _ => Condition::StringNotLike(
                                        key.clone(),
                                        vs.iter()
                                            .map(|v| check_pattern(v, &where_))
                                            .collect::<Result<_, _>>()?,
                                    ),
                                }
                            }
                            "IpAddress" | "NotIpAddress" => {
                                if !IP_KEYS.contains(&key.as_str()) {
                                    return err(format!("{where_}: unsupported condition key"));
                                }
                                let mut nets = Vec::new();
                                for v in strings(vals, &where_)? {
                                    nets.push(Cidr::parse(&v).ok_or_else(|| {
                                        PolicyError(format!("{where_}: invalid CIDR '{v}'"))
                                    })?);
                                }
                                if op == "IpAddress" {
                                    Condition::IpAddress(nets)
                                } else {
                                    Condition::NotIpAddress(nets)
                                }
                            }
                            "Bool" => {
                                if !BOOL_KEYS.contains(&key.as_str()) {
                                    return err(format!("{where_}: unsupported condition key"));
                                }
                                let b = match vals {
                                    Value::Bool(b) => *b,
                                    Value::String(s) if s == "true" => true,
                                    Value::String(s) if s == "false" => false,
                                    _ => return err(format!("{where_}: expected true or false")),
                                };
                                Condition::Bool(key.clone(), b)
                            }
                            _ => {
                                return err(format!(
                                    "{where_}: unsupported condition operator '{op}'"
                                ));
                            }
                        };
                        conditions.push(cond);
                    }
                }
            }
            out.push(Statement {
                sid,
                effect,
                principals,
                actions,
                resources,
                conditions,
            });
        }
        if out.is_empty() {
            return err("Statement: at least one statement is required");
        }
        Ok(Self {
            bucket: bucket.to_string(),
            statements: out,
        })
    }

    /// The bucket this policy was registered for.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// `Sid` of statement `i`, for audit records.
    #[must_use]
    pub fn sid(&self, i: usize) -> Option<&str> {
        self.statements.get(i).and_then(|s| s.sid.as_deref())
    }

    /// Evaluate (RFC-012 §5.3.2 rules 1 and 2): any matching `Deny` wins;
    /// otherwise any matching `Allow`; otherwise no match (which the
    /// caller treats as deny).
    #[must_use]
    pub fn evaluate(&self, req: &Request<'_>) -> Decision {
        let mut allow = None;
        for (i, st) in self.statements.iter().enumerate() {
            if !st.matches(req) {
                continue;
            }
            match st.effect {
                Effect::Deny => return Decision::Deny(i),
                Effect::Allow => {
                    allow.get_or_insert(i);
                }
            }
        }
        allow.map_or(Decision::NoMatch, Decision::Allow)
    }
}

impl Statement {
    fn matches(&self, req: &Request<'_>) -> bool {
        let principal_ok = match &self.principals {
            Principals::Any => true,
            Principals::Names(names) => {
                req.principal != ANONYMOUS && names.iter().any(|n| n == req.principal)
            }
        };
        if !principal_ok {
            return false;
        }
        if !self.actions.iter().any(|a| a.matches(req.action)) {
            return false;
        }
        let object_scoped = req.key.is_some();
        let resource_ok = self.resources.iter().any(|r| match r {
            Resource::Any => true,
            Resource::Bucket => !object_scoped,
            Resource::Objects(p) => req.key.is_some_and(|k| p.matches(k)),
        });
        if !resource_ok {
            return false;
        }
        self.conditions.iter().all(|c| c.holds(req))
    }
}

impl Condition {
    fn holds(&self, req: &Request<'_>) -> bool {
        let string_value = |key: &str| -> Option<&str> {
            match key {
                "s3:prefix" => req.prefix,
                "s3:delimiter" => req.delimiter,
                "s3:x-amz-metadata-directive" => req.metadata_directive,
                "s3:x-amz-copy-source" => req.copy_source,
                _ => None,
            }
        };
        match self {
            // AWS semantics: a missing key makes positive conditions false
            // and negative ("Not") conditions true.
            Self::StringEquals(k, vs) => string_value(k).is_some_and(|v| vs.iter().any(|x| x == v)),
            Self::StringNotEquals(k, vs) => {
                string_value(k).is_none_or(|v| !vs.iter().any(|x| x == v))
            }
            Self::StringLike(k, ps) => {
                string_value(k).is_some_and(|v| ps.iter().any(|p| p.matches(v)))
            }
            Self::StringNotLike(k, ps) => {
                string_value(k).is_none_or(|v| !ps.iter().any(|p| p.matches(v)))
            }
            Self::IpAddress(nets) => req
                .source_ip
                .is_some_and(|ip| nets.iter().any(|n| n.contains(ip))),
            Self::NotIpAddress(nets) => req
                .source_ip
                .is_none_or(|ip| !nets.iter().any(|n| n.contains(ip))),
            Self::Bool(_, b) => req.secure_transport == *b,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(json: &str) -> Result<Policy, PolicyError> {
        Policy::parse("b", json)
    }

    fn req<'a>(principal: &'a str, action: &'a str, key: Option<&'a str>) -> Request<'a> {
        Request {
            principal,
            action,
            key,
            ..Default::default()
        }
    }

    #[test]
    fn glob() {
        assert!(glob_match("s3:*", "s3:GetObject"));
        assert!(glob_match("s3:Get*", "s3:GetObject"));
        assert!(!glob_match("s3:Get*", "s3:PutObject"));
        assert!(glob_match("*Object*", "s3:GetObjectVersion"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(!glob_match("a*b*c", "aXXbYY"));
        assert!(glob_match("*", ""));
        assert!(glob_match("logs/*", "logs/x"));
        assert!(!glob_match("logs/*", "log/x"));
    }

    #[test]
    fn rejects_every_unsupported_element() {
        let cases = [
            (r#"{"Statement":[]}"#, "Version"),
            (r#"{"Version":"2008-10-17","Statement":[]}"#, "Version"),
            (r#"{"Version":"2012-10-17","Statement":[]}"#, "at least one"),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","NotAction":"s3:*","Resource":"*"}]}"#,
                "NotAction",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:*","NotResource":"*"}]}"#,
                "NotResource",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"x"},"Action":"s3:*","Resource":"*"}]}"#,
                "Service",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectAcl","Resource":"*"}]}"#,
                "PutObjectAcl",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::other/*"}]}"#,
                "not in bucket",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":{"aws:userid":"x"}}}]}"#,
                "aws:userid",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*","Condition":{"DateLessThan":{"aws:CurrentTime":"x"}}}]}"#,
                "DateLessThan",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*","Condition":{"StringEqualsIfExists":{"s3:prefix":"x"}}}]}"#,
                "IfExists",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*","Extra":1}]}"#,
                "Extra",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Sid":"a","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"},{"Sid":"a","Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}"#,
                "duplicate Sid",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/a?c"}]}"#,
                "'?'",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/99"}}}]}"#,
                "CIDR",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Maybe","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}"#,
                "Effect",
            ),
            (
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#,
                "Principal is required",
            ),
        ];
        for (json, needle) in cases {
            let e = p(json).expect_err(json);
            assert!(e.0.contains(needle), "{json}: {e}");
        }
    }

    #[test]
    fn deny_wins_allow_matches_and_scoping_works() {
        let pol = p(r#"{
          "Version": "2012-10-17",
          "Statement": [
            {"Sid": "read", "Effect": "Allow", "Principal": {"AWS": ["alice", "arn:aws:iam::123456789012:user/bob"]},
             "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": ["arn:aws:s3:::b", "arn:aws:s3:::b/public/*"]},
            {"Sid": "nosecret", "Effect": "Deny", "Principal": "*", "Action": "s3:*", "Resource": "arn:aws:s3:::b/public/secret*"},
            {"Sid": "anon", "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::b/www/*",
             "Condition": {"Bool": {"aws:SecureTransport": "true"}, "IpAddress": {"aws:SourceIp": ["10.0.0.0/8", "2001:db8::/32"]}}},
            {"Sid": "list", "Effect": "Allow", "Principal": {"AWS": "carol"}, "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::b",
             "Condition": {"StringLike": {"s3:prefix": ["carol/*"]}}}
          ]}"#).unwrap();
        assert_eq!(
            pol.evaluate(&req("alice", "s3:GetObject", Some("public/a"))),
            Decision::Allow(0)
        );
        assert_eq!(
            pol.evaluate(&req("bob", "s3:GetObject", Some("public/a"))),
            Decision::Allow(0)
        );
        assert_eq!(
            pol.evaluate(&req("alice", "s3:GetObject", Some("private/a"))),
            Decision::NoMatch
        );
        assert_eq!(
            pol.evaluate(&req("alice", "s3:PutObject", Some("public/a"))),
            Decision::NoMatch
        );
        assert_eq!(
            pol.evaluate(&req("alice", "s3:GetObject", Some("public/secret1"))),
            Decision::Deny(1)
        );
        assert_eq!(
            pol.evaluate(&req("alice", "s3:ListBucket", None)),
            Decision::Allow(0)
        );
        assert_eq!(
            pol.evaluate(&req("dave", "s3:GetObject", Some("public/a"))),
            Decision::NoMatch
        );
        // Anonymous: only the conditional statement, and only when it holds.
        let mut r = req(ANONYMOUS, "s3:GetObject", Some("www/index.html"));
        assert_eq!(pol.evaluate(&r), Decision::NoMatch, "no ip, no tls");
        r.secure_transport = true;
        r.source_ip = Some("10.1.2.3".parse().unwrap());
        assert_eq!(pol.evaluate(&r), Decision::Allow(2));
        r.source_ip = Some("192.168.1.1".parse().unwrap());
        assert_eq!(pol.evaluate(&r), Decision::NoMatch);
        r.source_ip = Some("2001:db8::1".parse().unwrap());
        assert_eq!(pol.evaluate(&r), Decision::Allow(2));
        // Named principals never match anonymous requests.
        assert_eq!(
            pol.evaluate(&req(ANONYMOUS, "s3:GetObject", Some("public/a"))),
            Decision::NoMatch
        );
        // Prefix condition.
        let mut r = req("carol", "s3:ListBucket", None);
        assert_eq!(pol.evaluate(&r), Decision::NoMatch, "missing prefix key");
        r.prefix = Some("carol/photos");
        assert_eq!(pol.evaluate(&r), Decision::Allow(3));
        r.prefix = Some("alice/");
        assert_eq!(pol.evaluate(&r), Decision::NoMatch);
        assert_eq!(pol.sid(1), Some("nosecret"));
    }

    #[test]
    fn deny_is_monotonic_over_added_statements() {
        // Property: for every (policy, extra statement, request), if the
        // policy denies, the policy plus the extra statement denies too.
        let base = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/x*"}]}"#;
        let extra = [
            r#"{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"}"#,
            r#"{"Effect":"Allow","Principal":{"AWS":"alice"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}"#,
            r#"{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"*"}"#,
        ];
        let pol = p(base).unwrap();
        for e in extra {
            let combined = base.replace("}]}", &format!("}},{e}]}}"));
            let pol2 = p(&combined).unwrap();
            for key in ["x1", "x", "y", "xyz"] {
                for who in ["alice", ANONYMOUS, "bob"] {
                    let r = req(who, "s3:GetObject", Some(key));
                    if matches!(pol.evaluate(&r), Decision::Deny(_)) {
                        assert!(
                            matches!(pol2.evaluate(&r), Decision::Deny(_)),
                            "{e} {key} {who}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn cidr_parsing_and_matching() {
        let c = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(c.contains("10.255.1.1".parse().unwrap()));
        assert!(!c.contains("11.0.0.1".parse().unwrap()));
        assert!(
            Cidr::parse("1.2.3.4")
                .unwrap()
                .contains("1.2.3.4".parse().unwrap())
        );
        assert!(
            Cidr::parse("0.0.0.0/0")
                .unwrap()
                .contains("9.9.9.9".parse().unwrap())
        );
        assert!(
            Cidr::parse("::/0")
                .unwrap()
                .contains("::1".parse().unwrap())
        );
        assert!(
            !Cidr::parse("::/0")
                .unwrap()
                .contains("1.1.1.1".parse().unwrap())
        );
        assert!(Cidr::parse("1.2.3.4/33").is_none());
        assert!(Cidr::parse("nope").is_none());
    }
}

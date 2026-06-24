use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use minijinja::{Environment, UndefinedBehavior};
use regex::Regex;
use serde_json::Value;

pub fn validate_template(source: &str, allowed_top_level: &[String]) -> Result<()> {
    let mut env = environment();
    env.add_template("message", source)
        .context("invalid message template")?;

    let allowed: HashSet<&str> = allowed_top_level.iter().map(String::as_str).collect();
    for variable in detectable_top_level_variables(source) {
        if !allowed.contains(variable.as_str()) {
            bail!("unknown top-level template variable {variable:?}");
        }
    }
    Ok(())
}

pub fn render_template(source: &str, context: &Value) -> Result<String> {
    let mut env = environment();
    env.add_template("message", source)
        .context("invalid message template")?;
    env.get_template("message")?
        .render(context)
        .context("failed to render message template")
}

fn environment() -> Environment<'static> {
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Lenient);
    env
}

fn detectable_top_level_variables(source: &str) -> Vec<String> {
    let loop_variable =
        Regex::new(r#"\{%\s*for\s+([A-Za-z_]\w*)\s+in"#).expect("static loop regex must compile");
    let locals = loop_variable
        .captures_iter(source)
        .filter_map(|capture| capture.get(1).map(|value| value.as_str().to_owned()))
        .collect::<HashSet<_>>();
    let expression = Regex::new(r#"(?:\{\{|\{%\s*(?:if|for\s+\w+\s+in))\s*([A-Za-z_]\w*)"#)
        .expect("static template variable regex must compile");
    expression
        .captures_iter(source)
        .filter_map(|capture| capture.get(1).map(|value| value.as_str().to_owned()))
        .filter(|value| {
            !locals.contains(value)
                && !matches!(
                    value.as_str(),
                    "true" | "false" | "none" | "loop" | "else" | "endif" | "endfor"
                )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn renders_missing_values_as_empty() {
        assert_eq!(
            render_template(
                "{{ stream.title }}|{{ stream.missing }}",
                &json!({"stream": {"title": "Live"}})
            )
            .unwrap(),
            "Live|"
        );
    }

    #[test]
    fn rejects_unknown_top_level_values() {
        let error = validate_template("{{ unknown.value }}", &["stream".into()]).unwrap_err();
        assert!(error.to_string().contains("unknown"));
    }

    #[test]
    fn rejects_malformed_templates() {
        assert!(validate_template("{% if stream %}", &["stream".into()]).is_err());
    }

    #[test]
    fn permits_loop_local_variables() {
        validate_template(
            "{% for item in stream.tags %}{{ item }}{% endfor %}",
            &["stream".into()],
        )
        .unwrap();
    }
}

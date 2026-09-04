use std::collections::BTreeSet;

use async_trait::async_trait;
use serde::Deserialize;

use crate::error::{Result, RuntimeError};

use super::{Capability, Tool};

#[derive(Debug, Deserialize)]
struct HttpRequestArguments {
    url: String,
    #[serde(default = "default_method")]
    method: String,
    #[serde(default)]
    body: Option<String>,
}

fn default_method() -> String {
    "GET".to_owned()
}

pub struct HttpRequestTool {
    allowed_hosts: BTreeSet<String>,
    max_response_bytes: usize,
    client: reqwest::Client,
}

impl HttpRequestTool {
    pub fn new(allowed_hosts: BTreeSet<String>, max_response_bytes: usize) -> Result<Self> {
        let allowed_hosts = allowed_hosts
            .into_iter()
            .map(|host| host.to_ascii_lowercase())
            .collect();
        let client = reqwest::Client::builder()
            .user_agent("agent-runtime/0.1")
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| RuntimeError::Execution(format!("http_request: {error}")))?;

        Ok(Self {
            allowed_hosts,
            max_response_bytes,
            client,
        })
    }

    fn error(message: impl Into<String>) -> RuntimeError {
        RuntimeError::Execution(format!("http_request: {}", message.into()))
    }
}

#[async_trait]
impl Tool for HttpRequestTool {
    fn name(&self) -> &'static str {
        "http_request"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["http.get"]
    }

    fn required_capability(&self) -> Option<Capability> {
        Some(Capability::NetworkHttp)
    }

    async fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value> {
        let arguments: HttpRequestArguments = serde_json::from_value(arguments)
            .map_err(|error| Self::error(format!("invalid arguments: {error}")))?;
        let method = arguments.method.to_ascii_uppercase();
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|_| Self::error(format!("unsupported HTTP method '{method}'")))?;

        if !matches!(
            method,
            reqwest::Method::GET
                | reqwest::Method::POST
                | reqwest::Method::PUT
                | reqwest::Method::PATCH
                | reqwest::Method::DELETE
                | reqwest::Method::HEAD
        ) {
            return Err(Self::error(format!(
                "HTTP method '{method}' is not allowed"
            )));
        }

        let url = reqwest::Url::parse(&arguments.url)
            .map_err(|error| Self::error(format!("invalid URL: {error}")))?;

        if !matches!(url.scheme(), "http" | "https") {
            return Err(Self::error("URL scheme must be http or https"));
        }

        let host = url
            .host_str()
            .ok_or_else(|| Self::error("URL must include a host"))?
            .to_ascii_lowercase();

        if !self.allowed_hosts.contains(&host) {
            return Err(RuntimeError::PermissionDenied(format!(
                "http_request host '{host}' is not in the runtime allowlist"
            )));
        }

        let mut request = self.client.request(method.clone(), url.clone());
        if let Some(body) = arguments.body {
            request = request.body(body);
        }

        let response = request
            .send()
            .await
            .map_err(|error| Self::error(error.to_string()))?;
        let status = response.status();

        if !status.is_success() {
            return Err(Self::error(format!("request returned HTTP {status}")));
        }

        if response
            .content_length()
            .is_some_and(|length| length as usize > self.max_response_bytes)
        {
            return Err(Self::error(format!(
                "response exceeds the {max} byte limit",
                max = self.max_response_bytes
            )));
        }

        let body = response
            .bytes()
            .await
            .map_err(|error| Self::error(error.to_string()))?;

        if body.len() > self.max_response_bytes {
            return Err(Self::error(format!(
                "response exceeds the {max} byte limit",
                max = self.max_response_bytes
            )));
        }

        Ok(serde_json::json!({
            "url": url.as_str(),
            "method": method.as_str(),
            "status": status.as_u16(),
            "body": String::from_utf8_lossy(&body),
        }))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn http_request_rejects_hosts_outside_the_runtime_allowlist() {
        let allowed_hosts = BTreeSet::from(["example.com".to_owned()]);
        let tool = HttpRequestTool::new(allowed_hosts, 1_024).unwrap();
        let error = tool
            .execute(json!({ "url": "https://not-example.com" }))
            .await
            .unwrap_err();

        assert!(matches!(error, RuntimeError::PermissionDenied(_)));
    }
}

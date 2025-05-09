/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
use humantime_serde::re::humantime;
use reqwest::ClientBuilder;
use tracing::{error, trace, warn};

use super::ALERTS;

use super::{AlertState, CallableTarget, Context};

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(untagged)]
pub enum Retry {
    Infinite,
    Finite(usize),
}

impl Default for Retry {
    fn default() -> Self {
        Retry::Finite(1)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
#[serde(try_from = "TargetVerifier")]
pub struct Target {
    #[serde(flatten)]
    pub target: TargetType,
    #[serde(default, rename = "repeat")]
    pub timeout: Timeout,
}

impl Target {
    pub fn call(&self, context: Context) {
        trace!("target.call context- {context:?}");
        let timeout = &self.timeout;
        let resolves = context.alert_info.alert_state;
        let mut state = timeout.state.lock().unwrap();
        trace!("target.call state- {state:?}");
        state.alert_state = resolves;

        match resolves {
            AlertState::Triggered => {
                if !state.timed_out {
                    // call once and then start sleeping
                    // reduce repeats by 1
                    call_target(self.target.clone(), context.clone());
                    trace!("state not timed out- {state:?}");
                    // set state
                    state.timed_out = true;
                    state.awaiting_resolve = true;
                    drop(state);
                    self.spawn_timeout_task(timeout, context.clone());
                }
            }
            alert_state @ (AlertState::Resolved | AlertState::Silenced) => {
                state.alert_state = alert_state;
                if state.timed_out {
                    // if in timeout and resolve came in, only process if it's the first one ( awaiting resolve )
                    if state.awaiting_resolve {
                        state.awaiting_resolve = false;
                    } else {
                        // no further resolve will be considered in timeout period
                        return;
                    }
                }

                call_target(self.target.clone(), context);
            }
        }
    }

    fn spawn_timeout_task(&self, target_timeout: &Timeout, alert_context: Context) {
        trace!("repeat-\n{target_timeout:?}");
        let state = Arc::clone(&target_timeout.state);
        let retry = target_timeout.times;
        let timeout = target_timeout.interval;
        let target = self.target.clone();
        let alert_id = alert_context.alert_info.alert_id;

        let sleep_and_check_if_call =
            move |timeout_state: Arc<Mutex<TimeoutState>>, current_state: AlertState| {
                async move {
                    tokio::time::sleep(timeout).await;

                    let mut state = timeout_state.lock().unwrap();

                    if current_state == AlertState::Triggered {
                        // it is still firing .. sleep more and come back
                        state.awaiting_resolve = true;
                        true
                    } else {
                        state.timed_out = false;
                        false
                    }
                }
            };

        trace!("Spawning retry task");
        tokio::spawn(async move {
            match retry {
                Retry::Infinite => loop {
                    let current_state = if let Ok(state) = ALERTS.get_state(alert_id).await {
                        state
                    } else {
                        *state.lock().unwrap() = TimeoutState::default();
                        warn!("Unable to fetch state for given alert_id- {alert_id}, stopping target notifs");
                        return;
                    };

                    let should_call =
                        sleep_and_check_if_call(Arc::clone(&state), current_state).await;
                    if should_call {
                        call_target(target.clone(), alert_context.clone())
                    }
                },
                Retry::Finite(times) => {
                    for _ in 0..(times - 1) {
                        let current_state = if let Ok(state) = ALERTS.get_state(alert_id).await {
                            state
                        } else {
                            *state.lock().unwrap() = TimeoutState::default();
                            warn!("Unable to fetch state for given alert_id- {alert_id}, stopping target notifs");
                            return;
                        };

                        let should_call =
                            sleep_and_check_if_call(Arc::clone(&state), current_state).await;
                        if should_call {
                            call_target(target.clone(), alert_context.clone())
                        }
                    }
                    // // fallback for if this task only observed FIRING on all RETRIES
                    // // Stream might be dead and sending too many alerts is not great
                    // // Send and alert stating that this alert will only work once it has seen a RESOLVE
                    // state.lock().unwrap().timed_out = false;
                    // let context = alert_context;
                    // // context.alert_info.message = format!(
                    // //     "Triggering alert did not resolve itself after {times} retries, This alert is paused until it resolves");
                    // // Send and exit this task.
                    // call_target(target, context);
                }
            }
            *state.lock().unwrap() = TimeoutState::default();
        });
    }
}

fn call_target(target: TargetType, context: Context) {
    trace!("Calling target with context- {context:?}");
    tokio::spawn(async move { target.call(&context).await });
}

#[derive(Debug, serde::Deserialize)]
pub struct RepeatVerifier {
    interval: Option<String>,
    times: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct TargetVerifier {
    #[serde(flatten)]
    pub target: TargetType,
    #[serde(default)]
    pub repeat: Option<RepeatVerifier>,
}

impl TryFrom<TargetVerifier> for Target {
    type Error = String;

    fn try_from(value: TargetVerifier) -> Result<Self, Self::Error> {
        let mut timeout = Timeout::default();

        // Default is Infinite in case of alertmanager
        if matches!(value.target, TargetType::AlertManager(_)) {
            timeout.times = Retry::Infinite
        }

        if let Some(repeat_config) = value.repeat {
            let interval = repeat_config
                .interval
                .map(|ref interval| humantime::parse_duration(interval))
                .transpose()
                .map_err(|err| err.to_string())?;

            if let Some(interval) = interval {
                timeout.interval = interval
            }

            if let Some(times) = repeat_config.times {
                timeout.times = Retry::Finite(times)
            }
        }

        Ok(Target {
            target: value.target,
            timeout,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
#[serde(deny_unknown_fields)]
pub enum TargetType {
    Slack(SlackWebHook),
    #[serde(rename = "webhook")]
    Other(OtherWebHook),
    AlertManager(AlertManager),
}

impl TargetType {
    pub async fn call(&self, payload: &Context) {
        match self {
            TargetType::Slack(target) => target.call(payload).await,
            TargetType::Other(target) => target.call(payload).await,
            TargetType::AlertManager(target) => target.call(payload).await,
        }
    }
}

fn default_client_builder() -> ClientBuilder {
    ClientBuilder::new()
}

#[derive(Default, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlackWebHook {
    endpoint: String,
}

#[async_trait]
impl CallableTarget for SlackWebHook {
    async fn call(&self, payload: &Context) {
        let client = default_client_builder()
            .build()
            .expect("Client can be constructed on this system");

        let alert = match payload.alert_info.alert_state {
            AlertState::Triggered => {
                serde_json::json!({ "text": payload.default_alert_string() })
            }
            AlertState::Resolved => {
                serde_json::json!({ "text": payload.default_resolved_string() })
            }
            AlertState::Silenced => {
                serde_json::json!({ "text": payload.default_silenced_string() })
            }
        };

        if let Err(e) = client.post(&self.endpoint).json(&alert).send().await {
            error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OtherWebHook {
    endpoint: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    skip_tls_check: bool,
}

#[async_trait]
impl CallableTarget for OtherWebHook {
    async fn call(&self, payload: &Context) {
        let mut builder = default_client_builder();
        if self.skip_tls_check {
            builder = builder.danger_accept_invalid_certs(true)
        }

        let client = builder
            .build()
            .expect("Client can be constructed on this system");

        let alert = match payload.alert_info.alert_state {
            AlertState::Triggered => payload.default_alert_string(),
            AlertState::Resolved => payload.default_resolved_string(),
            AlertState::Silenced => payload.default_silenced_string(),
        };

        let request = client
            .post(&self.endpoint)
            .headers((&self.headers).try_into().expect("valid_headers"));

        if let Err(e) = request.body(alert).send().await {
            error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AlertManager {
    endpoint: String,
    #[serde(default)]
    skip_tls_check: bool,
    #[serde(flatten)]
    auth: Option<Auth>,
}

#[async_trait]
impl CallableTarget for AlertManager {
    async fn call(&self, payload: &Context) {
        let mut builder = default_client_builder();

        if self.skip_tls_check {
            builder = builder.danger_accept_invalid_certs(true)
        }

        if let Some(Auth { username, password }) = &self.auth {
            let basic_auth_value = "Basic ".to_string()
                + &base64::prelude::BASE64_STANDARD.encode(format!("{username}:{password}"));
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::try_from(basic_auth_value).expect("valid value"),
            )]);
            builder = builder.default_headers(headers)
        }

        let client = builder
            .build()
            .expect("Client can be constructed on this system");

        let mut alerts = serde_json::json!([{
          "labels": {
            "alertname": payload.alert_info.alert_name,
            // "stream": payload.stream,
            "deployment_instance": payload.deployment_info.deployment_instance,
            "deployment_id": payload.deployment_info.deployment_id,
            "deployment_mode": payload.deployment_info.deployment_mode
            },
          "annotations": {
            "message": "MESSAGE",
            "reason": "REASON"
          }
        }]);

        let alert = &mut alerts[0];

        // alert["labels"].as_object_mut().expect("is object").extend(
        //     payload
        //         .additional_labels
        //         .as_object()
        //         .expect("is object")
        //         .iter()
        //         // filter non null values for alertmanager and only pass strings
        //         .filter(|(_, value)| !value.is_null())
        //         .map(|(k, value)| (k.to_owned(), json::convert_to_string(value))),
        // );

        // fill in status label accordingly
        match payload.alert_info.alert_state {
            AlertState::Triggered => alert["labels"]["status"] = "triggered".into(),
            AlertState::Resolved => {
                alert["labels"]["status"] = "resolved".into();
                alert["annotations"]["reason"] =
                    serde_json::Value::String(payload.default_resolved_string());
                alert["endsAt"] = Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                    .into();
            }
            AlertState::Silenced => {
                alert["labels"]["status"] = "silenced".into();
                alert["annotations"]["reason"] =
                    serde_json::Value::String(payload.default_silenced_string());
                // alert["endsAt"] = Utc::now()
                //     .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                //     .into();
            }
        };

        if let Err(e) = client.post(&self.endpoint).json(&alerts).send().await {
            error!("Couldn't make call to alertmanager, error: {}", e)
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Timeout {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default = "Retry::default")]
    pub times: Retry,
    #[serde(skip)]
    pub state: Arc<Mutex<TimeoutState>>,
}

impl Default for Timeout {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            times: Retry::default(),
            state: Arc::<Mutex<TimeoutState>>::default(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TimeoutState {
    pub alert_state: AlertState,
    pub timed_out: bool,
    pub awaiting_resolve: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Auth {
    username: String,
    password: String,
}

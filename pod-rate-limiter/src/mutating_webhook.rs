use actix_web::{post, web, Either, HttpResponse};
use k8s_openapi::api::authentication::v1::UserInfo;
use k8s_openapi::api::core::v1::{Container, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status;
use k8s_openapi::apimachinery::pkg::runtime::RawExtension;
use once_cell::sync::Lazy;
use prometheus::{opts, IntCounterVec, Registry};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

static MUTATE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        opts!("pod_rate_limiter_mutate_total", "Total calls to 'mutate'"),
        &["result"],
    )
    .unwrap()
});

static VALIDATE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        opts!(
            "pod_rate_limiter_validate_total",
            "Total calls to 'validate'"
        ),
        &["result"],
    )
    .unwrap()
});

#[post("/validate")]
pub async fn validate(admission_request: web::Json<Request>) -> HttpResponse {
    trace!(
        request = ?admission_request.0,
        "validate admission request"
    );

    let either = extract_pod(&admission_request.request);

    let pod = match either {
        Either::A(pod) => pod,
        Either::B(response) => return response,
    };

    let id = admission_request.request.uid.as_str();

    if pod.metadata.labels.is_some()
        && pod
            .metadata
            .labels
            .as_ref()
            .unwrap()
            .iter()
            .any(|x| x.0 == "pod-rate-limiter" && x.1 == "enabled")
    {
        let init_pos = pod
            .spec
            .as_ref()
            .unwrap()
            .init_containers
            .as_ref()
            .unwrap()
            .iter()
            .position(|p| p.name == "pod-rate-limiter-init");

        match init_pos {
            Some(i) => {
                if i != 0 {
                    VALIDATE_COUNTER
                        .with_label_values(&["init_container_wrong_position"])
                        .inc();
                } else {
                    // If pos==1, it's in the right place. Next validate the label.
                    if !pod.metadata.labels.is_some()
                        || pod
                            .metadata
                            .labels
                            .as_ref()
                            .unwrap()
                            .iter()
                            .find(|(k, v)| {
                                k.as_str() == "pod-rate-limiter" && v.as_str() == "enabled"
                            })
                            .is_none()
                    {
                        VALIDATE_COUNTER
                            .with_label_values(&["init_label_missing"])
                            .inc();
                    }
                }
            }
            None => VALIDATE_COUNTER
                .with_label_values(&["init_container_missing"])
                .inc(),
        }
    }

    let admission_response = Response::new(AdmissionResponse {
        uid: id.to_string(),
        allowed: true,
        ..Default::default()
    });

    HttpResponse::Ok().json(admission_response)
}

#[post("/mutate")]
pub async fn mutate(admission_request: web::Json<Request>) -> HttpResponse {
    trace!(
        request = ?admission_request.0,
        "mutate admission request"
    );

    let either = extract_pod(&admission_request.request);

    let mut pod = match either {
        Either::A(pod) => pod,
        Either::B(response) => return response,
    };

    let id = admission_request.request.uid.as_str();

    let pod_name = match pod.metadata.name.as_ref() {
        Some(name) => name,
        _ => pod.metadata.generate_name.as_ref().unwrap(),
    };

    // Filter out backend-service since it runs on dedicated hosts
    if let Some(labels) = &pod.metadata.labels {
        if let Some(app) = labels.get("app") {
            if app.starts_with("backend-service") {
                MUTATE_COUNTER.with_label_values(&["skipped"]).inc();
                debug!(
                    id,
                    app = app.as_str(),
                    "skipping app matching opt-out label"
                );
                return allow_without_mutation_response(admission_request);
            }
        }
    }

    if pod.spec.as_ref().unwrap().init_containers.is_none() {
        pod.spec.as_mut().unwrap().init_containers = Some(Vec::with_capacity(1))
    }

    let existing_pod_position = pod
        .spec
        .as_ref()
        .unwrap()
        .init_containers
        .as_ref()
        .unwrap()
        .iter()
        .position(|c| c.name == "pod-rate-limiter-init");

    if existing_pod_position.is_some() {
        if existing_pod_position.unwrap() == 0 {
            trace!(
                id,
                pod = pod_name.as_str(),
                "Pod already has the init container and it's in the first position."
            );
            MUTATE_COUNTER.with_label_values(&["reinvoked_noop"]).inc();
            return allow_without_mutation_response(admission_request);
        } else {
            debug!(
                id,
                pod = pod_name.as_str(),
                "Pod already has the init container, but in the wrong position. Moving to the first slot."
            );

            MUTATE_COUNTER
                .with_label_values(&["reinvoked_move_to_front"])
                .inc();

            let init_containers = pod.spec.as_mut().unwrap().init_containers.as_mut().unwrap();
            let existing_init_container = init_containers.remove(existing_pod_position.unwrap());
            init_containers.insert(0, existing_init_container);
        }
    } else {
        // Create and insert rate limiting init container.
        // Insert at position 0 so that it does not get inserted after networking sidecars like Istio
        // that would end up blocking network traffic (i.e. the call back to this service to check if
        // the pod is released).
        pod.spec
            .as_mut()
            .unwrap()
            .init_containers
            .as_mut()
            .unwrap()
            .insert(0, build_init_container(None));

        // Add label for filtering by the k8s watcher in `controller`
        pod.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("pod-rate-limiter".to_string(), "enabled".to_string());

        debug!(
            id,
            pod = pod_name.as_str(),
            "Injected init container into pod."
        );
        MUTATE_COUNTER.with_label_values(&["mutated"]).inc();
    }

    // generate a patch for the init container if needed
    let patches = json_patch::diff(
        &admission_request.request.object.as_ref().unwrap().0,
        &serde_json::to_value(&pod).unwrap(),
    );

    let admission_response = Response::new(AdmissionResponse {
        uid: id.to_string(),
        allowed: true,
        patch: Some(base64::encode(serde_json::to_string(&patches).unwrap())),
        patch_type: Some("JSONPatch".to_string()),
        ..Default::default()
    });

    trace!(
        id,
        pod = pod_name.as_str(),
        response = ?admission_response,
        "Admission Response."
    );

    HttpResponse::Ok().json(admission_response)
}

fn extract_pod(request: &AdmissionRequest) -> Either<Pod, HttpResponse> {
    if !request.resource.is_some() || !request.resource.as_ref().unwrap().resource.is_some() {
        MUTATE_COUNTER.with_label_values(&["invalid_input"]).inc();
        warn!("Malformed admission request");
        return Either::B(HttpResponse::UnprocessableEntity().finish());
    }

    let id = request.uid.as_str();

    let resource_type = request
        .resource
        .as_ref()
        .unwrap()
        .resource
        .as_ref()
        .unwrap();

    if resource_type != "pods" {
        MUTATE_COUNTER
            .with_label_values(&["invalid_resource"])
            .inc();
        warn!(
            id,
            resource_type = resource_type.as_str(),
            "Invalid resource type"
        );
        return Either::B(HttpResponse::UnprocessableEntity().finish());
    }

    let pod: Pod = serde_json::from_value(request.object.clone().unwrap().0).unwrap();

    return Either::A(pod);
}

fn allow_without_mutation_response(admission_request: web::Json<Request>) -> HttpResponse {
    HttpResponse::Ok().json(Response::new(AdmissionResponse {
        allowed: true,
        uid: admission_request.request.uid.to_string(),
        ..Default::default()
    }))
}

pub fn build_init_container(uri_authority: Option<&str>) -> Container {
    let authority = match uri_authority {
        Some(h) => h,
        None => "pod-rate-limiter.pod-rate-limiter.svc.cluster.local",
    };

    serde_json::from_value(json!({
        "command": ["/bin/sh"],
        "args": [
            "-c",
            format!("start=`date +%s`; elapsed=0; released=-1; until [ $released -eq 0 ] || [ $elapsed -ge 600 ]; do curl -m 5 -f --no-progress-meter http://{}/try_release_pod?pod=$POD_NAME\\&node=$NODE_NAME; released=$?; now=`date +%s`; elapsed=`expr $now - $start`; sleep 2; done; echo \"Released: $released, Elapsed: $elapsed\"", authority)
        ],
        "name": "pod-rate-limiter-init",
        "image": "curlimages/curl",
        "env": [
            {
                "name":"NODE_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath":"spec.nodeName"
                    }
                }
            },
            {
                "name":"POD_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath":"metadata.name"
                    }
                }
            }
        ]
    })).unwrap()
}

pub fn register_metrics(registry: &Registry) {
    registry.register(Box::new(MUTATE_COUNTER.clone())).unwrap();
}

// Admission related structs not defined in k8s_openapi
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupVersionKind {
    group: Option<String>,
    version: Option<String>,
    kind: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupVersionResource {
    group: Option<String>,
    version: Option<String>,
    resource: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionRequest {
    uid: String,
    kind: Option<GroupVersionKind>,
    resource: Option<GroupVersionResource>,
    sub_resource: Option<String>,
    request_kind: Option<GroupVersionKind>,
    request_resource: Option<GroupVersionResource>,
    request_sub_resource: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
    operation: Option<String>,
    user_info: Option<UserInfo>,
    object: Option<RawExtension>,
    old_object: Option<RawExtension>,
    dry_run: Option<bool>,
    options: Option<RawExtension>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    api_version: String,
    kind: String,
    request: AdmissionRequest,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionResponse {
    uid: String,
    allowed: bool,
    status: Option<Status>,
    patch: Option<String>,
    patch_type: Option<String>,
    audit_annotations: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    api_version: String,
    kind: String,
    response: AdmissionResponse,
}

impl Response {
    fn new(admission_response: AdmissionResponse) -> Response {
        Response {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            response: admission_response,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging;
    use actix_web::{test, App};
    use serde_json::{json, Value};
    use std::collections::HashSet;

    #[actix_rt::test]
    async fn test_mutate() -> anyhow::Result<()> {
        logging::init_logging();

        let json = json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "602a6d56-c6b3-4b17-880f-0c8ae0ff3bb2",
                "kind": {
                    "group": "",
                    "version": "v1",
                    "kind": "Pod"
                },
                "resource": {
                    "group": "",
                    "version": "v1",
                    "resource": "pods"
                },
                "requestKind": {
                    "group": "",
                    "version": "v1",
                    "kind": "Pod"
                },
                "requestResource": {
                    "group": "",
                    "version": "v1",
                    "resource": "pods"
                },
                "namespace": "default",
                "operation": "CREATE",
                "object": {
                    "kind": "Pod",
                    "apiVersion": "v1",
                    "metadata": {
                        "name": "pod-rate-limiter-test",
                        "labels": {
                            "app": "my-service"
                        }
                    },
                    "spec": {
                        "initContainers": [{
                            "name": "istio-init",
                            "image": "bash"
                        }],
                        "containers": [{
                            "args": ["-c", "sleep 60"],
                            "name": "istio-proxy",
                            "image": "bash"
                        }, {
                            "args": ["-c", "sleep 60"],
                            "name": "pod-rate-limiter-test",
                            "image": "bash"
                        }],
                    }
                }
            }
        });

        let mut app = test::init_service(App::new().service(mutate)).await;

        let req = test::TestRequest::post()
            .uri("/mutate")
            .set_json(&json)
            .to_request();
        let resp = test::call_service(&mut app, req).await;

        assert_eq!(200, resp.status().as_u16());

        let admission_resp =
            serde_json::from_slice::<Response>(test::read_body(resp).await.as_ref())
                .unwrap()
                .response;

        assert_eq!("JSONPatch".to_string(), admission_resp.patch_type.unwrap());

        let patch = serde_json::from_slice::<Value>(
            base64::decode(admission_resp.patch.unwrap().to_owned())?.as_slice(),
        )
        .unwrap();

        let paths: HashSet<&str> = patch
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o.get("path").unwrap().as_str().unwrap())
            .collect();

        assert!(paths.contains("/metadata/labels/pod-rate-limiter"));
        assert!(paths.contains("/spec/initContainers/0/name"));
        assert!(paths.contains("/spec/initContainers/1"));

        Ok(())
    }
}
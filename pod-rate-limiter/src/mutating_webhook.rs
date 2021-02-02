use actix_web::{post, web, HttpResponse};
use k8s_openapi::api::authentication::v1::UserInfo;
use k8s_openapi::api::core::v1::{Container, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status;
use k8s_openapi::apimachinery::pkg::runtime::RawExtension;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

#[post("/mutate")]
pub async fn mutate(admission_request: web::Json<Request>) -> HttpResponse {
    //println!("{:?}", admission_request);

    if admission_request
        .request
        .resource
        .as_ref()
        .unwrap()
        .resource
        .as_ref()
        .unwrap()
        != "pods"
    {
        // err
    }

    let mut pod: Pod =
        serde_json::from_value(admission_request.request.object.clone().unwrap().0).unwrap();

    // TODO: add a label to the pod to opt out or provide some other more flexible config capability
    // Filter out backend-service since it runs on dedicated hosts
    if let Some(labels) = &pod.metadata.labels {
        if let Some(app) = labels.get("app") {
            if app == "backend-service-job" {
                return HttpResponse::Ok().json(Response::new(AdmissionResponse {
                    allowed: true,
                    uid: admission_request.request.uid.to_string(),
                    ..Default::default()
                }));
            }
        }
    }

    //println!("{:?}", pod);

    if pod.spec.as_ref().unwrap().init_containers == None {
        pod.spec.as_mut().unwrap().init_containers = Some(Vec::with_capacity(1))
    }

    // create rate limiting init container if it doesn't yet exist
    let init_container = if pod
        .spec
        .as_ref()
        .unwrap()
        .init_containers
        .as_ref()
        .unwrap()
        .iter()
        .find(|c| c.name == "pod-rate-limiter-init")
        .is_none()
    {
        Some(build_init_container())
    } else {
        None
    };

    // generate a patch for the init container if needed
    let patches = if init_container.is_some() {
        pod.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("pod-rate-limiter".to_string(), "enabled".to_string());

        pod.spec
            .as_mut()
            .unwrap()
            .init_containers
            .as_mut()
            .unwrap()
            .insert(0, init_container.unwrap());

        let patches = json_patch::diff(
            &admission_request.request.object.as_ref().unwrap().0,
            &serde_json::to_value(&pod).unwrap(),
        );

        // for x in &patches.0 {
        //     println!("Patch: {:?}", x);
        // }

        Some(patches)
    } else {
        None
    };

    let admission_response = Response::new(AdmissionResponse {
        uid: admission_request.request.uid.to_string(),
        allowed: true,
        patch: match &patches {
            Some(p) => Some(base64::encode(serde_json::to_string(&p).unwrap())),
            _ => None,
        },
        patch_type: match &patches {
            Some(_) => Some("JSONPatch".to_string()),
            _ => None,
        },
        ..Default::default()
    });

    HttpResponse::Ok().json(admission_response)
}

fn build_init_container() -> Container {
    serde_json::from_value(json!({
        "command": ["/bin/sh"],
        "args": [
            "-c",
            "start=`date +%s`; elapsed=0; released=-1; until [ $released -eq 0 ] || [ $elapsed -ge 600 ]; do curl -m 5 -f http://pod-rate-limiter.kube-system.svc.cluster.local/is_pod_released?pod=$POD_NAME\\&node=$NODE_NAME; released=$?; now=`date +%s`; elapsed=`expr $now - $start`; sleep 2; done; echo \"Released: $released, Elapsed: $elapsed\""
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

// Admission related structs not defined in k8s_openapi
#[derive(Debug, Deserialize)]
pub struct GroupVersionKind {
    group: Option<String>,
    version: Option<String>,
    kind: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GroupVersionResource {
    group: Option<String>,
    version: Option<String>,
    resource: Option<String>,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
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
    use actix_web::body::Body::Bytes;
    use actix_web::{test, App};
    use serde_json::{json, Value};
    use std::collections::HashSet;

    #[actix_rt::test]
    async fn test_mutate() -> anyhow::Result<()> {
        let mut app = test::init_service(App::new().service(mutate)).await;

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

        let req = test::TestRequest::post()
            .uri("/mutate")
            .set_json(&json)
            .to_request();
        let resp = test::call_service(&mut app, req).await;

        assert_eq!(200, resp.status().as_u16());

        let admission_resp = match resp.response().body().as_ref() {
            Some(Bytes(b)) => serde_json::from_slice::<Response>(b)?,
            _ => panic!(),
        }
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

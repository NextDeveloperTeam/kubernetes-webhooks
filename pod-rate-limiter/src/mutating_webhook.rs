use k8s_openapi::api::core::v1::ObjectFieldSelector;
use k8s_openapi::api::core::v1::EnvVarSource;
use k8s_openapi::api::core::v1::EnvVar;
use actix_web::{post, web, HttpResponse};
use json_patch::PatchOperation::Add;
use k8s_openapi::api::authentication::v1::UserInfo;
use k8s_openapi::api::core::v1::{Container, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status;
use k8s_openapi::apimachinery::pkg::runtime::RawExtension;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[post("/mutate")]
pub async fn mutate(
    admission_request: web::Json<Request>,
) -> HttpResponse {
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
        Some(Container {
            args: Some(vec![
                "-c".to_string(),
                "start=`date +%s`; elapsed=0; released=-1; until [ $released -eq 0 ] || [ $elapsed -ge 300 ]; do curl -m 5 -f http://pod-rate-limiter.kube-system.svc.cluster.local/is_pod_released?pod=$POD_NAME\\&node=$NODE_NAME; released=$?; now=`date +%s`; elapsed=`expr $now - $start`; sleep 1; done".to_string()
            ]),
            command: Some(vec!["/bin/sh".to_string()]),
            env: Some(vec![
                EnvVar {
                    name: "NODE_NAME".to_string(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        config_map_key_ref: None,
                        field_ref: Some(ObjectFieldSelector {
                            api_version: None,
                            field_path: "spec.nodeName".to_string()
                        }),
                        resource_field_ref: None,
                        secret_key_ref: None,
                    })
                },
                EnvVar {
                    name: "POD_NAME".to_string(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        config_map_key_ref: None,
                        field_ref: Some(ObjectFieldSelector {
                            api_version: None,
                            field_path: "metadata.name".to_string()
                        }),
                        resource_field_ref: None,
                        secret_key_ref: None,
                    })
                },
            ]),
            env_from: None,
            image: Some("curlimages/curl".to_string()),
            image_pull_policy: None,
            lifecycle: None,
            liveness_probe: None,
            name: "pod-rate-limiter-init".to_string(),
            ports: None,
            readiness_probe: None,
            resources: None,
            security_context: None,
            startup_probe: None,
            stdin: None,
            stdin_once: None,
            termination_message_path: None,
            termination_message_policy: None,
            tty: None,
            volume_devices: None,
            volume_mounts: None,
            working_dir: None,
        })
    } else {
        None
    };

    // generate a patch for the init container if needed
    let patch = if init_container.is_some() {
        pod.spec
            .as_mut()
            .unwrap()
            .init_containers
            .as_mut()
            .unwrap()
            .push(init_container.unwrap());

        let patches = json_patch::diff(
            &admission_request.request.object.as_ref().unwrap().0,
            &serde_json::to_value(&pod).unwrap(),
        );

        // for x in &patches.0 {
        //     println!("Patch: {:?}", x);
        // }

        patches.0.into_iter().find(|p| match p {
            Add(a) => a.path == "/spec/initContainers",
            _ => false,
        })
    } else {
        None
    };

    let admission_response = Response {
        api_version: "admission.k8s.io/v1".to_owned(),
        kind: "AdmissionReview".to_owned(),
        response: AdmissionResponse {
            uid: admission_request.request.uid.clone(),
            allowed: true,
            status: None,
            patch: match &patch {
                Some(p) => Some(base64::encode(serde_json::to_string(&[p]).unwrap())),
                _ => None,
            },
            patch_type: match &patch {
                Some(_) => Some("JSONPatch".to_string()),
                _ => None,
            },
            audit_annotations: None,
        },
    };

    // pod lacks a name at this point.
    // if admission_response.response.patch.is_some() {
    //     controller.add_pod_pending_assignment(pod.name());
    // }
    // Potential race - the init container may start before we've received & processed the pod added event.
    //   if this happens we'd release the pod too soon... perhaps ensure we've seen a pod event before releasing
    //   (issue w/ this is a restarted instance, though it should reconcile on start) and/or add a 'sleep' in the container
    //   and hope we get the event in time.

    HttpResponse::Ok().json(admission_response)
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionResponse {
    uid: String,
    allowed: bool,
    status: Option<Status>,
    patch: Option<String>,
    patch_type: Option<String>,
    audit_annotations: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    api_version: String,
    kind: String,
    response: AdmissionResponse,
}

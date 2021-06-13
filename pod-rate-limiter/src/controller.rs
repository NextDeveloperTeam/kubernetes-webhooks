use actix_web::dev::Payload;
use actix_web::http::{Error, StatusCode};
use actix_web::{get, web, FromRequest, HttpRequest, HttpResponse, Responder};
use futures::future::{ok, Ready};
use futures::prelude::*;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::chrono;
use kube::api::{Api, ListParams, Meta};
use kube_runtime::reflector::Store;
use kube_runtime::watcher::Event;
use kube_runtime::{reflector, watcher};
use once_cell::sync::Lazy;
use prometheus::{opts, IntCounterVec, IntGaugeVec, Registry};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

#[derive(Debug, Clone)]
struct PodState {
    pods: Vec<Pod>,
    released_pod: Option<String>,
}

#[derive(Debug, Clone)]
struct RateLimitingControllerData {
    nodes_to_pods: HashMap<String, PodState>,
}

#[derive(Clone)]
pub struct RateLimitingController {
    data: Arc<Mutex<RateLimitingControllerData>>,
    pods_api: Api<Pod>,
}

#[derive(Deserialize, Debug)]
pub struct PodReleasedQuery {
    node: String,
    pod: String,
}

enum PodEvent {
    Applied(Pod),
    Deleted(Pod),
}

// Metrics
static PENDING_PODS_GAUGE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        opts!(
            "pod_rate_limiter_pending_pods",
            "Count of starting pods by node"
        ),
        &["node"],
    )
    .unwrap()
});

static PROCESSED_POD_EVENT_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        opts!(
            "pod_rate_limiter_processed_pod_event_total",
            "Total processed pods events"
        ),
        &["result"],
    )
    .unwrap()
});

static RELEASED_PODS_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        opts!(
            "pod_rate_limiter_released_pods_total",
            "Released pod counter by node"
        ),
        &["node"],
    )
    .unwrap()
});

#[get("/try_release_pod")]
pub async fn try_release_pod(
    controller: RateLimitingController,
    query: web::Query<PodReleasedQuery>,
) -> impl Responder {
    if controller.try_release_pod(query.node.as_str(), query.pod.as_str()) {
        HttpResponse::new(StatusCode::OK)
    } else {
        HttpResponse::new(StatusCode::LOCKED)
    }
}

impl RateLimitingController {
    pub async fn new(registry: &Registry) -> Self {
        registry
            .register(Box::new(PENDING_PODS_GAUGE.clone()))
            .unwrap();
        registry
            .register(Box::new(PROCESSED_POD_EVENT_COUNTER.clone()))
            .unwrap();
        registry
            .register(Box::new(RELEASED_PODS_COUNTER.clone()))
            .unwrap();

        let client = kube::Client::try_default().await.expect("create client");
        let pods = Api::<Pod>::all(client.clone());

        Self {
            data: Arc::new(Mutex::new(RateLimitingControllerData {
                nodes_to_pods: HashMap::new(),
            })),
            pods_api: pods,
        }
    }

    pub async fn run(&self) {
        let pod_store = reflector::store::Writer::<Pod>::default();
        let pod_store_reader = pod_store.as_reader();
        let pod_reflector = reflector(
            pod_store,
            watcher(
                self.pods_api.clone(),
                ListParams::default().labels("pod-rate-limiter=enabled"),
            ),
        );

        info!("Starting reconciler");
        self.spawn_reconciler(pod_store_reader);

        let mut pod_stream = pod_reflector
            .map_ok(|event| {
                stream::iter(
                    match event {
                        Event::Applied(obj) => vec![PodEvent::Applied(obj)],
                        Event::Deleted(obj) => vec![PodEvent::Deleted(obj)],
                        Event::Restarted(objs) => {
                            // We don't care that the stream restarted, so map & flatten 'Restarted' into regular 'Applied' events.
                            objs.into_iter().map(|o| PodEvent::Applied(o)).collect()
                        }
                    }
                    .into_iter()
                    .map(Result::<PodEvent, watcher::Error>::Ok),
                )
            })
            .try_flatten()
            .boxed();

        info!("Starting event processing loop");

        loop {
            match pod_stream.try_next().await {
                Ok(Some(event)) => self.process_pod(event),
                Err(e) => {
                    error!("Processing loop error. {:?}", e);
                    tokio::time::delay_for(Duration::from_secs(15)).await;
                }
                Ok(None) => {
                    // TODO: if the stream is restarted do we get a None or just Restarted events?
                    warn!("Event stream ended or graceful shutdown.");
                    break;
                }
            }
        }
    }

    fn spawn_reconciler(&self, pod_store: Store<Pod>) {
        let this = self.clone();

        tokio::spawn(async move {
            loop {
                debug!("Reconciling state");

                // Best effort, "good enough" sort.
                let mut pod_state = pod_store.state();
                pod_state.sort_by(|a, b| {
                    a.metadata
                        .creation_timestamp
                        .as_ref()
                        .unwrap()
                        .cmp(&b.metadata.creation_timestamp.as_ref().unwrap())
                });

                let node_names: HashSet<String> = pod_state
                    .iter()
                    .map(|p| p.spec.as_ref().unwrap().node_name.as_ref().unwrap())
                    .cloned()
                    .collect();

                // ensure current state includes all items from the pod store
                for pod in pod_state {
                    this.process_pod(PodEvent::Applied(pod));
                }

                // create a scope around the mutexguard as to drop it prior to the sleep
                {
                    let mut data = this.data.lock().unwrap();

                    // trace!(?node_names, nodes_to_pods = ?data.nodes_to_pods, "Reconciling pods across nodes");

                    // clear any nodes in `nodes_to_pods` that no longer exist
                    for node_name in data.nodes_to_pods.keys() {
                        if !node_names.contains(node_name) {
                            PENDING_PODS_GAUGE
                                .remove_label_values(&[node_name])
                                .unwrap();
                        }
                    }
                    data.nodes_to_pods.retain(|k, _| node_names.contains(k));

                    // clear any pods in `nodes_to_pods` that no longer exist
                    for node in node_names {
                        if let Some(p) = data.nodes_to_pods.get_mut(node.as_str()) {
                            let pods_on_node: HashSet<String> = pod_store
                                .state()
                                .iter()
                                .filter(|p| match &p.spec.as_ref().unwrap().node_name {
                                    Some(n) => *n == node,
                                    _ => false,
                                })
                                .map(|p| Meta::name(p))
                                .collect();

                            trace!(node = node.as_str(), ?pods_on_node, "Pods on node");

                            p.pods
                                .retain(|p| pods_on_node.contains(Meta::name(p).as_str()));

                            // If the currently released pod vanished, clear it.
                            if !pods_on_node
                                .contains(p.released_pod.as_ref().unwrap_or(&"".to_string()))
                            {
                                p.released_pod = None;
                            }
                        }
                    }
                }

                let delay = 60;
                debug!("Done reconciling. Sleeping for {} seconds.", delay);
                tokio::time::delay_for(Duration::from_secs(delay)).await
            }
        });
    }

    pub fn try_release_pod(&self, node_name: &str, pod_name: &str) -> bool {
        // High level pod states we deal with:
        // 1. Blocked
        // 2. Starting
        // 3. Started (i.e. typically 'Ready', exceptions including hot standbys that do not return 'true' to their readiness probes.)
        // 4. Completed
        //
        // States typically flow in sequence. Exceptions include a pod moving directly to completed (ex: init container failure)
        // or back to pending (ex: container restart w/ backoff).
        //
        // Pods are entered into the `starting_pods` list in order of processing and not necessarily ordered by `creation_timestamp`, though some attempt is made in the reconciler.
        //
        // To determine if a pod is released:
        // 1. Filter out `blocked`, 'started', and `completed` pods. For the purposes of rate limiting these don't matter.
        // 2. This leaves us with only `starting` pods. If the first pod in this list matches that of the request, the pod is released. Otherwise it must wait.
        //    Notes: This allows for non-rate limited pods to block rate-limited pods. We may decide later to reverse this or make it conditional.
        //           This allows only a single rate limited pod to start, per node. We may decide later to add more complexity to allow for other policies
        //             Ex: Release pods such that `sum(starting.resources.cpu.limits) < (50% node CPU capacity)`.
        //
        // Exception case: if a pod is `Started` and later a container's liveness probe fails, it will move back to `blocked` with a restart backoff
        // and then to `Starting`. This causes rate limiting gaps since its rate limiting init container, if any, does _NOT_ re-execute. Further, the sequence
        // of this pod is likely to appear before any other pods already in the starting state. Under these conditions a rate limited, released, and starting pod
        // that is consuming heavy compute will now _also_ be competing with the restarted pod.  There's little we can do to handle this other than deleting
        // the pod to force it to re-queue. The downside of deleting the pod is that it resets the exponential backoff logic and would lead to the
        // pod thrashing CPU cycles, etc. as it continues to rapidly restart over and over, and it also breaks alerts configured pod restart counts & the like.
        // We could also attempt to reimplement our own backoff logic here, since we have some control over the pod starting, but boy that's a huge amount
        // of complexity and still does not address breaking of any alerts nor deviating from expected k8s behaviours.
        // So... we choose the lesser of two evils here as the cure is much worse than the unlikely symptom.

        debug!(
            node = node_name,
            pod = pod_name,
            "try_release_pod: Checking if pod released"
        );

        let mut data = self.data.lock().unwrap();

        if !data.nodes_to_pods.contains_key(node_name) {
            debug!("try_release_pod: pod not found");
            return false;
        }

        let pod_state = data.nodes_to_pods.get_mut(node_name).unwrap();

        if pod_state.released_pod.is_some() && pod_state.released_pod.as_ref().unwrap() == pod_name
        {
            debug!(
                pod_name,
                released_pod = pod_state.released_pod.as_ref().unwrap().as_str(),
                "try_release_pod: A pod is already released"
            );
            return true;
        }

        let two_minutes_ago = Time(chrono::Utc::now() - chrono::Duration::seconds(120));

        let starting_pods = pod_state
            .pods
            .iter()
            .filter(|pod| {
                //trace!(?pod, pod_name, "Processing pod");

                // Find `Starting` pods by filtering out blocked, not ready, and not completed pods.

                let status = pod.status.as_ref().unwrap();

                let mut started = false;
                let mut initialized = false;

                // Is this pod ready & started?
                if let Some(conditions) = &status.conditions {
                    for condition in conditions {
                        if condition.type_ == "ContainersReady" && condition.status == "True" {
                            trace!(started, "try_release_pod: started");
                            started = true;
                        }
                        if condition.type_ == "Initialized" && condition.status == "True" {
                            trace!(initialized, "try_release_pod: initialized");
                            initialized = true;
                        }
                    }
                }

                // If the pod level 'ContainersReady'' is not true, there's additional states that we still consider
                // as 'started' and must do additional inspection of the state to detect these cases.
                if !started {
                    // If ALL containers are NOT ready, the pod may still be considered "started".
                    // Examples:
                    // 1) One pod is 'Running' & 'Ready', while another pod has terminated.
                    //    (e.g. some init-like pod that isn't in an init container or
                    //      a cronjob whose Istio sidecar was not killed... this is a separate issue when using Istio that needs fixing in the job itself, but we should not let that block pods from starting).
                    // 2) A hot-standby pod that is 'Running' but NOT 'Ready'. If it's been 'Running' for more than X minutes, consider it a hot-standby and started.

                    if let Some(statuses) = &status.container_statuses {
                        // Check if all containers appear 'started' according to the exception rules
                        started = statuses.iter().all(|c| {
                            let mut is_hot_standby_pod = false;

                            if c.state.is_some() && c.state.as_ref().unwrap().running.is_some() {
                                let running = c.state.as_ref().unwrap().running.as_ref().unwrap();

                                is_hot_standby_pod = running.started_at.is_some()
                                    && running.started_at.as_ref().unwrap().le(&two_minutes_ago);
                            }

                            let terminated =
                                c.state.is_some() && c.state.as_ref().unwrap().terminated.is_some();

                            trace!(
                                name = c.name.as_str(),
                                is_hot_standby_pod,
                                ready = c.ready,
                                terminated,
                                "try_release_pod: not started - container status"
                            );

                            // The container is 'started' if we're a 'hot standby', 'ready', or 'terminated'
                            is_hot_standby_pod || c.ready || terminated
                        });

                        trace!(started, "try_release_pod: started by container status?")
                    }
                }

                // if let Some(<TDB>) = status {
                //     // TODO: handle pods w/ startup probes. If startup probe status is "true/ready" also set 'running=true'
                // }

                // Is this pod completed?
                let completed = match status.phase.as_ref().unwrap().as_str() {
                    "Failed" | "Succeeded" | "Unknown" => true,
                    _ => false,
                };

                trace!(
                    phase = status.phase.as_ref().unwrap().as_str(),
                    completed,
                    "try_release_pod: completed?"
                );

                // Is this pod 'waiting' and blocked by a non-PodInitializing reason
                // (ErrImagePull, ImagePullBackOff, CrashLoopBackOff, etc.)?
                let container_statuses = if initialized && status.container_statuses.is_some() {
                    &status.container_statuses
                } else if status.init_container_statuses.is_some() {
                    &status.init_container_statuses
                } else {
                    &None
                };

                let blocked = match container_statuses {
                    None => false,
                    Some(cs) => cs.iter().any(|s| {
                        if s.state.is_some() && s.state.as_ref().unwrap().waiting.is_some() {
                            let waiting = s.state.as_ref().unwrap().waiting.as_ref().unwrap();

                            match waiting.reason.as_deref() {
                                Some("PodInitializing") => false,
                                None | Some(_) => true,
                            }
                        } else {
                            false
                        }
                    }),
                };

                debug!(
                    completed,
                    blocked,
                    started,
                    pod_name = Meta::name(*pod).as_str(),
                    "try_release_pod: filter flags"
                );
                return !completed && !blocked && !started;
            })
            .collect::<Vec<&Pod>>();

        trace!(
            starting_pods = ?starting_pods.iter().map(|p| Meta::name(*p)).collect::<Vec<String>>(),
            "try_release_pod: starting pods"
        );

        // The requested pod is NOT in the first slot. It must wait.
        if starting_pods.len() == 0 || Meta::name(starting_pods[0]) != pod_name {
            return false;
        }

        // The requested pod IS in the first slot. It may proceed.
        pod_state.released_pod = Some(pod_name.to_string());
        RELEASED_PODS_COUNTER.with_label_values(&[node_name]).inc();
        return true;
    }

    fn insert_pod(&self, pod: Pod) {
        PROCESSED_POD_EVENT_COUNTER
            .with_label_values(&["insert_pod"])
            .inc();

        let pod_name = Meta::name(&pod);
        let node = pod.spec.as_ref().unwrap().node_name.as_ref().unwrap();

        debug!(
            pod = pod_name.as_str(),
            node = node.as_str(),
            "Upserting pod"
        );

        let mut data = self.data.lock().unwrap();

        let pod_state = &mut data
            .nodes_to_pods
            .entry(node.to_string())
            .or_insert_with(|| PodState {
                pods: Vec::default(),
                released_pod: None,
            });

        // Check if this pod is the current 'released_pod' and clear if it's now 'Ready'
        if pod.status.is_some()
            && pod_state.released_pod.as_ref().unwrap_or(&"".to_string()) == pod_name.as_str()
        {
            if let Some(conditions) = &pod.status.as_ref().unwrap().conditions {
                for condition in conditions {
                    if condition.type_ == "ContainersReady" && condition.status == "True" {
                        pod_state.released_pod = None;
                        break;
                    }
                }
            }
        }

        let pods = &mut pod_state.pods;

        // If the pod already exists in the queue, update with the latest state so that we
        // do not reset its position (unless it has restarted).  Otherwise add the pod to the end of the queue.
        match pods.iter().position(|p| Meta::name(p) == pod_name) {
            None => {
                trace!(?pod, "Upserting as new pod");
                pods.push(pod)
            }
            Some(i) => {
                let stored_pod = &pods[i];

                let build_restart_map = |p: &Pod| {
                    p.status
                        .as_ref()
                        .unwrap()
                        .container_statuses
                        .as_ref()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|s| (s.name.clone(), s.restart_count))
                        .collect::<HashMap<_, _>>()
                };

                let stored_statuses = build_restart_map(stored_pod);
                let new_statuses = build_restart_map(&pod);

                let restarted = stored_statuses
                    .iter()
                    .any(|i| new_statuses.get(i.0).unwrap() != i.1);

                let patch = || {
                    let stored_json = serde_json::to_value(stored_pod).unwrap();
                    let new_json = serde_json::to_value(&pod).unwrap();

                    format!("{:?}", json_patch::diff(&stored_json, &new_json).0)
                };

                trace!(?restarted, patch = patch().as_str(), "Upsert diff");

                if restarted {
                    if pod_state.released_pod.is_some()
                        && pod_state.released_pod.as_ref().unwrap() == &Meta::name(&pod)
                    {
                        pod_state.released_pod = None;
                        trace!(
                            pod_name = Meta::name(&pod).as_str(),
                            "Clearing restarted pod"
                        );
                    }

                    pods.remove(i);
                    pods.push(pod);
                } else {
                    pods[i] = pod
                }
            }
        }
    }

    fn remove_pod(&self, pod: &Pod) {
        PROCESSED_POD_EVENT_COUNTER
            .with_label_values(&["remove_pod"])
            .inc();

        trace!(
            pod = Meta::name(pod).as_str(),
            "Removing pod from internal state"
        );

        let pod_name = Meta::name(pod);

        if let Some(node) = pod.spec.as_ref().unwrap().node_name.as_ref() {
            let mut data = self.data.lock().unwrap();

            if let Some(pod_state) = data.nodes_to_pods.get_mut(node.as_str()) {
                let pods = &mut pod_state.pods;
                let len = pods.len();

                pods.retain(|p| Meta::name(p) != pod_name);

                if len != pods.len() {
                    debug!(
                        node = node.as_str(),
                        pod = Meta::name(pod).as_str(),
                        "Removed pod from node list"
                    );
                }

                if pod_state.released_pod.as_ref().unwrap_or(&"".to_string()) == &Meta::name(pod) {
                    pod_state.released_pod = None;
                }
            }
        }
    }

    fn process_pod(&self, pod_event: PodEvent) {
        {
            let pod = match &pod_event {
                PodEvent::Applied(p) | PodEvent::Deleted(p) => p,
            };

            if pod.spec.as_ref().unwrap().node_name.is_none() {
                PROCESSED_POD_EVENT_COUNTER
                    .with_label_values(&["unassigned_node"])
                    .inc();
                trace!(
                    pod = Meta::name(pod).as_str(),
                    "Pod not yet assigned to a node. Skipping"
                );
                return;
            }
        }

        match pod_event {
            PodEvent::Applied(p) => self.insert_pod(p),
            PodEvent::Deleted(p) => self.remove_pod(&p),
        };

        let data = self.data.lock().unwrap();

        for (node, pods) in &data.nodes_to_pods {
            PENDING_PODS_GAUGE
                .with_label_values(&[node])
                .set(pods.pods.len() as i64);
        }
    }

    pub async fn live(&self) -> bool {
        // ensure the mutex is not poisoned and that it's not blocked.
        let this = self.clone();
        let handle = tokio::spawn(async move {
            let _ = this.data.lock();
        });

        match tokio::time::timeout(Duration::from_secs(10), handle).await {
            Ok(_) => true,
            _ => false,
        }
    }
}

impl FromRequest for RateLimitingController {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;
    type Config = ();

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ok(req.app_data::<RateLimitingController>().unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::controller::RateLimitingController;
    use crate::logging;
    use actix_rt::System;
    use actix_web::dev::Server;
    use actix_web::{middleware, App, HttpServer};
    use anyhow::anyhow;
    use futures::{StreamExt, TryStreamExt};
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::{DeleteParams, ListParams, Meta, PostParams, WatchEvent};
    use kube::Api;
    use prometheus::Registry;
    use serde_json::json;
    use serial_test::serial;
    use std::sync::mpsc;
    use std::sync::mpsc::{Receiver, Sender};
    use std::thread;
    use tokio::time;
    #[allow(unused_imports)]
    use tracing::{debug, error, info, trace, warn};

    /// Test Summary
    ///
    /// Create three identical pods, ensure that no more than one is starting at any given time
    /// and that, after a pod becomes 'ready', the next pod in line is 'released' for its startup
    /// phase.
    #[actix_rt::test]
    #[serial]
    async fn test_try_release() -> anyhow::Result<()> {
        let authority = get_authority();

        let (registry, server_rx) = init().await;

        let client = kube::Client::try_default().await.expect("create client");
        let pods_api = Api::<Pod>::namespaced(client.clone(), "default");

        let mut pod_template: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod-rate-limiter-test",
                "labels": {
                    "pod-rate-limiter": "enabled"
                }
            },
            "spec": {
                "initContainers": [{
                    "name": "pod-rate-limiter-sleep-helper",
                    "image": "bash"
                },
                    crate::mutating_webhook::build_init_container(Some(authority.as_str()))
                ],
                "containers": [{
                    "args": ["-c", "sleep 60"],
                    "name": "pod-rate-limiter-test",
                    "image": "bash"
                }],
            }
        }))?;

        let pod_names: Vec<String> = (1..=3).map(|i| format!("pod-rate-limiter-{}", i)).collect();
        reset_pods(&pod_names).await?;

        let mut sleep = 10;
        let pp = PostParams::default();

        for pod_name in &pod_names {
            pod_template.metadata.name = Some(pod_name.to_string());
            pod_template
                .spec
                .as_mut()
                .unwrap()
                .init_containers
                .as_mut()
                .unwrap()
                .get_mut(0)
                .unwrap()
                .args = Some(vec!["-c".to_string(), format!("sleep {}", sleep)]);
            sleep += 5;

            match pods_api.create(&&pp, &pod_template).await {
                Err(e) => assert!(false, "pod creation failed {:?}", e),
                _ => (),
            };

            // Wait for this pod's init container to start running. This ensures the pods start and
            // are queued up in sequence.
            wait_for_running(pod_name, true, pods_api.clone()).await?;

            // debug!(?pod_template, "pod template");
        }

        let released_total = || {
            let released_metric = registry
                .gather()
                .into_iter()
                .find(|m| m.get_name() == "pod_rate_limiter_released_pods_total");
            match released_metric {
                Some(p) => p.get_metric()[0].get_counter().get_value(),
                _ => 0 as f64,
            }
        };

        let released_total_before = released_total();

        for pod_name in &pod_names {
            assert_released(pod_name, &pod_names, &authority).await;
            wait_for_running(pod_name, false, pods_api.clone()).await?;
        }

        let released_total_after = released_total();

        assert_eq!(3 as f64, released_total_after - released_total_before);

        reset_pods(&pod_names).await?;

        // TODO: Let's find a way to ensure that the server is stopped. If it _isn't_ stopped then the server will likely
        // fail to start on a subsequent test, as the 'leaked' server is bound to the common http server port.
        let server = server_rx.recv().unwrap();
        server.stop(true).await;

        Ok(())
    }

    /// Test Summary
    ///
    /// Simulate the case where a 'hot standby' pod is running that never enters the full 'ready' state.
    /// To do so, we create two pods. Pod #1 is the hot standby and blocks Pod #2 from starting until
    /// the 'timeout' threshold is reached, after which Pod #2 is 'released'.
    #[actix_rt::test]
    #[serial]
    async fn test_try_release_with_hot_standby() -> anyhow::Result<()> {
        let authority = get_authority();

        let (registry, server_rx) = init().await;

        let client = kube::Client::try_default().await.expect("create client");
        let pods_api = Api::<Pod>::namespaced(client.clone(), "default");

        let hot_standby_pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod-rate-limiter-hot-standby",
                "labels": {
                    "pod-rate-limiter": "enabled"
                }
            },
            "spec": {
                "containers": [{
                    // Just sleep to simulate the hot-standby not being 'ready'
                    "args": ["-c", "sleep 180"],
                    "name": "pod-rate-limiter-hot-standby",
                    "image": "bash",
                    "readinessProbe": {
                        "httpGet": {
                            "path": "/actuator/readiness",
                            "port": 80
                        },
                        "initialDelaySeconds": 180
                    }
                }],
            }
        }))?;

        let pod_2: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod-rate-limiter-pod-2",
                "labels": {
                    "pod-rate-limiter": "enabled"
                }
            },
            "spec": {
                "initContainers": [crate::mutating_webhook::build_init_container(Some(authority.as_str()))],
                "containers": [{
                    "args": ["-c", "sleep 10"],
                    "name": "pod-rate-limiter-pod-2",
                    "image": "bash"
                }],
            }
        }))?;

        let pod_names = vec![Meta::name(&hot_standby_pod), Meta::name(&pod_2)];

        reset_pods(&pod_names).await?;

        let pp = PostParams::default();

        // Start the pods
        match pods_api.create(&&pp, &hot_standby_pod).await {
            Err(e) => assert!(false, "pod creation failed {:?}", e),
            _ => (),
        };

        // Wait for conditions
        wait_for_running(&hot_standby_pod.name(), false, pods_api.clone()).await?;
        match pods_api.create(&&pp, &pod_2).await {
            Err(e) => assert!(false, "pod creation failed {:?}", e),
            _ => (),
        };

        assert_released(&hot_standby_pod.name(), &pod_names, &authority).await;

        // Wait 60 seconds. The hot standby should be the only one released
        time::delay_for(time::Duration::from_secs(60)).await;
        assert_released(&hot_standby_pod.name(), &pod_names, &authority).await;

        // Wait 90 seconds. The standby AND pod-2 should be released at this point (2 minute timeout, total sleep => 60+90=150 seconds).  TODO: make this customizable so we don't just sit here sleeping
        time::delay_for(time::Duration::from_secs(90)).await;

        let released_metric = registry
            .gather()
            .into_iter()
            .find(|m| m.get_name() == "pod_rate_limiter_released_pods_total");

        match released_metric {
            Some(p) => assert_eq!(2 as f64, p.get_metric()[0].get_counter().get_value()),
            _ => assert!(false),
        }

        reset_pods(&pod_names).await?;

        let server = server_rx.recv().unwrap();
        server.stop(true).await;

        Ok(())
    }

    /// Test Summary
    ///
    /// Simulate a terminating pod with a hanging sidecar. Similar to a 'hot standby' that
    /// is not 'ready', but is actually in a state that should _not_ block a new pod from starting,
    /// a pod with a mix of ready terminated and ready container states should be considered
    /// 'started' and not block new pods from starting.
    #[actix_rt::test]
    #[serial]
    async fn test_try_release_with_hung_sidecar() -> anyhow::Result<()> {
        let authority = get_authority();

        let (registry, server_rx) = init().await;

        let client = kube::Client::try_default().await.expect("create client");
        let pods_api = Api::<Pod>::namespaced(client.clone(), "default");

        let hanging_pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod-rate-limiter-hanging",
                "labels": {
                    "pod-rate-limiter": "enabled"
                }
            },
            "spec": {
                "initContainers": [crate::mutating_webhook::build_init_container(Some(authority.as_str()))],
                "containers": [{
                    // Sleep to simulate the hanging sidecar pod
                    "args": ["-c", "sleep 180"],
                    "name": "pod-rate-limiter-hanging",
                    "image": "bash",
                    "readinessProbe": {
                        "httpGet": {
                            "path": "/actuator/readiness",
                            "port": 80
                        },
                        "initialDelaySeconds": 180
                    }
                }, {
                    // Main service pod that exists cleanly
                    "args": ["-c", "sleep 15"],
                    "name": "pod-rate-limiter-sidecar",
                    "image": "bash"
                }],
            }
        }))?;

        let pod_2: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod-rate-limiter-pod-2",
                "labels": {
                    "pod-rate-limiter": "enabled"
                }
            },
            "spec": {
                "initContainers": [crate::mutating_webhook::build_init_container(Some(authority.as_str()))],
                "containers": [{
                    "args": ["-c", "sleep 60"],
                    "name": "pod-rate-limiter-test",
                    "image": "bash"
                }],
            }
        }))?;

        let pod_names = vec![Meta::name(&hanging_pod), Meta::name(&pod_2)];

        reset_pods(&pod_names).await?;

        let pp = PostParams::default();

        // Start the pods
        match pods_api.create(&&pp, &hanging_pod).await {
            Err(e) => assert!(false, "pod creation failed {:?}", e),
            _ => (),
        };

        wait_for_running(&hanging_pod.name(), false, pods_api.clone()).await?;

        match pods_api.create(&&pp, &pod_2).await {
            Err(e) => assert!(false, "pod creation failed {:?}", e),
            _ => (),
        };

        // Initially only the hanging pod should be running
        assert_released(&hanging_pod.name(), &pod_names, &authority).await;

        // Wait 10 seconds. The hanging pod should be the only one released
        time::delay_for(time::Duration::from_secs(10)).await;
        assert_released(&hanging_pod.name(), &pod_names, &authority).await;

        // Wait 10 more seconds. The second pod should now be released and running
        time::delay_for(time::Duration::from_secs(10)).await;

        let released_metric = registry
            .gather()
            .into_iter()
            .find(|m| m.get_name() == "pod_rate_limiter_released_pods_total");

        match released_metric {
            Some(p) => assert_eq!(2 as f64, p.get_metric()[0].get_counter().get_value()),
            _ => assert!(false),
        }

        reset_pods(&pod_names).await?;

        let server = server_rx.recv().unwrap();
        server.stop(true).await;

        Ok(())
    }

    async fn init() -> (Registry, Receiver<Server>) {
        logging::init_logging();

        let registry = prometheus::Registry::default();
        let controller = RateLimitingController::new(&registry).await;

        // Start the controller's logic and web server
        let controller_runner = controller.clone();
        tokio::spawn(async move { controller_runner.run().await });

        let (tx, rx) = mpsc::channel();

        start_test_server(&controller, tx);

        return (registry, rx);
    }

    fn start_test_server(controller: &RateLimitingController, tx: Sender<Server>) {
        let controller_clone = controller.clone();
        thread::spawn(move || {
            let sys = System::new("http-server");

            let server = HttpServer::new(move || {
                App::new()
                    .app_data(controller_clone.clone())
                    .wrap(middleware::Logger::default())
                    .service(crate::controller::try_release_pod)
            })
            .bind("0.0.0.0:8080")
            .expect("Can not bind to 0.0.0.0:8080")
            .run();

            // From actix docs - use a mpsc channel to pass the created server to the "main" test thread
            let _ = tx.send(server);
            sys.run().unwrap();
        });
    }

    fn get_authority() -> String {
        let output = std::process::Command::new("/bin/bash")
            .arg("-c")
            .arg("minikube ssh grep host.minikube.internal /etc/hosts | cut -f1")
            .output()
            .unwrap();

        let ip = String::from_utf8(output.stdout).unwrap().trim().to_string();

        return format!("{}:8080", ip);
    }

    async fn wait_for_running(
        pod_name: &str,
        wait_for_init: bool,
        pods_api: Api<Pod>,
    ) -> anyhow::Result<()> {
        debug!(pod_name, wait_for_init, "Waiting for running");

        let lp = ListParams::default()
            .fields(&format!("metadata.name={}", pod_name))
            .timeout(20);
        let mut stream = pods_api.watch(&lp, "0").await?.boxed();

        let mut running = false;

        while let Some(status) = stream.try_next().await? {
            match status {
                WatchEvent::Modified(o) => {
                    let status = o.status.as_ref().expect("status exists on pod");
                    running = if wait_for_init {
                        if status.init_container_statuses.is_some() {
                            status
                                .init_container_statuses
                                .as_ref()
                                .unwrap()
                                .iter()
                                .any(|c| c.state.as_ref().unwrap().running.is_some())
                        } else {
                            false
                        }
                    } else {
                        status.phase.clone().unwrap_or_default() == "Running"
                    };

                    if running {
                        break;
                    }
                }
                _ => {}
            }
        }

        match running {
            true => {
                debug!("Pod is ready");
                Ok(())
            }
            false => Err(anyhow!("Pod not ready: {}", pod_name)),
        }
    }

    async fn assert_released(
        expected_released_pod: &str,
        pod_names: &Vec<String>,
        server_authority: &str,
    ) {
        debug!(expected_released_pod, "Verifying release status");

        let mut expected_pod_released = false;
        let mut total_released = 0;

        let client = reqwest::Client::new();

        for pod_name in pod_names {
            let resp = client
                .get(format!("http://{}/try_release_pod", server_authority).as_str())
                .query(&[("node", "minikube"), ("pod", pod_name)])
                .send()
                .await
                .unwrap();

            if resp.status() == 200 {
                total_released += 1;

                if pod_name == expected_released_pod {
                    expected_pod_released = true;
                }
            }
            debug!(
                pod_name = pod_name.as_str(),
                total_released, expected_pod_released, "Release status"
            );
        }

        assert_eq!(1, total_released);
        assert!(expected_pod_released);
    }

    async fn reset_pods(pod_names: &Vec<String>) -> anyhow::Result<()> {
        let client = kube::Client::try_default().await.expect("create client");
        let pods = Api::<Pod>::namespaced(client.clone(), "default");

        let dp = DeleteParams {
            grace_period_seconds: Some(0),
            ..Default::default()
        };
        let lp = ListParams::default();

        for pod in pods.list(&lp).await? {
            let pod_name = Meta::name(&pod);

            if pod_names.contains(&pod_name) {
                pods.delete(pod_name.as_str(), &dp).await?;
            } else {
                // FAIL if any other pods are running... this is a safety & sanity check (other pods may conflict with expectations of a given test).
                return Err(anyhow!("Found unknown running pod: {}", pod_name));
            }
        }

        Ok(())
    }
}

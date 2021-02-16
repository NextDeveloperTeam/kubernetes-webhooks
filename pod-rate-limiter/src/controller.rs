use actix_web::dev::Payload;
use actix_web::http::Error;
use actix_web::{FromRequest, HttpRequest};
use futures::future::{ok, Ready};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, Meta};
use kube_runtime::reflector::Store;
use kube_runtime::utils::try_flatten_touched;
use kube_runtime::{reflector, watcher};
use once_cell::sync::Lazy;
use prometheus::{opts, IntCounterVec, IntGaugeVec, Registry};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

// TODO: Rewrite docs how this now works

#[derive(Debug, Clone)]
struct RateLimitingControllerData {
    nodes_to_pods: HashMap<String, Vec<Pod>>,
}

#[derive(Clone)]
pub struct RateLimitingController {
    data: Arc<Mutex<RateLimitingControllerData>>,
    pods_api: Api<Pod>,
}

// Metrics
static PENDING_PODS_GAUGE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        opts!(
            "pod_rate_limiter_pending_pods",
            "Count of pending pods by node"
        ),
        &["node"],
    )
    .unwrap()
});

static PROCESSED_PODS_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        opts!(
            "pod_rate_limiter_processed_pods_total",
            "Total processed pods"
        ),
        &["result"],
    )
    .unwrap()
});

impl RateLimitingController {
    pub async fn new(registry: &Registry) -> Self {
        registry
            .register(Box::new(PENDING_PODS_GAUGE.clone()))
            .unwrap();
        registry
            .register(Box::new(PROCESSED_PODS_COUNTER.clone()))
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

        let mut pod_stream = try_flatten_touched(pod_reflector).boxed();

        info!("Starting event processing loop");

        loop {
            match pod_stream.try_next().await {
                Ok(Some(pod)) => self.process_pod(pod),
                // some sort of Err() or Ok(None).  Log and sleep? Will the stream recover? -> yes
                Err(e) => {
                    error!("Processing loop error. {:?}", e);
                    tokio::time::delay_for(Duration::from_secs(15)).await;
                }
                Ok(None) => {
                    // TODO: if the stream is restarted do we get a None or just the Restarted (which is filtered by 'try_flatted_touched`)
                    warn!("Event stream ended or graceful shutdown.");
                    break;
                }
            }
        }
    }

    fn spawn_reconciler(&self, pod_store: Store<Pod>) {
        let this = self.clone();

        tokio::spawn(async move {
            let delay = 60;
            tokio::time::delay_for(Duration::from_secs(delay)).await;

            loop {
                debug!("Reconciling state");
                let pod_state = pod_store.state();

                let node_names: HashSet<String> = pod_state
                    .iter()
                    .map(|p| p.spec.as_ref().unwrap().node_name.as_ref().unwrap())
                    .cloned()
                    .collect();

                // ensure current state includes all items from the pod store
                for pod in pod_state {
                    this.process_pod(pod);
                }

                // create a scope around the mutexguard as to drop it prior to the sleep
                {
                    let mut data = this.data.lock().unwrap();

                    trace!(?node_names, nodes_to_pods = ?data.nodes_to_pods, "Reconciling pods across nodes");

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
                        if let Some(pods) = data.nodes_to_pods.get_mut(node.as_str()) {
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

                            pods.retain(|p| pods_on_node.contains(Meta::name(p).as_str()));
                        }
                    }
                }

                debug!("Done reconciling. Sleeping for {} seconds.", delay);
                tokio::time::delay_for(Duration::from_secs(delay)).await
            }
        });
    }

    pub fn is_pod_released(&self, node_name: &str, pod_name: &str) -> bool {
        // add metrics & logs

        let data = self.data.lock().unwrap();

        trace!(node = node_name, pod = pod_name, "Checking if pod released");

        if !data.nodes_to_pods.contains_key(node_name) {
            return false;
        }

        let pods = data.nodes_to_pods.get(node_name).unwrap();

        if pods.len() == 0 {
            return false;
        }

        let first_non_ready_pod = pods.iter().find(|p| {
            // Find the first non-ready pod with the 'pod-rate-limiter-init' init container.
            // Most of this is sanity checking, since any pod without the init container
            // should not be in the pod list, and could be simplified to just filter on
            // non-ready pods.

            if p.status.is_none()
                || p.status.as_ref().unwrap().conditions.is_none()
                || p.status.as_ref().unwrap().init_container_statuses.is_none()
            {
                warn!(
                    pod = pod_name,
                    node = node_name,
                    "Found pod without 'status'."
                );
                return false;
            }

            if p.spec.is_none() || p.spec.as_ref().unwrap().init_containers.is_none() {
                warn!(
                    pod = pod_name,
                    node = node_name,
                    "Found pod without 'init_containers'."
                );
                return false;
            }

            // TODO: handle pods w/ startup probes. If startup probe status is "true/ready" also consider
            // the pod to be 'ready' (it may still report non-ready in cases where' it's intentionally
            // responding false to the readiness probe, such as hot standbys of "singleton" stateful services).

            let ready = p
                .status
                .as_ref()
                .unwrap()
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True");

            if ready {
                // If the pod is ready, ignore. It should be removed from the list shortly once
                // the new pod state event is received.
                return false;
            }

            let init_containers = p.spec.as_ref().unwrap().init_containers.as_ref().unwrap();

            let pod_rate_limit_init_container = init_containers
                .iter()
                .find(|c| c.name == "pod-rate-limiter-init");

            if !pod_rate_limit_init_container.is_some() {
                // This pod isn't rate limited. Should not happen as we don't insert non-rate limited pods.
                warn!(
                    pod = pod_name,
                    node = node_name,
                    "Found pod without 'pod-rate-limiter-init' init container."
                );
                return false;
            }

            let init_container_waiting = p
                .status
                .as_ref()
                .unwrap()
                .init_container_statuses
                .as_ref()
                .unwrap()
                .iter()
                .any(|s| {
                    s.name == "pod-rate-limiter-init"
                        && s.state.is_some()
                        && s.state.as_ref().unwrap().waiting.is_some()
                });

            if init_container_waiting {
                // This should not occur since we don't insert pods until the init container is no longer waiting.
                warn!(
                    pod = pod_name,
                    node = node_name,
                    "Found pod with waiting 'pod-rate-limiter-init' init container."
                );
                return false;
            }

            true
        });

        let found_name = match first_non_ready_pod {
            Some(_) => Meta::name(first_non_ready_pod.unwrap()),
            _ => "".to_string(),
        };

        let released = first_non_ready_pod.is_some() && found_name == pod_name;

        debug!(
            pod = pod_name,
            node = node_name,
            released,
            first_pod_non_ready_pod = first_non_ready_pod.is_some(),
            found_name = found_name.as_str(),
            "Is pod released?"
        );

        released
    }

    fn insert_pod(&self, pod: Pod) {
        // add metrics & logs

        let pod_name = Meta::name(&pod);
        let node = pod.spec.as_ref().unwrap().node_name.as_ref().unwrap();

        debug!(
            pod = pod_name.as_str(),
            node = node.as_str(),
            "Upserting pod"
        );

        let mut data = self.data.lock().unwrap();

        let pods = data
            .nodes_to_pods
            .entry(node.to_string())
            .or_insert_with(Vec::default);

        // If the pod already exists in the queue, update with the latest state so that we
        // do not reset its position.  Otherwise add the pod to the end of the queue.
        match pods.iter().position(|p| Meta::name(p) == pod_name) {
            Some(i) => {
                let patch = || {
                    let current = serde_json::to_value(&pods[i]).unwrap();
                    let new = serde_json::to_value(&pod).unwrap();

                    format!("{:?}", json_patch::diff(&current, &new).0)
                };
                trace!(patch = patch().as_str(), "Upsert diff");
                pods[i] = pod
            }
            None => pods.push(pod),
        }
    }

    fn remove_pod(&self, pod: &Pod) {
        trace!(
            pod = Meta::name(pod).as_str(),
            "Removing pod from internal state"
        );

        let pod_name = Meta::name(pod);

        if let Some(node) = pod.spec.as_ref().unwrap().node_name.as_ref() {
            let mut data = self.data.lock().unwrap();

            if let Some(v) = data.nodes_to_pods.get_mut(node.as_str()) {
                let len = v.len();
                v.retain(|p| Meta::name(p) != pod_name);
                if len != v.len() {
                    debug!(
                        node = node.as_str(),
                        pod = Meta::name(pod).as_str(),
                        "Removed pod from node list"
                    );
                }
            }
        }
    }

    fn delete_pod_from_k8s(&self, pod: &Pod) {
        let api = self.pods_api.clone();
        let pod_name = Meta::name(pod);

        // spawn the delete to avoid making this function async and it infecting upwards
        tokio::spawn(async move {
            let dp = DeleteParams {
                grace_period_seconds: Some(0),
                ..Default::default()
            };

            info!(pod = pod_name.as_str(), "Deleting pod");
            match api.delete(pod_name.as_str(), &dp).await {
                Ok(_) => (),
                Err(error) => error!(pod = pod_name.as_str(), ?error, "Failed to delete pod"),
            }
        });
    }

    fn process_pod(&self, pod: Pod) {
        let pod_name = Meta::name(&pod);

        if pod.spec.as_ref().unwrap().node_name.is_none() {
            PROCESSED_PODS_COUNTER
                .with_label_values(&["unassigned_node"])
                .inc();
            trace!(
                pod = pod_name.as_str(),
                "Pod not yet assigned to a node. Skipping"
            );
            return;
        }

        trace!(?pod, pod_name = pod_name.as_str(), "Processing pod");

        let status = pod.status.as_ref().unwrap();

        let mut init_container_not_waiting = false;
        let mut restarted = false;
        let mut completed_startup = match status.phase.as_ref().unwrap().as_str() {
            "Failed" | "Succeeded" | "Unknown" => true,
            _ => false,
        };

        if let Some(conds) = &status.conditions {
            completed_startup |= conds
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True");
        }

        if let Some(container_status) = &status.container_statuses {
            restarted = container_status.iter().any(|c| c.restart_count > 0);
        }

        if let Some(init_container_statuses) = &status.init_container_statuses {
            // Verify that the init container hasn't started yet. This can happen if a volume isn't mounting, for example.
            // Ignore this pod as it is "stuck" and would block the release queue.
            init_container_not_waiting = init_container_statuses.iter().any(|c| {
                c.name == "pod-rate-limiter-init"
                    && c.state.as_ref().is_some()
                    && c.state.as_ref().unwrap().waiting.is_none()
            });

            restarted |= init_container_statuses.iter().any(|c| c.restart_count > 0);
        }

        debug!(
            init_container_not_waiting,
            restarted,
            completed_startup,
            pod = pod_name.as_str(),
            "Pod status"
        );

        if restarted && !completed_startup {
            // If a pod is restarted, we need to delete it to force the pod-rate-limiter-init process
            // to run again. Otherwise the crashing container will start up again without checking
            // for rate limiting and throw off our rate limiting assumptions.
            // This will move it to the back the queue, which is OK since it crashed and
            // may need a backoff anyway.
            // Additionally, the pod's config may be corrupt or otherwise unable to start.  Leaving it
            // as a "released" pod may indefinitely block all other queued pods.
            //
            // Don't delete if it's `ready` (perhaps the pod started before the rate-limiter was activated).
            //
            // TODO: consider alternatives to this. It will have the affect of breaking the backoff delays.
            PROCESSED_PODS_COUNTER
                .with_label_values(&["delete_pod_from_k8s"])
                .inc();
            self.delete_pod_from_k8s(&pod);
        } else if init_container_not_waiting && !completed_startup {
            // The pod is starting (either the rate limiter container itself or the
            // main container(s)).
            PROCESSED_PODS_COUNTER
                .with_label_values(&["insert_pod"])
                .inc();
            self.insert_pod(pod);
        } else {
            PROCESSED_PODS_COUNTER
                .with_label_values(&["remove_pod"])
                .inc();
            self.remove_pod(&pod);
        }

        let data = self.data.lock().unwrap();
        for (node, pods) in &data.nodes_to_pods {
            PENDING_PODS_GAUGE
                .with_label_values(&[node])
                .set(pods.len() as i64);
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
    use anyhow::anyhow;
    use futures::{StreamExt, TryStreamExt};
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::{DeleteParams, ListParams, PostParams, WatchEvent};
    use kube::Api;
    use serde_json::json;
    use tokio::time;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        logging::init_logging();

        let pod_names: Vec<String> = (1..=3).map(|i| format!("pod-rate-limiter-{}", i)).collect();
        reset_pods(&pod_names).await?;

        let registry = prometheus::Registry::default();
        let controller = RateLimitingController::new(&registry).await;

        let client = kube::Client::try_default().await.expect("create client");
        let pods_api = Api::<Pod>::namespaced(client.clone(), "default");

        let mut pod: Pod = serde_json::from_value(json!({
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
                    "name": "pod-rate-limiter-init",
                    "image": "bash"
                }],
                "containers": [{
                    "args": ["-c", "sleep 60"],
                    "name": "pod-rate-limiter-test",
                    "image": "bash"
                }],
            }
        }))?;

        // Start the controller's logic
        let controller_spawn = controller.clone();
        tokio::spawn(async move { controller_spawn.run().await });

        let mut sleep = 5;
        let pp = PostParams::default();

        for pod_name in &pod_names {
            pod.metadata.name = Some(pod_name.to_string());
            pod.spec
                .as_mut()
                .unwrap()
                .init_containers
                .as_mut()
                .unwrap()
                .get_mut(0)
                .unwrap()
                .args = Some(vec!["-c".to_string(), format!("sleep {}", sleep)]);
            sleep += 5;

            match pods_api.create(&&pp, &pod).await {
                Err(e) => assert!(false, "pod creation failed {:?}", e),
                _ => (),
            };

            // For for this pod's init container to start running. This ensures the pods start and
            // and queued up in sequence.
            wait_for_running(pod_name, true, pods_api.clone()).await?;
        }

        for pod_name in &pod_names {
            assert_released(pod_name, &pod_names, &controller);
            println!("Waiting for ready: {}", pod_name);
            wait_for_running(pod_name, false, pods_api.clone()).await?;
            println!("Pod is ready");
            time::delay_for(time::Duration::from_millis(100)).await;
        }

        // check any other internal state?

        reset_pods(&pod_names).await?;

        Ok(())
    }

    async fn wait_for_running(
        pod_name: &str,
        wait_for_init: bool,
        pods_api: Api<Pod>,
    ) -> anyhow::Result<()> {
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
            true => Ok(()),
            false => Err(anyhow!("Pod not ready: {}", pod_name)),
        }
    }

    fn assert_released(
        released_pod: &str,
        pod_names: &Vec<String>,
        controller: &RateLimitingController,
    ) {
        let mut pod_released = false;
        let mut total_released = 0;

        for pod_name in pod_names {
            if controller.is_pod_released("minikube", pod_name.as_str()) {
                total_released += 1;

                if pod_name == released_pod {
                    pod_released = true;
                }
            }
        }

        assert_eq!(1, total_released);
        assert!(pod_released);
    }

    async fn reset_pods(pod_names: &Vec<String>) -> anyhow::Result<()> {
        let client = kube::Client::try_default().await.expect("create client");
        let pods = Api::<Pod>::namespaced(client.clone(), "default");

        let dp = DeleteParams {
            grace_period_seconds: Some(0),
            ..Default::default()
        };
        let lp = ListParams::default().labels("pod-rate-limiter=enabled");

        for pod in pods.list(&lp).await? {
            let name = pod.metadata.name.unwrap();
            if pod_names.contains(&name) {
                pods.delete(name.as_str(), &dp).await?;
            }
        }

        Ok(())
    }
}

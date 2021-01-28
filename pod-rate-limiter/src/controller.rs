use actix_web::dev::Payload;
use actix_web::http::{Error, StatusCode};
use actix_web::{get, web, FromRequest, HttpRequest, HttpResponse, Responder};
use futures::future::{ok, Ready};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, Meta};
use kube::api::{DeleteParams, ListParams};
use kube_runtime::reflector::Store;
use kube_runtime::utils::try_flatten_touched;
use kube_runtime::{reflector, watcher};
use prometheus::Registry;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Duration;

// Flow:
// - The MutatingAdmissionWebhook injects a rate limiting init container. The init container polls this service to query its "released" status.
// - Once pod is assigned to a node add it to the `nodes_to_pods` map.
// - Wait for pods to become "ready".
//   - Upon becoming ready, remove from the `nodes_to_pods` map (as well as the `pods_pending_assignment` set just in case).
//   - Removing the pod from the `nodes_to_pods` maps releases the next pod in the queue, if any.
//     - Todo: nice to have in the future is ability to check the liveness and startup probes, if any.
//       - Use case: some pods are alive, but never ready, such as a hot standby pod.
//
// Exception cases:
// - If a pod does not become ready after X minutes, remove from the `nodes_to_pods` map and release new pods.
// Exceptions should emit metrics and log sufficient details for debugging, whether an issue with this service or
// the cluster.
// - If pod errors what happens?

// TODO: Check all `unwraps` and handle correctly

#[derive(Debug, Clone)]
struct RateLimitingControllerData {
    //pods_pending_assignment: HashSet<String>, // probably record podname+timestamp to purge pods that are never assigned and get deleted (e.g. denied by another admission controller, other failures)
    nodes_to_pods: HashMap<String, Vec<String>>,
}

#[derive(Clone)]
pub struct RateLimitingController {
    data: Arc<Mutex<RateLimitingControllerData>>,
    pods_api: Api<Pod>,
}

impl RateLimitingController {
    pub async fn new(registry: Registry) -> Self {
        let client = kube::Client::try_default().await.expect("create client");
        let pods = Api::<Pod>::all(client.clone());

        Self {
            data: Arc::new(Mutex::new(RateLimitingControllerData {
                //pods_pending_assignment: HashSet::new(),
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
            watcher(self.pods_api.clone(), ListParams::default()),
        );

        println!("Starting reconciler");
        self.spawn_reconciler(pod_store_reader);

        let mut pod_stream = try_flatten_touched(pod_reflector).boxed();

        println!("Starting event processing loop");

        loop {
            match pod_stream.try_next().await {
                Ok(Some(pod)) => {
                    // println!("Event: {:?}", pod);
                    self.process_pod(&pod);
                }
                // some sort of Err() or Ok(None).  Log and sleep? Will the stream recover?
                Err(e) => {
                    println!("Processing loop error. {:?}", e);
                    tokio::time::delay_for(Duration::from_secs(1)).await;
                }
                Ok(None) => {
                    // TODO: if the stream is restarted do we get a None or just the Restarted (which is filtered by 'try_flatted_touched`)
                    println!("Event stream ended or graceful shutdown.");
                    break;
                }
            }
        }
    }

    fn spawn_reconciler(&self, pod_store: Store<Pod>) {
        // TODO: what if we expire a pod and then this reconciler adds it back (e.g. stuck in pending). How to ignore
        //       pods that never start (container state=waiting {reason='CrashLoopBackOff'})...
        let this = self.clone();

        tokio::spawn(async move {
            // delay initial iteration since the store is empty upon start.
            let delay = 10; // 120. 10 for debugging
            tokio::time::delay_for(Duration::from_secs(delay)).await;

            loop {
                println!("Reconciling");
                let pod_state = pod_store.state();

                // ensure current state matches the store
                for pod in &pod_state {
                    this.process_pod(pod);
                }

                // clear lost objects
                let pod_names: HashSet<String> = pod_state
                    .iter()
                    .map(|p| p.metadata.name.as_ref().unwrap())
                    .cloned()
                    .collect();

                let node_names: HashSet<String> = pod_state
                    .iter()
                    .map(|p| p.spec.as_ref().unwrap().node_name.as_ref().unwrap())
                    .cloned()
                    .collect();

                // create a scope around the mutexguard as to drop it prior to the sleep
                {
                    let mut data = this.data.lock().unwrap();

                    // clear any pods in `pods_pending_assignment` that no longer exist
                    // data.pods_pending_assignment
                    //     .retain(|p| pod_names.contains(p));

                    println!("Node names: {:#?}", node_names);
                    println!("nodes_to_pods:\n\t{:#?}", data.nodes_to_pods);
                    // clear any nodes in `nodes_to_pods` that no longer exist
                    data.nodes_to_pods.retain(|k, _| node_names.contains(k));

                    // clear any pods in `nodes_to_pods` that no longer exist
                    println!("nodes_to_pods:\n\t{:#?}", data.nodes_to_pods);
                    for node in node_names {
                        if let Some(pods) = data.nodes_to_pods.get_mut(node.as_str()) {
                            println!("Clearing pods that no longer exist on node: {:?}", node);

                            let pods_on_node: HashSet<String> = pod_store
                                .state()
                                .iter()
                                .filter(|p| match &p.spec.as_ref().unwrap().node_name {
                                    Some(n) => *n == node,
                                    _ => false,
                                })
                                .map(|p| Meta::name(p))
                                .collect();

                            pods.retain(|p| pods_on_node.contains(p));
                        }
                    }
                }

                println!("Done reconciling");
                tokio::time::delay_for(Duration::from_secs(120)).await
            }
        });
    }

    pub fn is_pod_released(&self, node_name: &str, pod_name: &str) -> bool {
        // add metrics & logs
        if pod_name.contains("backend-service") {
            return true;
        }

        println!("{:#?}", self.data.lock().unwrap().nodes_to_pods);

        // If the pod name is not in the list, or its in the first position, then it is "released"
        self.data
            .lock()
            .unwrap()
            .nodes_to_pods
            .get(node_name)
            .and_then(|pods| pods.iter().skip(1).find(|p| *p == pod_name))
            .is_none()
    }

    fn enqueue_pod(&self, pod: &Pod) {
        // add metrics & logs

        let pod_name = Meta::name(pod);
        let node = pod.spec.as_ref().unwrap().node_name.as_ref().unwrap();

        println!("Enqueuing pod: {:?} to node: {:?}", pod_name, node);

        let mut data = self.data.lock().unwrap();

        //data.pods_pending_assignment.remove(pod_name.as_str());

        let pods = data
            .nodes_to_pods
            .entry(node.to_string())
            .or_insert_with(Vec::default);

        if !pods.contains(&pod_name) {
            pods.push(pod_name);
        }
    }

    fn release_pod(&self, pod: &Pod) {
        // add metrics & logs
        println!("Releasing pod: {:?}", Meta::name(pod));

        let pod_name = Meta::name(pod);

        // TODO: handle cases where no node is not yet assigned (return/no-op)
        let node = pod.spec.as_ref().unwrap().node_name.as_ref().unwrap();

        println!("Releasing pod: {:?} on node: {:?}", Meta::name(pod), node);

        let mut data = self.data.lock().unwrap();

        //data.pods_pending_assignment.remove(pod_name.as_str());

        if let Some(v) = data.nodes_to_pods.get_mut(node.as_str()) {
            v.retain(|x| *x != pod_name)
        } else {
            println!("Node not found: {:?}", node);
            // log?  should not happen?
        }
    }

    fn delete_pod(&self, pod: &Pod) {
        let api = self.pods_api.clone();
        let pod_name = Meta::name(pod);

        // spawn the delete to avoid making this function async and it infecting upwards
        tokio::spawn(async move {
            let dp = DeleteParams {
                grace_period_seconds: Some(0),
                ..Default::default()
            };

            // TODO log/handle await Result
            api.delete(pod_name.as_str(), &dp).await;
        });
    }

    fn process_pod(&self, pod: &Pod) {
        println!(
            "\nPod Details @ {:?}:\n\tPod Name: {:?}\n\tNode Name: {:?}",
            time::OffsetDateTime::now_utc(),
            Meta::name(pod),
            pod.spec.as_ref().unwrap().node_name
        );

        //println!("{:#?}\n\n", pod.status.as_ref().unwrap());

        // if 'starting', no node assigned, do nothing
        // if 'not ready' => move to nodes_to_pods
        // if 'ready' => remove from nodes_to_pods
        // if 'waiting' => kill pod : |  This will force the init container to re-run
        //   todo: check that this happens after the first crash and not after some N number of crashes
        //   add metrics for the DELETE and adjust crash looping alerts
        //   add naive crash looping backoff handling
        // if 'terminating/dead' => remove from nodes_to_pods & pods_pending_assignment

        // TODO: review status, etc: https://github.com/kubernetes/kube-state-metrics/blob/master/docs/pod-metrics.md

        // TODO: verify logic here for `evicted`, other cases? => `phase=Failed`?
        // TODO: handle pods w/ startup probes (prefer over 'ready')
        if pod.spec.as_ref().unwrap().node_name.is_none() {
            // no node assigned, do nothing until the pod is scheduled on a node
            return;
        }

        let mut deleting = false;
        let mut scheduled = false;
        let mut ready = false;
        let mut restarted = false;
        let phase = pod.status.as_ref().unwrap().phase.as_ref().unwrap();

        let status = pod.status.as_ref().unwrap();

        if let Some(conds) = &status.conditions {
            scheduled = conds
                .iter()
                .any(|c| c.type_ == "PodScheduled" && c.status == "True");
            ready = conds
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True");
        }

        if let Some(container_status) = &status.container_statuses {
            deleting = container_status
                .iter()
                .all(|c| c.state.as_ref().unwrap().terminated.is_some());
            restarted = container_status.iter().any(|c| c.restart_count > 0);
        }

        println!(
            "\tScheduled: {:?}, Ready: {:?}, Deleting: {:?}, Restarted: {:?}, Pod: {:?}",
            scheduled,
            ready,
            deleting,
            restarted,
            Meta::name(pod)
        );

        // order important here. `deleting` and `scheduled` may both be true, so handle deleting first.
        // similarly, 'ready' and 'scheduled' may both be true, so handle 'ready' prior to `scheduled`
        if restarted {
            // If a pod is restarted, we need to delete it to force the pod-rate-limiter-init process
            // to run again.  Otherwise the crashing container will start up again without checking
            // for rate limiting and throw off our rate limiting assumptions.
            // This will move it to the back the queue, which is probably OK since it crashed and
            // may need a backoff anyway (which would occur anyway if the container fails a few times).
            self.delete_pod(pod);
        } else if deleting || ready || phase == "Failed" {
            // || "time expired?"
            self.release_pod(pod);
        } else if scheduled {
            // this feels a bit aggressive. what other conditions result in a 'scheduled'
            // pod, but one that should not be enqueued?
            self.enqueue_pod(pod);
        } else {
            // should not happen? log & metric?
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

#[derive(Deserialize)]
pub struct PodReleasedQuery {
    node: String,
    pod: String,
}

#[get("/is_pod_released")]
pub async fn is_pod_released(
    controller: RateLimitingController,
    query: web::Query<PodReleasedQuery>,
) -> impl Responder {
    if controller.is_pod_released(query.node.as_str(), query.pod.as_str()) {
        HttpResponse::new(StatusCode::OK)
    } else {
        HttpResponse::new(StatusCode::LOCKED)
    }
}

#[cfg(test)]
mod tests {
    use crate::controller::RateLimitingController;
    use anyhow::anyhow;
    use futures::{StreamExt, TryStreamExt};
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::{DeleteParams, ListParams, PostParams, WatchEvent};
    use kube::Api;
    use serde_json::json;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let pod_names: Vec<String> = (1..=3).map(|i| format!("pod-rate-limiter-{}", i)).collect();
        reset_pods(&pod_names).await?;

        let registry = prometheus::Registry::default();
        let controller = RateLimitingController::new(registry.clone()).await;

        let client = kube::Client::try_default().await.expect("create client");
        let pods = Api::<Pod>::namespaced(client.clone(), "default");

        let mut pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "pod-rate-limiter-test" },
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

            match pods.create(&&pp, &pod).await {
                Err(e) => assert!(false, "pod creation failed {:?}", e),
                _ => (),
            };
        }

        for i in 0..3 {
            let pod_name = pod_names.get(i).unwrap();
            assert_released(pod_name, i + 1, &pod_names, &controller);
            println!("Waiting for ready: {}", pod_name);
            wait_for_running(pod_name, pods.clone()).await?;
            println!("Pod is ready");
        }

        // check any other internal state?

        reset_pods(&pod_names).await?;

        Ok(())
    }

    async fn wait_for_running(pod_name: &str, pods: Api<Pod>) -> anyhow::Result<()> {
        let lp = ListParams::default()
            .fields(&format!("metadata.name={}", pod_name))
            .timeout(20);
        let mut stream = pods.watch(&lp, "0").await?.boxed();

        let mut running = false;

        while let Some(status) = stream.try_next().await? {
            match status {
                WatchEvent::Modified(o) => {
                    let s = o.status.as_ref().expect("status exists on pod");
                    let phase = s.phase.clone().unwrap_or_default();
                    if phase == "Running" {
                        running = true;
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
        expected_released_count: usize,
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
        assert!(pod_released);
        assert_eq!(expected_released_count, total_released);
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
            let name = pod.metadata.name.unwrap();
            if pod_names.contains(&name) {
                pods.delete(name.as_str(), &dp).await?;
            }
        }

        Ok(())
    }
}

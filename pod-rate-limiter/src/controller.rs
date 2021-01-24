use actix_web::dev::Payload;
use actix_web::http::{Error, StatusCode};
use actix_web::{get, web, FromRequest, HttpRequest, HttpResponse, Responder};
use futures::future::{ok, Ready};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::api::{Api, Meta};
use kube_runtime::reflector::Store;
use kube_runtime::utils::try_flatten_touched;
use kube_runtime::{reflector, watcher};
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Duration;

// Flow:
// - The MutatingAdmissionWebhook injects a rate limiting init container. The init container polls this service to query its "released" status.
//   - At this point the pod is not yet assigned to a node, so we watch for assignment events and put it in the `pods_pending_assignment` set in the meantime.
// - Once pod is assigned move it from the `pods_pending_assignments` set to the `nodes_to_pods` map.
// - Wait for pods to become "ready".
//   - Upon becoming ready, remove from the `nodes_to_pods` map (as well as the `pods_pending_assignment` set just in case).
//   - Removing the pod from the `nodes_to_pods` maps releases the next pod in the queue, if any.
//     - Todo: nice to have in the future is ability to check the liveness and startup probes, if any.
//       - Use case: some pods are alive, but never ready, such as a hot standby pod.
//
// Exception cases:
// - If a pod does not become ready after X minutes, remove from the `nodes_to_pods` map and release new pods.
// - If a pod never moves from the `pods_pending_assignments` to `nodes_to_pods` map.  Some cases of this may
//   be expected if other admission webhooks deny entry.
// Exceptions should emit metrics and log sufficient details for debugging, whether an issue with this service or
// the cluster.
// - If pod errors what happens?

// TODO: Check all `unwraps` and handle correctly

#[derive(Debug, Clone)]
struct RateLimitingControllerData {
    pods_pending_assignment: HashSet<String>, // probably record podname+timestamp to purge pods that are never assigned and get deleted (e.g. denied by another admission controller, other failures)
    nodes_to_pods: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct RateLimitingController {
    data: Arc<Mutex<RateLimitingControllerData>>,
}

impl RateLimitingController {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(RateLimitingControllerData {
                pods_pending_assignment: HashSet::new(),
                nodes_to_pods: HashMap::new(),
            })),
        }
    }

    pub async fn run(&self) {
        let client = kube::Client::try_default().await.expect("create client");
        let pods = Api::<Pod>::all(client.clone());

        let pod_store = reflector::store::Writer::<Pod>::default();
        let pod_store_reader = pod_store.as_reader();
        let pod_reflector = reflector(pod_store, watcher(pods, ListParams::default()));

        println!("Starting reconciler");
        self.spawn_reconciler(pod_store_reader);

        let mut pod_stream = try_flatten_touched(pod_reflector).boxed();

        println!("Starting event processing loop");

        // add some ability to terminate the loop

        loop {
            match pod_stream.try_next().await {
                Ok(Some(pod)) => {
                    // println!("Event: {:?}", pod);
                    self.process_pod(&pod);
                }
                // some sort of Err() or Ok(None).  Log and sleep? Will the stream recover?
                Err(e) => {
                    println!("Processing loop error. {:?}", e);
                    tokio::time::delay_for(Duration::from_secs(10)).await;
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
            let delay = 10; // 120. 10 for debuging
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
                    data.pods_pending_assignment
                        .retain(|p| pod_names.contains(p));

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
                                .filter(|p|
                                    match p.spec.as_ref().unwrap().node_name.as_ref() {
                                        Some(n) => *n == node,
                                        _ => false
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

    pub fn add_pod_pending_assignment(&self, pod_name: String) {
        // add metrics & logs

        self.data
            .lock()
            .unwrap()
            .pods_pending_assignment
            .insert(pod_name);
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

        data.pods_pending_assignment.remove(pod_name.as_str());

        let pods = data
            .nodes_to_pods
            .entry(node.to_string())
            .or_insert_with(Vec::default);

        if !pods.contains(&pod_name) {
            pods.push(pod_name);
        }
    }

    fn requeue_pod(&self, pod: &Pod) {
        // TODO: rework lock scoping. There's a race here if a
        // `is_pod_released` call happens in between the
        // release & enqueu
        self.release_pod(pod);
        self.enqueue_pod(pod);
    }

    fn release_pod(&self, pod: &Pod) {
        // add metrics & logs
        println!("Releasing pod: {:?}", Meta::name(pod));

        let pod_name = Meta::name(pod);

        // TODO: handle cases where no node is not yet assigned (return/no-op)
        let node = pod.spec.as_ref().unwrap().node_name.as_ref().unwrap();

        println!("Releasing pod: {:?} on node: {:?}", Meta::name(pod), node);

        let mut data = self.data.lock().unwrap();

        data.pods_pending_assignment.remove(pod_name.as_str());

        if let Some(v) = data.nodes_to_pods.get_mut(node.as_str()) {
            v.retain(|x| *x != pod_name)
        } else {
            println!("Node not found: {:?}", node);
            // log?  should not happen?
        }
    }

    fn process_pod(&self, pod: &Pod) {
        println!(
            "\nPod Details @ {:?}:\n\tPod Name: {:?}\n\tNode Name: {:?}",
            time::OffsetDateTime::now_utc(),
            Meta::name(pod),
            pod.spec.as_ref().unwrap().node_name
        );

        //println!("{:#?}\n\n", pod.status.as_ref().unwrap());

        // if 'starting', no node assigned => pods_pending_assignment
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
            println!("No node name");
            self.add_pod_pending_assignment(Meta::name(pod));
        } else {
            let mut deleting = false;
            let mut scheduled = false;
            let mut ready = false;
            let mut crash_loop_back_off = false;
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
                crash_loop_back_off = container_status.iter().any(|c| {
                    // TODO: yuck, add var
                    c.state.as_ref().unwrap().waiting.is_some()
                        && c.state
                            .as_ref()
                            .unwrap()
                            .waiting
                            .as_ref()
                            .unwrap()
                            .reason
                            .is_some()
                        && c.state
                            .as_ref()
                            .unwrap()
                            .waiting
                            .as_ref()
                            .unwrap()
                            .reason
                            .as_ref()
                            .unwrap()
                            == "CrashLoopBackOff"
                });
            }

            println!(
                "\tScheduled: {:?}, Ready: {:?}, Deleting: {:?}, Pod: {:?}",
                scheduled,
                ready,
                deleting,
                Meta::name(pod)
            );

            // order important here. `deleting` and `scheduled` may both be true, so handle deleting first.
            // similarly, 'ready' and 'scheduled' may both be true, so handle 'ready' first
            if deleting || ready || phase == "Failed" {
                // || "time expired?"
                self.release_pod(pod);
            } else if crash_loop_back_off {
                //TODO: self.delete_pod(pod);
                //self.requeue_pod(pod);
            } else if scheduled {
                // this feels a bit aggressive. what other conditions result in a 'scheduled'
                // pod, but one that should not be enqueued?
                self.enqueue_pod(pod);
            } else {
                // should not happen? log?
            }
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
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::{DeleteParams, ListParams, PostParams};
    use kube::Api;
    use serde_json::json;
    use tokio::time::{delay_for, Duration};

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let pod_names: Vec<String> = (1..=3).map(|i| format!("pod-rate-limiter-{}", i)).collect();
        reset_pods(&pod_names).await?;

        let controller = RateLimitingController::new();

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

            delay_for(Duration::from_millis(100)).await;
        }

        delay_for(Duration::from_millis(3000)).await;

        // TODO: think more about sequencing and sleeps -> pods need to release in order so that they sleep correctly.
        //       Maybe we should just give up and run a full actix server and effectively turn this to a full integration test
        //       which it kind of is ending up as.
        // t1 - 1 of 3 pods released
        assert_released(&pod_names, 1, &controller);
        delay_for(Duration::from_millis(7000)).await;

        assert_released(&pod_names, 2, &controller);
        delay_for(Duration::from_millis(7000)).await;

        assert_released(&pod_names, 3, &controller);
        // hard to check `controller.add_pod_pending_assignment()`. Add metrics to verify 3 items hit this list?
        // check any other internal state?

        reset_pods(&pod_names).await?;

        // trigger.cancel();

        Ok(())
    }

    fn assert_released(
        pod_names: &Vec<String>,
        expected: i32,
        controller: &RateLimitingController,
    ) {
        let mut released = 0;
        for pod_name in pod_names {
            // ick: hardcoding 'minikube'
            if controller.is_pod_released("minikube", pod_name.as_str()) {
                released += 1;
            }
        }
        assert_eq!(expected, released);
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

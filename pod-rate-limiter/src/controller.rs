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
// - If a pod does not become ready after 5 minutes, remove from the `nodes_to_pods` map and release new pods.
// - If a pod never moves from the `pods_pending_assignments` to `nodes_to_pods` map.  Some cases of this may
//   be expected if other admission webhooks deny entry.
// Exceptions should emit metrics and log sufficient details for debugging, whether an issue with this service or
// the cluster.
// - If pod errors what happens?

// TODO: Check all `unwraps` and handle correctly

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
       // let pods: Api<Pod> = Api::namespaced(client.clone(), "default");
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
                    println!("Event: {:?}", pod);
                    self.process_pod(&pod);
                },
                // some sort of Err() or Ok(None).  Log and sleep? Will the stream recover?
                _ => {
                    println!("Processing loop error.");
                    tokio::time::delay_for(Duration::from_secs(5)).await;
                }
            }
        }
        
        //Box::pin(pod_stream)
        // Box::pin( async { () })
        // log & report metric if the loop exists, which would mean something went bad
    }

    fn spawn_reconciler(&self, pod_store: Store<Pod>) {
        // TODO: add some ability to terminate the loop - https://docs.rs/stream-cancel/0.4.4/stream_cancel/ ?
        // TODO: what if we expire a pod and then this reconciler adds it back (e.g. stuck in pending). How to ignore
        //       pods that never start (container state=waiting {reason='CrashLoopBackOff'})...
        let this = self.clone();

        tokio::spawn(async move {
            {
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
                    .map(|p| {
                        p.spec
                            .as_ref()
                            .unwrap()
                            .node_name
                            .as_ref()
                            .unwrap()
                    })
                    .cloned()
                    .collect();

                let mut data = this.data.lock().unwrap();

                // clear any pods in `pods_pending_assignment` that no longer exist
                data.pods_pending_assignment
                    .retain(|p| pod_names.contains(p));

                // clear any nodes in `nodes_to_pods` that no longer exist
                data.nodes_to_pods.retain(|k, _| node_names.contains(k));

                // clear any pods in `nodes_to_pods` that no longer exist
                for node in node_names {
                    let pods = data.nodes_to_pods.get_mut(node.as_str()).unwrap();

                    let pods_on_node: HashSet<String> = pod_store
                        .state()
                        .iter()
                        .filter_map(|p| p.spec.as_ref().unwrap().node_name.as_ref())
                        .map(|s| s.to_string())
                        .collect();

                    pods.retain(|p| pods_on_node.contains(p));
                }
            }
            tokio::time::delay_for(Duration::from_secs(120)).await;
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
            // log?  should not happen?
        }
    }

    fn process_pod(&self, pod: &Pod) {
        println!("\nPod Details:\n\tPod Name: {:?}\n\tNode Name: {:?}", Meta::name(pod), pod.spec.as_ref().unwrap().node_name);
        //println!("{:#?}\n\n", pod.status.as_ref().unwrap());

        // if 'starting', no node assigned => pods_pending_assignment
        // if 'not ready' => move to nodes_to_pods
        // if 'ready' => remove from nodes_to_pods
        // if 'terminating/dead' => remove from nodes_to_pods & pods_pending_assignment

        // TODO: verify logic here for `evicted`, other cases? => `phase=Failed`?
        // TODO: handle pods w/ startup probes (prefer over 'ready')
        if pod.spec.as_ref().unwrap().node_name.is_none() {
            println!("No node name");
            self.add_pod_pending_assignment(Meta::name(pod));
        } else {
            let mut deleting = false;
            let mut scheduled = false;
            let mut ready = false;
            let phase = pod.status.as_ref().unwrap().phase.as_ref().unwrap();

            let status = pod.status.as_ref().unwrap();

            if let Some(conds) = &status.conditions {
                scheduled = conds.iter().any(|c| c.type_ == "PodScheduled" && c.status == "True");
                ready = conds.iter().any(|c| c.type_ == "Ready" && c.status == "True");
            }

            if let Some(container_status) = &status.container_statuses {
                deleting = container_status.iter().all(|c| c.state.as_ref().unwrap().terminated.is_some());
            }

            println!("Scheduled: {:?}, Ready: {:?}, Deleting: {:?}, Pod: {:?}", scheduled, ready, deleting, Meta::name(pod));

            // order important here. `deleting` and `scheduled` may both be true, so handle deleting first.
            // similarly, 'ready' and 'scheduled' may both be true, so handle 'ready' first
            if deleting || phase == "Failed" {
                // this isn't the most efficient way but will ensure the pod is
                // removed from the pending_assignments vec and then cleared from the
                // nodes_to_pods vec.
                // TODO: to this efficiently?
                self.enqueue_pod(pod);
                self.release_pod(pod);
            } else if ready {   // || "time expired"
                self.release_pod(pod);
            } else if scheduled {
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

Docker Proxy Admission Webhook
------------------------------

This webhook intercepts POD resources and rewrites container `image` URLs to point to a "pull-through"
caching docker proxy. This way, should any images disappear from their original location (e.g. due to docker hub's 
retention policy) or an outage at the original provider, your image will remain available.

Using a webhook avoids having to:
1. Update all existing deployments to reference the caching proxy.
2. Remember to update all future deployments to reference the caching proxy. 

Dependencies
------------

- [cert-manager](https://github.com/jetstack/cert-manager)
  - Update the cert-manager deployments to point to your docker cache.
    - The cert-manager namespace is omitted from the proxy webhook to avoid a bi-directional dependency between cert-manager and docker-proxy-webhook,
      where neither pod may start as they depend on each other (with the docker-proxy depending on a certificate injection and the cert-manager
      depending on the docker-proxy webhook itself).
- For similar reasons to the above, update all `kube-system` deployments to pull from your docker proxies. Failing to do
  so may result in nodes unable to join the cluster, as CNI, DNS, and/or kube-proxy pods will fail to start if the webhook is not running.
- In each namespace, create a sufficiently configured docker config secret named `docker-proxy-credentials`. See: https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/ 
  - https://github.com/alexellis/registry-creds can help automate this

Deploying
---------

- `kubectl label namespace cert-manager docker-proxy-webhook=disabled`
- `kubectl label namespace kube-system docker-proxy-webhook=disabled`
- Update `manifests/k8s.yaml` as needed
    - Update the `configMap` to:
        - map your registries from public hosts to your private host
        - configure any domains to ignore, such as those pointing to private AWS ECR registries
    - `kubectl apply -f manifests/k8s.yaml`
- Add alert conditions for unmapped image references, other failure cases

TODO
----
[] Clean up TODOs... some of this are done
[] A few more docs on what this looks like in artifactory
[] Validating webhook
[] Record metric if image not rewritten
[] Test servicemonitor
[] Set up integration tests
[] Document metrics + suggested alerts

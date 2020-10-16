NEXT Kubernetes Webhooks
========================

This repo contains NEXT Trucking's public webhooks.

While they work for us, and _some_ effort has been made to make them
generally usable, they may not work for you : )  Patches are very welcomed.

Please submit security vulnerabilities to `security@nexttrucking.com`

Webhooks
--------

- `docker-proxy-webhook` - This webhook rewrites the `image` value of `pod` specs to point to a
    caching docker proxy server (in our case, artifactory).  We use this to archive images in
    case they are deleted from their canonical location, such as when images are purged from
    Docker Hub (either by their retention policy or manually - c.f. `leftpad`s removal from NPM),
    as well as to avoid any potential rate limiting imposed by public registries.

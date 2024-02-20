/*
Copyright 2018 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// Based on https://github.com/kubernetes-sigs/controller-runtime/blob/cc532575f39e8368d0f3cf6e2ad2f14590cae16f/examples/builtins/validatingwebhook.go

package v1

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/docker/distribution/reference"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"net/http"
	"regexp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"strings"
)

type DockerConfig struct {
	IgnoreList []string          `yaml:"ignoreList"`
	DomainMap  map[string]string `yaml:"domainMap"`
}

type DockerProxyMutatingWebhook struct {
	Client     client.Client
	PullSecret string
	decoder    *admission.Decoder
	config     DockerConfig
}

var (
	anchoredShortIdentifierRegexp = regexp.MustCompile("^" + reference.ShortIdentifierRegexp.String() + "$")

	webhookResultCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "docker_proxy_mutating_webhook_result_total",
			Help: "Number of webhook invocations",
		},
		[]string{"mutated", "request_namespace"},
	)
	webhookFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "docker_proxy_mutating_webhook_failures_total",
			Help: "Number of webhook failures'",
		},
		[]string{"failure_reason", "request_namespace"},
	)
	containerRewriteCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "docker_proxy_mutating_webhook_container_rewrites_total",
			Help: "Number of container image values rewritten",
		},
		[]string{"domain", "request_namespace"},
	)
	unknownDomainCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "docker_proxy_mutating_webhook_unknown_domain_total",
			Help: "Number of unmapped domains",
		},
		[]string{"domain", "request_namespace"})
)

// log is for logging in this package.
var log = logf.Log.WithName("docker-proxy-mutating-webhook")

func init() {
	metrics.Registry.MustRegister(webhookResultCounter, webhookFailureCounter, containerRewriteCounter, unknownDomainCounter)
}

func NewDockerProxyMutatingWebhook(mutatingWebhookConfig []byte, client client.Client, pullSecret string) (*DockerProxyMutatingWebhook, error) {
	config := DockerConfig{}
	err := yaml.Unmarshal(mutatingWebhookConfig, &config)
	if err != nil {
		log.Error(err, "Unable to load config file.")
		return nil, err
	}

	if config.DomainMap != nil {
		for from, to := range config.DomainMap {
			log.V(1).Info("Remapping entry", "from", from, "to", to)
		}
	} else {
		err = errors.New("no domain mapping entries set")
		log.Error(err, "Invalid config.")
		return nil, err
	}

	if config.IgnoreList != nil {
		for _, ignore := range config.IgnoreList {
			log.V(1).Info("Ignore list entry", "value", ignore)
		}
	} else {
		log.V(1).Info("Ignore list empty")
	}

	return &DockerProxyMutatingWebhook{config: config, Client: client, PullSecret: pullSecret}, nil
}

// TODO: add _validating_ webhook to catch cases where the order of operations leads to non-conforming request

func (webhook *DockerProxyMutatingWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log.V(1).Info("mutating pod")

	if req.Resource.Resource != "pods" {
		webhookFailureCounter.WithLabelValues("invalid_resource_type", req.Namespace).Inc()

		err := errors.New("expect resource to be pods")
		logf.Log.Error(err, err.Error())
		return admission.Errored(http.StatusInternalServerError, err)
	}

	pod := &corev1.Pod{}

	err := webhook.decoder.Decode(req, pod)
	if err != nil {
		webhookFailureCounter.WithLabelValues("decode_error", req.Namespace).Inc()

		log.Error(err, "failed to decode pod")
		return admission.Errored(http.StatusBadRequest, err)
	}

	changed := false

	var containers []*corev1.Container
	for i := 0; i < len(pod.Spec.Containers); i++ {
		containers = append(containers, &pod.Spec.Containers[i])
	}
	for i := 0; i < len(pod.Spec.InitContainers); i++ {
		containers = append(containers, &pod.Spec.InitContainers[i])
	}

	for _, container := range containers {
		newImage, err := RewriteImage(container.Image, req.Namespace, webhook.config)
		if err != nil {
			webhookFailureCounter.WithLabelValues("rewrite_failed", req.Namespace).Inc()

			log.Error(err, err.Error())
			return admission.Errored(http.StatusInternalServerError, err)
		}
		if newImage != container.Image {
			log.V(1).Info("Rewriting image", "oldImage", container.Image, "newImage", newImage, "namespace", req.Namespace)
			container.Image = newImage
			changed = true
		}
	}

	if changed && webhook.PullSecret != "" {
		pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{Name: webhook.PullSecret},
		}
	}

	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		webhookFailureCounter.WithLabelValues("marshaling_failed", req.Namespace).Inc()

		log.Error(err, err.Error())
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if changed {
		webhookResultCounter.WithLabelValues("true", req.Namespace).Inc()
		return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
	} else {
		webhookResultCounter.WithLabelValues("false", req.Namespace).Inc()
		return admission.Allowed("No `image`s rewritten")
	}
}

func RewriteImage(image string, namespace string, config DockerConfig) (string, error) {
	if anchoredShortIdentifierRegexp.MatchString(image) {
		// Do not process "identifiers"
		return image, nil
	}

	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		log.Error(err, "unable to parse image", "image", image)
		return "", err
	}

	newImage := ""
	domain := strings.ToLower(reference.Domain(named))

	// Check if the domain matches a mapped domain value.
	// If so, it's already conforming & valid and does not need rewriting.
	for _, val := range config.DomainMap {
		if val == domain {
			return image, nil
		}
	}

	if val, ok := config.DomainMap[domain]; ok {
		newImage = val
	}

	// Note: behaviour is unspecified if the domain appears in both the `DomainMap` and `IgnoreList`
	if config.IgnoreList != nil {
		for _, ignore := range config.IgnoreList {
			if domain == ignore {
				newImage = domain
				break
			}
		}
	}

	if newImage == "" {
		log.V(1).Info("Found unmapped domain", "domain", domain)
		unknownDomainCounter.WithLabelValues(domain, namespace).Inc()
		newImage = domain
	} else {
		containerRewriteCounter.WithLabelValues(domain, namespace).Inc()
	}

	newImage += "/" + reference.Path(named)

	if t, ok := named.(reference.Tagged); ok {
		newImage += ":" + t.Tag()
	}

	if d, ok := named.(reference.Digested); ok {
		newImage += "@" + d.Digest().String()
	}

	return newImage, nil
}

func (webhook *DockerProxyMutatingWebhook) InjectDecoder(decoder *admission.Decoder) error {
	webhook.decoder = decoder
	return nil
}

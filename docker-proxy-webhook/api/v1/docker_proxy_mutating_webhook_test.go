/*
Copyright 2020 NEXT Trucking.
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
package v1

import (
	"testing"
)

func TestRewriteImage(t *testing.T) {
	type tcase struct {
		Image, Expected string
	}

	config := DockerConfig{
		IgnoreList: []string{"123456789012.dkr.ecr.us-east-1.amazonaws.com"},
		DomainMap: map[string]string{
			"docker.io":         "org-name-docker-io.jfrog.io",
			"quay.io":           "org-name-quay-io.jfrog.io",
			"gcr.io":            "org-name-gcr-io.jfrog.io",
			"k8s.gcr.io":        "org-name-k8s-gcr-io.jfrog.io",
			"docker.elastic.co": "org-name-docker-elastic-co.jfrog.io",
		},
	}

	tcases := []tcase{
		{
			Image:    "123456789012.dkr.ecr.us-east-1.amazonaws.com/org-name/xyz-service:77092c522d97113ec952fce7d27cfec20be5fd82",
			Expected: "123456789012.dkr.ecr.us-east-1.amazonaws.com/org-name/xyz-service:77092c522d97113ec952fce7d27cfec20be5fd82",
		},
		{
			Image:    "prom/statsd-exporter:latest",
			Expected: "org-name-docker-io.jfrog.io/prom/statsd-exporter:latest",
		},
		{
			Image:    "vault:1.2.2",
			Expected: "org-name-docker-io.jfrog.io/library/vault:1.2.2",
		},
		{
			Image:    "docker:latest",
			Expected: "org-name-docker-io.jfrog.io/library/docker:latest",
		},
		{
			Image:    "quay.io/coreos/kube-state-metrics:v1.8.0",
			Expected: "org-name-quay-io.jfrog.io/coreos/kube-state-metrics:v1.8.0",
		},
		{
			Image:    "gcr.io/google_containers/metrics-server-amd64:v0.3.5",
			Expected: "org-name-gcr-io.jfrog.io/google_containers/metrics-server-amd64:v0.3.5",
		},
		{
			Image:    "k8s.gcr.io/defaultbackend-amd64:1.5",
			Expected: "org-name-k8s-gcr-io.jfrog.io/defaultbackend-amd64:1.5",
		},
		{
			Image:    "docker.elastic.co/beats/filebeat-oss:7.1.1",
			Expected: "org-name-docker-elastic-co.jfrog.io/beats/filebeat-oss:7.1.1",
		},
		{
			Image:    "unmapped-domain.com/registry/repo",
			Expected: "unmapped-domain.com/registry/repo",
		},
		{
			Image:    "org-name-docker-io.jfrog.io/already-mapped-domain/test",
			Expected: "org-name-docker-io.jfrog.io/already-mapped-domain/test",
		},
		{
			Image:    "kubernetes-ingress-controller/nginx-ingress-controller@sha256:b494b781dbe0a164c7954a7ee9c9918ead58455b856045ae6d68c7c96192ac9d",
			Expected: "org-name-docker-io.jfrog.io/kubernetes-ingress-controller/nginx-ingress-controller@sha256:b494b781dbe0a164c7954a7ee9c9918ead58455b856045ae6d68c7c96192ac9d",
		},
		{
			Image:    "clojure:openjdk-8-lein-2.9.1-alpine@sha256:d5454be246358c9cd683a06108ccdecc8b31a0341e45973af480bef708e8cf1e",
			Expected: "org-name-docker-io.jfrog.io/library/clojure:openjdk-8-lein-2.9.1-alpine@sha256:d5454be246358c9cd683a06108ccdecc8b31a0341e45973af480bef708e8cf1e",
		},
		{
			Image:    "4a581cd6feb1",
			Expected: "4a581cd6feb1",
		},
	}

	for _, tcase := range tcases {
		img, err := RewriteImage(tcase.Image, "my_namespace", config)
		if err != nil {
			t.Errorf("Expected: %v. Got error: %v", tcase.Expected, err)
		} else if img != tcase.Expected {
			t.Errorf("Expected: %v. Got: %v", tcase.Expected, img)
		}
	}

	// another test case for err != nil
	img, err := RewriteImage("INVALID-UPPERCASE-REPO", "my_namespace", config)
	if err == nil {
		t.Errorf("Expected error. Got: %v", img)
	}
}

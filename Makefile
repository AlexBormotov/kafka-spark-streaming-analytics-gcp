# One command per task. CI runs these same targets.
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

ENV ?= local
GCP_PROJECT ?= qoe-spark-streaming
GCP_REGION ?= us-central1

TF_DIRS := $(wildcard infra/terraform/*)
CHART_DIRS := deploy/argocd/apps $(wildcard deploy/charts/*)

KIND_CLUSTER := qoe
ARGOCD_CHART_VERSION := 10.11.7
REVISION ?= $(shell git rev-parse --abbrev-ref HEAD)
WAIT_TIMEOUT ?= 900s
# GITOPS=1: Argo CD deploys this repository's charts too. Otherwise `make dev` deploys them.
GITOPS ?= 0
GITOPS_BOOL := $(if $(filter 1 true,$(GITOPS)),true,false)
# Own charts in install order, with the Helm release namespace of each.
OWN_CHARTS := platform
NS_platform := kafka

ifeq ($(ENV),local)
KUBE_CONTEXT := kind-$(KIND_CLUSTER)
endif
KUBECTL = kubectl --context $(KUBE_CONTEXT)
HELM = helm --kube-context $(KUBE_CONTEXT)

.PHONY: help
help: ## List targets
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Install the Python toolchain and dependencies
	uv sync --all-packages --frozen

.PHONY: lint
lint: setup ## Static checks: Python, YAML, Terraform, Helm
	uv run ruff check .
	uv run ruff format --check .
	uv run yamllint --strict .
	@for d in $(TF_DIRS); do echo "terraform fmt $$d"; terraform -chdir=$$d fmt -check -recursive; done
	@for c in $(CHART_DIRS); do helm lint --strict $$c; done

.PHONY: fmt
fmt: setup ## Apply formatters
	uv run ruff format .
	uv run ruff check --fix .
	@for d in $(TF_DIRS); do terraform -chdir=$$d fmt -recursive; done

.PHONY: test
test: setup ## Unit tests
	uv run pytest

.PHONY: check-env
check-env:
	@[ "$(ENV)" = local ] || { echo "ENV=$(ENV) is not implemented yet (GKE arrives in U8)"; exit 1; }

.PHONY: cluster
cluster: check-env
	@kind get clusters | grep -qx $(KIND_CLUSTER) || kind create cluster --config deploy/kind/cluster.yaml

.PHONY: check-pushed
check-pushed:
	@git fetch -q origin $(REVISION)
	@[ -z "$$(git log origin/$(REVISION)..$(REVISION) --oneline)" ] || \
		{ echo "Argo CD reads values from origin/$(REVISION): push local commits first"; exit 1; }

.PHONY: up
up: check-env check-pushed cluster ## Create the cluster, install Argo CD, sync every component (ENV=local)
	$(HELM) upgrade --install argocd argo-cd --repo https://argoproj.github.io/argo-helm \
		--version $(ARGOCD_CHART_VERSION) --namespace argocd --create-namespace \
		-f deploy/values/common/argocd.yaml --wait --timeout 10m
	@$(KUBECTL) create namespace monitoring --dry-run=client -o yaml | $(KUBECTL) apply -f - >/dev/null
	@$(KUBECTL) -n monitoring get secret grafana-admin >/dev/null 2>&1 || \
		$(KUBECTL) -n monitoring create secret generic grafana-admin --from-literal=admin-user=admin \
		--from-literal=admin-password="$$(openssl rand -base64 18)"
	sed -e 's/REVISION/$(REVISION)/g' -e 's/value: ENV/value: $(ENV)/' -e 's/"GITOPS"/"$(GITOPS_BOOL)"/' \
		deploy/argocd/root.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n argocd wait application/root --for=jsonpath='{.status.sync.status}'=Synced --timeout=$(WAIT_TIMEOUT)
	$(KUBECTL) -n argocd wait applications --all --for=jsonpath='{.status.sync.status}'=Synced --timeout=$(WAIT_TIMEOUT)
	$(KUBECTL) -n argocd wait applications --all --for=jsonpath='{.status.health.status}'=Healthy --timeout=$(WAIT_TIMEOUT)
	@[ "$(GITOPS_BOOL)" = true ] || $(MAKE) --no-print-directory dev
	$(KUBECTL) -n kafka wait kafka/qoe --for=condition=Ready --timeout=$(WAIT_TIMEOUT)
	@$(MAKE) --no-print-directory status

.PHONY: dev
dev: check-env ## Install or upgrade this repository's charts with Helm (fast loop, no push needed)
	$(foreach c,$(OWN_CHARTS),$(HELM) upgrade --install $(c) deploy/charts/$(c) \
		--namespace $(NS_$(c)) --create-namespace --wait --timeout 15m \
		$(if $(wildcard deploy/charts/$(c)/values-$(ENV).yaml),-f deploy/charts/$(c)/values-$(ENV).yaml) &&) true

.PHONY: smoke-registry
smoke-registry: check-env ## Round-trip a record through the schema registry with the Confluent client
	@$(KUBECTL) -n kafka port-forward svc/schema-registry 8081:8080 >/dev/null 2>&1 & pf=$$!; \
		trap "kill $$pf" EXIT; sleep 3; uv run python scripts/smoke_registry.py http://localhost:8081

.PHONY: status
status: check-env ## Show Argo CD applications and any pod that is not ready
	@$(KUBECTL) -n argocd get applications
	@echo "Pods not ready:"
	@$(KUBECTL) get pods -A --no-headers | awk '$$4 != "Completed" && ($$4 != "Running" || split($$3,a,"/") && a[1] != a[2])' || true

.PHONY: grafana
grafana: check-env ## Print the Grafana admin password and forward http://localhost:3000
	@echo "admin / $$($(KUBECTL) -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d)"
	$(KUBECTL) -n monitoring port-forward svc/grafana 3000:80

.PHONY: down
down: check-env ## Delete the cluster
	kind delete cluster --name $(KIND_CLUSTER)

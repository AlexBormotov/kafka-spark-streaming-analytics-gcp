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
	sed -e 's/REVISION/$(REVISION)/g' -e 's/value: ENV/value: $(ENV)/' deploy/argocd/root.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n argocd wait application/root --for=jsonpath='{.status.sync.status}'=Synced --timeout=$(WAIT_TIMEOUT)
	$(KUBECTL) -n argocd wait applications --all --for=jsonpath='{.status.sync.status}'=Synced --timeout=$(WAIT_TIMEOUT)
	$(KUBECTL) -n argocd wait applications --all --for=jsonpath='{.status.health.status}'=Healthy --timeout=$(WAIT_TIMEOUT)
	@$(MAKE) --no-print-directory status

.PHONY: status
status: check-env ## Show Argo CD applications and any pod that is not ready
	@$(KUBECTL) -n argocd get applications
	@echo "Pods not ready:"
	@$(KUBECTL) get pods -A --no-headers | awk '$$4 != "Completed" && ($$4 != "Running" || split($$3,a,"/") && a[1] != a[2])' || true

.PHONY: down
down: check-env ## Delete the cluster
	kind delete cluster --name $(KIND_CLUSTER)

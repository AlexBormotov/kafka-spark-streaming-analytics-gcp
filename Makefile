# One command per task. CI runs these same targets.
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

ENV ?= local
GCP_PROJECT ?= qoe-spark-streaming
GCP_REGION ?= us-central1

TF_DIRS := $(wildcard infra/terraform/*)
CHART_DIRS := $(wildcard deploy/charts/*)

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

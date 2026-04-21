ifneq (,$(wildcard .env))
include .env
export
endif

KESTRA_URL ?= http://localhost:8082
STREAMLIT_URL ?= http://localhost:8502
KESTRA_ADMIN_EMAIL ?= kestra@kestra.com
KESTRA_ADMIN_PASSWORD ?= Kestra1234

.PHONY: help \
	check-env check-terraform-vars \
	terraform-init terraform-plan terraform-apply terraform-destroy terraform-output \
	up down restart logs ps clean \
	kestra-url streamlit-url open \
	dbt-test batch-instructions \
	wait-kestra deploy-flows trigger-batch \
	run-local run-cloud destroy-cloud

help:
	@echo ""
	@echo "UK EV Takeup Data Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make check-env             Check required .env keys exist"
	@echo "  make check-terraform-vars  Check terraform/terraform.tfvars exists"
	@echo ""
	@echo "  make terraform-init        Initialize Terraform in ./terraform"
	@echo "  make terraform-plan        Preview infrastructure changes"
	@echo "  make terraform-apply       Apply infrastructure changes"
	@echo "  make terraform-destroy     Destroy infrastructure defined by Terraform"
	@echo "  make terraform-output      Show Terraform outputs"
	@echo ""
	@echo "  make up                    Start local stack, wait for Kestra, deploy flows, trigger batch"
	@echo "  make down                  Stop the local stack"
	@echo "  make restart               Restart the local stack, redeploy flows, trigger batch"
	@echo "  make logs                  Follow docker compose logs"
	@echo "  make ps                    Show running services"
	@echo "  make clean                 Stop containers and remove volumes"
	@echo ""
	@echo "  make kestra-url            Print local Kestra URL"
	@echo "  make streamlit-url         Print local Streamlit URL"
	@echo "  make open                  Print both local app URLs"
	@echo ""
	@echo "  make dbt-test              Run dbt tests for marts locally"
	@echo "  make batch-instructions    Print how the batch flow is triggered"
	@echo "  make wait-kestra           Wait for local Kestra API to become reachable"
	@echo "  make deploy-flows          Deploy flow YAML files to Kestra via API"
	@echo "  make trigger-batch         Trigger batch_uk_ev_pipeline on local Kestra"
	@echo ""
	@echo "  make run-local             Alias for full local pipeline bootstrap"
	@echo "  make run-cloud             Apply cloud infrastructure and print cloud URLs"
	@echo "  make destroy-cloud         Destroy cloud infrastructure"
	@echo ""

check-env:
	@echo "Checking .env file..."
	@test -f .env || (echo "Missing .env file. Create one first." && exit 1)
	@grep -q "^GCP_PROJECT_ID=" .env || (echo "Missing GCP_PROJECT_ID in .env" && exit 1)
	@grep -q "^GCS_BUCKET=" .env || (echo "Missing GCS_BUCKET in .env" && exit 1)
	@grep -q "^LOCAL_RAW_DIR=" .env || (echo "Missing LOCAL_RAW_DIR in .env" && exit 1)
	@grep -q "^LOCAL_PREPARED_DIR=" .env || (echo "Missing LOCAL_PREPARED_DIR in .env" && exit 1)
	@grep -q "^BANK_OF_ENGLAND_EUR_GBP_FX_URL=" .env || (echo "Missing BANK_OF_ENGLAND_EUR_GBP_FX_URL in .env" && exit 1)
	@grep -q "^EUROPEAN_WHOLESALE_ELECTRICITY_PRICES_URL=" .env || (echo "Missing EUROPEAN_WHOLESALE_ELECTRICITY_PRICES_URL in .env" && exit 1)
	@grep -q "^DESNZ_PETROLEUM_PRODUCTS_PRICES_URL=" .env || (echo "Missing DESNZ_PETROLEUM_PRODUCTS_PRICES_URL in .env" && exit 1)
	@grep -q "^DVLA_VEH1103_URL=" .env || (echo "Missing DVLA_VEH1103_URL in .env" && exit 1)
	@grep -q "^DVLA_VEH1153_URL=" .env || (echo "Missing DVLA_VEH1153_URL in .env" && exit 1)
	@grep -q "^KESTRA_ADMIN_EMAIL=" .env || (echo "Missing KESTRA_ADMIN_EMAIL in .env" && exit 1)
	@grep -q "^KESTRA_ADMIN_PASSWORD=" .env || (echo "Missing KESTRA_ADMIN_PASSWORD in .env" && exit 1)
	@echo "OK: required .env keys are present."

check-terraform-vars:
	@echo "Checking terraform variables file..."
	@test -f terraform/terraform.tfvars || (echo "Missing terraform/terraform.tfvars" && exit 1)
	@echo "OK: terraform/terraform.tfvars exists."

terraform-init: check-terraform-vars
	terraform -chdir=terraform init

terraform-plan: check-terraform-vars
	terraform -chdir=terraform plan

terraform-apply: check-terraform-vars
	terraform -chdir=terraform apply -auto-approve

terraform-destroy: check-terraform-vars
	terraform -chdir=terraform destroy -auto-approve

terraform-output: check-terraform-vars
	terraform -chdir=terraform output

up: check-env
	docker compose up -d --build
	$(MAKE) wait-kestra
	$(MAKE) deploy-flows
	$(MAKE) trigger-batch
	@echo ""
	@echo "Local pipeline run started."
	@echo "Kestra UI:     $(KESTRA_URL)"
	@echo "Streamlit app: $(STREAMLIT_URL)"
	@echo ""

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d --build
	$(MAKE) wait-kestra
	$(MAKE) deploy-flows
	$(MAKE) trigger-batch
	@echo ""
	@echo "Local pipeline restarted."
	@echo "Kestra UI:     $(KESTRA_URL)"
	@echo "Streamlit app: $(STREAMLIT_URL)"
	@echo ""

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v

kestra-url:
	@echo "Kestra UI: $(KESTRA_URL)"

streamlit-url:
	@echo "Streamlit app: $(STREAMLIT_URL)"

open:
	@echo "Kestra UI: $(KESTRA_URL)"
	@echo "Streamlit app: $(STREAMLIT_URL)"

dbt-test:
	cd dbt/uk_ev_takeup && uv run dbt test --select marts

batch-instructions:
	@echo ""
	@echo "The batch flow is triggered automatically by 'make up'."
	@echo "Manual endpoint:"
	@echo "  POST $(KESTRA_URL)/api/v1/main/executions/uk_ev_takeup/batch_uk_ev_pipeline"
	@echo ""

wait-kestra:
	@echo "Waiting for local Kestra API to become reachable..."
	@until curl -fsS -u "$(KESTRA_ADMIN_EMAIL):$(KESTRA_ADMIN_PASSWORD)" \
		"$(KESTRA_URL)/" >/dev/null 2>&1; do \
		echo "Kestra not ready yet..."; \
		sleep 5; \
	done
	@echo "Kestra is up."

deploy-flows:
	@echo "Deploying Kestra flows..."
	@chmod +x scripts/deploy_kestra_flows.sh
	@KESTRA_URL="$(KESTRA_URL)" \
	KESTRA_USER="$(KESTRA_ADMIN_EMAIL)" \
	KESTRA_PASS="$(KESTRA_ADMIN_PASSWORD)" \
	bash scripts/deploy_kestra_flows.sh
	@echo "Kestra flows deployed."

trigger-batch:
	@echo "Triggering Kestra batch flow on local Kestra..."
	@curl -fsS -X POST \
		-u "$(KESTRA_ADMIN_EMAIL):$(KESTRA_ADMIN_PASSWORD)" \
		"$(KESTRA_URL)/api/v1/main/executions/uk_ev_takeup/batch_uk_ev_pipeline" >/dev/null
	@echo "Batch flow triggered: uk_ev_takeup.batch_uk_ev_pipeline"

run-local: up

run-cloud: check-terraform-vars terraform-apply
	@echo ""
	@echo "Cloud infrastructure applied."
	@echo "Kestra URL:    $$(terraform -chdir=terraform output -raw cloud_kestra_url 2>/dev/null || true)"
	@echo "Streamlit URL: $$(terraform -chdir=terraform output -raw cloud_streamlit_url 2>/dev/null || true)"
	@echo ""

destroy-cloud: check-terraform-vars terraform-destroy
	@echo ""
	@echo "Cloud infrastructure destroyed."
	@echo ""

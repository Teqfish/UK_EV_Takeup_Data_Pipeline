ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: help \
	check-env check-terraform-vars \
	terraform-init terraform-plan terraform-apply terraform-destroy terraform-output \
	up down restart logs ps clean \
	kestra-url streamlit-url open \
	dbt-test batch-instructions \
	wait-kestra trigger-batch \
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
	@echo "  make up                    Start the local stack (Kestra, Postgres, Streamlit)"
	@echo "  make down                  Stop the local stack"
	@echo "  make restart               Restart the local stack"
	@echo "  make logs                  Follow docker compose logs"
	@echo "  make ps                    Show running services"
	@echo "  make clean                 Stop containers and remove volumes"
	@echo ""
	@echo "  make kestra-url            Print local Kestra URL"
	@echo "  make streamlit-url         Print local Streamlit URL"
	@echo "  make open                  Print both local app URLs"
	@echo ""
	@echo "  make dbt-test              Run dbt tests for marts locally"
	@echo "  make batch-instructions    Print how to run the batch flow manually"
	@echo "  make wait-kestra           Wait for local Kestra to become reachable"
	@echo "  make trigger-batch         Trigger batch_uk_ev_pipeline on local Kestra"
	@echo ""
	@echo "  make run-local             Start local stack, wait for Kestra, trigger batch flow"
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
	@echo ""
	@echo "Stack started."
	@echo "Kestra UI:     http://localhost:8082"
	@echo "Streamlit app: http://localhost:8502"
	@echo ""

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d --build

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v

kestra-url:
	@echo "Kestra UI: http://localhost:8082"

streamlit-url:
	@echo "Streamlit app: http://localhost:8502"

open:
	@echo "Kestra UI: http://localhost:8082"
	@echo "Streamlit app: http://localhost:8502"

dbt-test:
	cd dbt/uk_ev_takeup && uv run dbt test --select marts

batch-instructions:
	@echo ""
	@echo "Run the full batch pipeline manually:"
	@echo "  1. Open Kestra: http://localhost:8082"
	@echo "  2. Log in"
	@echo "  3. Open flow: batch_uk_ev_pipeline"
	@echo "  4. Click Execute"
	@echo "  5. Wait for all subflows, mart builds, and tests to pass"
	@echo "  6. Open Streamlit: http://localhost:8502"
	@echo ""

wait-kestra:
	@echo "Waiting for local Kestra to become reachable..."
	@until curl -fsS -u "$(KESTRA_ADMIN_EMAIL):$(KESTRA_ADMIN_PASSWORD)" http://localhost:8082 >/dev/null 2>&1; do \
		echo "Kestra not ready yet..."; \
		sleep 5; \
	done
	@echo "Kestra is up."

trigger-batch:
	@echo "Triggering Kestra batch flow on local Kestra..."
	@curl -fsS -X POST \
		-u "$(KESTRA_ADMIN_EMAIL):$(KESTRA_ADMIN_PASSWORD)" \
		http://localhost:8082/api/v1/executions/uk_ev_takeup/batch_uk_ev_pipeline >/dev/null
	@echo "Batch flow triggered: uk_ev_takeup.batch_uk_ev_pipeline"

run-local: check-env up wait-kestra trigger-batch
	@echo ""
	@echo "Local pipeline run started."
	@echo "Kestra UI:     http://localhost:8082"
	@echo "Streamlit app: http://localhost:8502"
	@echo ""
	@echo "Next:"
	@echo "  1. Open Kestra to monitor flow: uk_ev_takeup.batch_uk_ev_pipeline"
	@echo "  2. Wait for completion"
	@echo "  3. Open Streamlit"
	@echo ""

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

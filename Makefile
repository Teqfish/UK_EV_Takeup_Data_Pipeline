.PHONY: help check-env setup-gcp up down restart logs ps \
	kestra-url streamlit-url open \
	dbt-test batch-instructions clean

help:
	@echo ""
	@echo "UK EV Takeup Data Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make check-env          Check whether required .env keys exist"
	@echo "  make setup-gcp          Print the required one-time GCP setup steps"
	@echo "  make up                 Start the local stack (Kestra, Postgres, Streamlit)"
	@echo "  make down               Stop the local stack"
	@echo "  make restart            Restart the local stack"
	@echo "  make logs               Follow docker compose logs"
	@echo "  make ps                 Show running services"
	@echo "  make kestra-url         Print Kestra URL"
	@echo "  make streamlit-url      Print Streamlit URL"
	@echo "  make open               Print both local app URLs"
	@echo "  make dbt-test           Run dbt tests for marts locally"
	@echo "  make batch-instructions Print how to run the full pipeline in Kestra"
	@echo "  make clean              Stop containers and remove volumes"
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
	@echo "OK: required .env keys are present."

setup-gcp:
	@echo ""
	@echo "One-time assessor setup:"
	@echo "  1. Create a GCP project"
	@echo "  2. Enable BigQuery and Cloud Storage APIs"
	@echo "  3. Create a GCS bucket"
	@echo "  4. Update .env with the project ID, bucket name, and source URLs"
	@echo "  5. Authenticate locally:"
	@echo "       gcloud auth application-default login"
	@echo "  6. Ensure your ADC file exists at:"
	@echo "       ~/.config/gcloud/application_default_credentials.json"
	@echo ""

up: check-env
	docker compose up -d --build
	@echo ""
	@echo "Stack started."
	@echo "Kestra UI:     http://localhost:8080"
	@echo "Streamlit app: http://localhost:8501"
	@echo ""
	@echo "Next:"
	@echo "  1. Open Kestra"
	@echo "  2. Run flow: uk_ev_takeup.batch_uk_ev_pipeline"
	@echo "  3. Open Streamlit"
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

kestra-url:
	@echo "Kestra UI: http://localhost:8080"

streamlit-url:
	@echo "Streamlit app: http://localhost:8501"

open:
	@echo "Kestra UI: http://localhost:8080"
	@echo "Streamlit app: http://localhost:8501"

dbt-test:
	cd dbt/uk_ev_takeup && uv run dbt test --select marts

batch-instructions:
	@echo ""
	@echo "Run the full batch pipeline:"
	@echo "  1. Open Kestra: http://localhost:8080"
	@echo "  2. Log in"
	@echo "  3. Open flow: batch_uk_ev_pipeline"
	@echo "  4. Click Execute"
	@echo "  5. Wait for all subflows, mart builds, and tests to pass"
	@echo "  6. Open Streamlit: http://localhost:8501"
	@echo ""

clean:
	docker compose down -v

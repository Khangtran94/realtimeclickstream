.PHONY: help install tf-init tf-apply tf-destroy simulate batch logs table clean

help:
	@echo "Available commands:"
	@echo "  make install       Install Python dependencies (uv)"
	@echo "  make tf-init       terraform init"
	@echo "  make tf-apply      Deploy all AWS resources"
	@echo "  make tf-destroy    Destroy all AWS resources (saves money)"
	@echo "  make simulate      Ingest sample events into Kinesis"
	@echo "  make batch         Run local Spark batch job"
	@echo "  make logs          Tail Lambda CloudWatch logs"
	@echo "  make table         Show a few items from DynamoDB"
	@echo "  make clean         Remove local build artifacts"

install:
	uv sync

tf-init:
	cd terraform && terraform init

tf-apply:
	cd terraform && terraform apply

tf-destroy:
	cd terraform && terraform destroy

# Ingest a limited number of rows so learning is fast and cheap
simulate:
	python src/event_simulator/ingest_to_kinesis.py --folder data --max-rows 20000

batch:
	python src/batch/batch_session_job.py \
		--input data/2019-Oct.csv \
		--output data/output/sessions \
		--max-rows 50000

logs:
	aws logs tail /aws/lambda/clickstream-sessionizer --follow --region ap-southeast-1

table:
	aws dynamodb scan \
		--table-name clickstream-sessions \
		--limit 5 \
		--region ap-southeast-1 \
		--output table

clean:
	rm -rf data/output __pycache__ src/**/__pycache__ terraform/sessionizer.zip



test:
	go install gotest.tools/gotestsum@latest
	docker-compose up -d
	gotestsum --format testname ./...
	docker-compose down

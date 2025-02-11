
test:
	go test ./... -count 1 -race

vet:
	go vet ./...

cover:
	go test ./... -count 1 -race -cover

gen:
	go generate ./...

plantuml:
	docker run -v $(shell pwd)/docs:/docs -w /docs ghcr.io/plantuml/plantuml *.pu


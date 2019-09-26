VERSION:=$(shell git tag -l | tail -n1 | tr -d 'v')

default: vet test

test:
	go test ./...

vet:
	go vet ./...

DOCKER_NAME=blacksquaremedia/rumour

docker-build:
	docker build -t ${DOCKER_NAME}:${VERSION} .
	docker tag ${DOCKER_NAME}:${VERSION} ${DOCKER_NAME}:latest

docker-push: docker-build
	docker push ${DOCKER_NAME}

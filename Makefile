VERSION=0.2.1

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

NAME = k-hop

build:
	docker build -t ${NAME} .

test: build
	docker run --rm ${NAME} poetry run pytest -v ./tests/

run: build
	docker run --rm -v $(shell pwd):/app ${NAME} poetry run python ./app.py ${ARGS}

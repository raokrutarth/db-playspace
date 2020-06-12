
build-and-start:
	docker-compose up --build --detach
	./enter-dev

enter-postgres:
	- echo TODO

start:
	docker-compose up --detach
	./enter-dev
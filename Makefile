app:
	go run main.go

docker_up:
	# Levanta el contenedor de la app
	docker compose up --build -d
	docker exec -it app_tarjetasCredito /bin/sh

docker_down:
	# Detiene el contenedor de la app y elimna la imagen
	docker compose down
	docker rmi bd1-tptarjetascredito-app:latest

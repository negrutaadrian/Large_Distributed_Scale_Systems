## Start Docker Compose 
docker compose build 
docker compose up 

## Build source code 
mvn clean package 

## Local Path Mount in docker-compose.yml
### namenode service
volumes parameter needs to be mounted to your local path 

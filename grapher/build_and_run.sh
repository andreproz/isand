docker build -t grapher:latest .

docker stop grapher
docker rm grapher
docker rm $(docker ps -aq -f status=exited)

docker image prune -f
docker run --name grapher -p 0.0.0.0:5002:5002 -d grapher:latest
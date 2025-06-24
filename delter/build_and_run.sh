docker stop delter
docker rm delter
docker rmi delter:latest

docker build -t delter:latest .
docker run --name delter -p 0.0.0.0:5001:5001 -d delter:latest
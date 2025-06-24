#!/bin/bash

# Строим образ с помощью docker-compose
docker build -t grapher_tmp .

docker save -o grapher.tar grapher_tmp

# Копируем архив opensearch-api.tar на удаленный сервер

scp -i /home/mxcitn/.ssh/mxcitn /home/mxcitn/projects/grapher/grapher.tar isand_user@193.232.208.28:~/grapher

# Выполняем скрипт на удаленном сервере
ssh -i /home/mxcitn/.ssh/mxcitn isand_user@193.232.208.28 'bash -s' << 'ENDSSH'
#!/bin/bash

# Останавливаем, удаляем и пересоздаем контейнер с именем "grapher"
if [ "$(docker ps -a -q -f name=grapher)" ]; then
    docker stop $(docker ps -a --filter "name=grapher" --format "{{.ID}}")
    docker rm $(docker ps -a --filter "name=grapher" --format "{{.ID}}")
else
    echo "Containers not found"
fi
if [ "$(docker image ls -q -f=reference=grapher)" ]; then
    docker rmi $(docker image ls -q -f=reference=grapher)
else
    echo "Images not found"
fi
docker load -i ~/grapher
docker run -d -p 5002:5002 --name grapher --restart=always grapher
ENDSSH

# Удаляем образ и архив с именем "grapher"
docker rmi grapher_tmp
rm -r grapher.tar
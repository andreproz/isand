#!/bin/bash

# Строим образ с помощью docker-compose
docker build -t opensearch_api .

docker save -o opensearch_api.tar opensearch_api

# Копируем архив opensearch-api.tar на удаленный сервер

scp -i ~/.ssh/baozorp ./opensearch_api.tar isand_user@193.232.208.28:~/opensearch_api

# Выполняем скрипт на удаленном сервере
ssh -i ~/.ssh/baozorp isand_user@193.232.208.28 'bash -s' << 'ENDSSH'
#!/bin/bash

# Останавливаем, удаляем и пересоздаем контейнер с именем "opensearch_api"
if [ "$(docker ps -a -q -f name=opensearch_api)" ]; then
    docker stop $(docker ps -a --filter "name=opensearch_api" --format "{{.ID}}")
    docker rm $(docker ps -a --filter "name=opensearch_api" --format "{{.ID}}")
else
    echo "Containers doesn't found"
fi
if [ "$(docker image ls -q -f=reference=opensearch_api)" ]; then
    docker rmi $(docker image ls -q -f=reference=opensearch_api)
else
    echo "Images doesn't found"
fi

docker load -i ~/opensearch_api/opensearch_api.tar
docker run -d -p 9200:9200 --name opensearch_api --restart=always opensearch_api
ENDSSH

# Удаляем образ и архив с именем "opensearch_api"
docker rmi opensearch_api
rm -r opensearch_api.tar
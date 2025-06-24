#!/bin/bash

# Строим образ с помощью docker-compose
docker build -t delter_tmp .

docker save -o delter.tar delter_tmp

# Копируем архив opensearch-api.tar на удаленный сервер

scp -i /home/mxcitn/.ssh/mxcitn /home/mxcitn/projects/delter/delter.tar isand_user@193.232.208.28:~/delter

# Выполняем скрипт на удаленном сервере
ssh -i /home/mxcitn/.ssh/mxcitn isand_user@193.232.208.28 'bash -s' << 'ENDSSH'
#!/bin/bash

# Останавливаем, удаляем и пересоздаем контейнер с именем "delter"
if [ "$(docker ps -a -q -f name=delter)" ]; then
    docker stop $(docker ps -a --filter "name=delter" --format "{{.ID}}")
    docker rm $(docker ps -a --filter "name=delter" --format "{{.ID}}")
else
    echo "Containers not found"
fi
if [ "$(docker image ls -q -f=reference=delter)" ]; then
    docker rmi $(docker image ls -q -f=reference=delter)
else
    echo "Images not found"
fi
docker load -i ~/delter
docker run -d -p 5001:5001 --name delter --restart=always delter
ENDSSH

# Удаляем образ и архив с именем "delter"
docker rmi delter_tmp
rm -r delter.tar
FROM python:3.12

ENV IN_DOCKER_CONTAINER=true
EXPOSE 9200
WORKDIR /opensearch-api
COPY ./requirements.txt /opensearch-api/requirements.txt
RUN pip install -r /opensearch-api/requirements.txt
COPY ./rest-api /opensearch-api
ENTRYPOINT ["uvicorn", "main:app","--port", "9200", "--host", "0.0.0.0", "--env-file", ".env"]
FROM python:3.12

WORKDIR /home/mxcitn/projects/grapher
COPY . .

RUN pip install -r requirements.txt

EXPOSE 5002

ENTRYPOINT ["uvicorn", "grapher:app","--port", "5002", "--host", "0.0.0.0"]
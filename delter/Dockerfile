FROM python:3.12

WORKDIR /home/mxcitn/projects/delter
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5001

ENTRYPOINT ["uvicorn", "delter:app","--port", "5001", "--host", "0.0.0.0"]
FROM python:3.11.5

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5005

CMD ["uvicorn", "filewatcher:app", "--host", "0.0.0.0", "--port", "5005"]
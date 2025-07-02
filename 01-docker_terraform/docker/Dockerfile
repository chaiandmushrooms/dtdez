FROM python:latest

RUN pip install pandas pyarrow wget psycopg2 sqlalchemy
WORKDIR /app
COPY ingest.py ingest.py
ENTRYPOINT [ "python", "ingest.py"]
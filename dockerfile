FROM python:latest

ENV path=/app/yellow_tripdata_2024-01.parquet \
    port=5432 \
    pass=d1m45 \
    db=db1 \
    user=dimas

RUN pip install pandas sqlalchemy argparse psycopg2 pyarrow
RUN mkdir app

COPY data.py /app
COPY yellow_tripdata_2024-01.parquet /app

WORKDIR /app

CMD python data.py -pth ${path} -p ${port} -pass ${pass} -db ${db} -u ${user}
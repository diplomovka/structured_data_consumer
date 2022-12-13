FROM python:3.8.16-slim-bullseye

# fix psycopg2 error in linux
RUN apt-get update && apt-get -y install libpq-dev gcc 

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r requirements.txt

COPY . ./

CMD [ "python", "./structured_data_consumer.py" ]
# created based on: https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_consumer.py

import time
import json
import traceback
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from serialization_classes.structured_data import StructuredData
import settings
from redis_db import get_redis_db
from postgres_db import conn


def dict_to_structured_data(obj, ctx):
    if obj is None:
        return None

    return StructuredData(data_hash=obj['data_hash'], table_name=obj['table_name'], insert_query=obj['insert_query'],
                        columns_names=obj['columns_names'], table_creation_queries=obj['table_creation_queries'])


def set_up_consumer():
    schema_registry_conf = {'url': settings.SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    with open(settings.STRUCTURED_DATA_SCHEMA_PATH, 'r', encoding=settings.AVRO_FILES_ENCODING) as f:
        schema_str = f.read()

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_structured_data)
    string_deserializer = StringDeserializer(settings.ENCODING)

    topic = settings.STRUCTURED_DATA_TOPIC
    consumer_conf = {
        'bootstrap.servers': settings.BOOTSTRAP_SERVERS,
        'key.deserializer': string_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': settings.GROUP_ID,
        'auto.offset.reset': settings.OFFSET
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    return consumer


def get_attributes(structured_data):
    return (structured_data.data_hash, structured_data.table_name, structured_data.insert_query,
        structured_data.columns_names, structured_data.table_creation_queries)


def check_if_table_contains_required_cols(query_results, table_name):
    for column_name, _ in query_results: # _ is column type
        if column_name not in columns_names:
            print(f'Unable to insert data in {table_name}, because this table already exists, but with different columns.')
            return True

    return False


if __name__ == '__main__':
    time.sleep(30)

    consumer = set_up_consumer()
    redis_db = get_redis_db(settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_STRUCTURED_DATA_DB)

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1)
            if msg is None or msg.value() is None:
                continue

            structured_data = msg.value()
            data_hash, table_name, insert_query, columns_names, table_creation_queries = get_attributes(structured_data)

            print(f'{msg.key()} StructuredData {data_hash}, {table_name}, {columns_names}, {insert_query}')

            table_exists_with_diff_cols_flag = False

            cursor = conn.cursor()
            table_name_without_schema = table_name.split('.')[-1]
            cursor.execute('SELECT column_name, data_type FROM information_schema.columns WHERE table_name=(%s)', (table_name_without_schema, ))
            results = cursor.fetchall()
            if len(results) == 0:
                # table isn't created, execute all queries for creation
                for sql_query in table_creation_queries:
                    cursor.execute(sql_query)

            else:
                # table with this name exists, but check if it contains all required columns
                table_exists_with_diff_cols_flag = check_if_table_contains_required_cols(results, table_name)

            conn.commit()
            cursor.close()

            if table_exists_with_diff_cols_flag or data_hash is None:
                # table with this table_name already exists, but it doesn't contains required columns
                # or table was created without data, which is allowed and necessary scenario
                continue

            pointer = redis_db.get(data_hash)
            # check if data is unique
            if pointer:
                # pointer already exists
                continue

            cursor = conn.cursor()

            # store received data
            cursor.execute(insert_query)

            # NOTE: table name in pointer should be enough in current state
            pointer = json.dumps({
                'table_name': table_name,
            }).encode('utf-8')

            # update pointer in redis
            redis_db.set(data_hash, pointer)

            conn.commit()

            cursor.close()

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(traceback.print_exc())

    consumer.close()

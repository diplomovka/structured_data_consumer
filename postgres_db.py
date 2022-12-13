import psycopg2
import settings

conn = psycopg2.connect(
    host=settings.POSTGRES_HOST,
    user=settings.POSTGRES_USER,
    password=settings.POSTGRES_PASS,
    port=settings.POSTGRES_PORT,
    database=settings.POSTGRES_DB
)

import os

os.environ.update(
    {
        "API_KEY": "test-api-key",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": "test",
        "POSTGRES_PASSWORD": "test",
        "POSTGRES_DB": "test",
        "RABBITMQ_URL": "amqp://guest:guest@localhost:5672/",
        "GATEWAY_MIN_DELAY_SECONDS": "0",
        "GATEWAY_MAX_DELAY_SECONDS": "0",
        "CONSUMER_RETRY_BASE_DELAY_SECONDS": "0.2",
    },
)

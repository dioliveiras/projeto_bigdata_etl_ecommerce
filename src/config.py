import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "pedidos_ecommerce")

# PostgreSQL
POSTGRES_CONN = os.getenv("POSTGRES_CONN", "postgresql://app:app@localhost:5432/ecommerce")

# API (exemplo)
ECOM_API_BASE = os.getenv("ECOM_API_BASE", "https://api.minhaloja.com")
ECOM_API_KEY = os.getenv("ECOM_API_KEY", "SUA_CHAVE_AQUI")

import json
import argparse
import urllib.parse as up

from kafka import KafkaConsumer
import psycopg  # psycopg v3

from src.config import KAFKA_BOOTSTRAP_SERVERS, POSTGRES_CONN, KAFKA_TOPIC
from src.utils.logger import get_logger

logger = get_logger("kafka-consumer")

DDL = '''
CREATE TABLE IF NOT EXISTS pedidos (
  pedido_id TEXT PRIMARY KEY,
  data_pedido TIMESTAMP,
  cliente_id TEXT,
  produto_id TEXT,
  categoria TEXT,
  qtde DOUBLE PRECISION,
  preco_unitario DOUBLE PRECISION,
  valor_total DOUBLE PRECISION,
  uf TEXT,
  cidade TEXT,
  canal_venda TEXT,
  status_pedido TEXT
);
'''

INSERT = '''
INSERT INTO pedidos (pedido_id, data_pedido, cliente_id, produto_id, categoria,
                     qtde, preco_unitario, valor_total, uf, cidade, canal_venda, status_pedido)
VALUES (%(pedido_id)s, %(data_pedido)s, %(cliente_id)s, %(produto_id)s, %(categoria)s,
        %(qtde)s, %(preco_unitario)s, %(valor_total)s, %(uf)s, %(cidade)s, %(canal_venda)s, %(status_pedido)s)
ON CONFLICT (pedido_id) DO UPDATE SET
  data_pedido=EXCLUDED.data_pedido,
  cliente_id=EXCLUDED.cliente_id,
  produto_id=EXCLUDED.produto_id,
  categoria=EXCLUDED.categoria,
  qtde=EXCLUDED.qtde,
  preco_unitario=EXCLUDED.preco_unitario,
  valor_total=EXCLUDED.valor_total,
  uf=EXCLUDED.uf,
  cidade=EXCLUDED.cidade,
  canal_venda=EXCLUDED.canal_venda,
  status_pedido=EXCLUDED.status_pedido;
'''

def _connect_pg():
    url = up.urlparse(POSTGRES_CONN or "")
    return psycopg.connect(
        dbname=(url.path.lstrip("/") or "ecommerce"),
        user=(url.username or "app"),
        password=(url.password or "app"),
        host=(url.hostname or "localhost"),
        port=(url.port or 5432),
        autocommit=False,
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=KAFKA_TOPIC)
    ap.add_argument("--group", default="ecommerce-consumers")
    args = ap.parse_args()

    conn = _connect_pg()
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode("utf-8"),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        consumer_timeout_ms=15000,
    )

    batch = []
    try:
        with conn.cursor() as cur:
            for msg in consumer:
                try:
                    key = msg.key
                    data = json.loads(msg.value)
                    row = {"pedido_id": key, **data}
                    batch.append(row)

                    if len(batch) >= 1000:
                        cur.executemany(INSERT, batch)
                        conn.commit()
                        logger.info(f"Persistidos {len(batch)} registros.")
                        batch.clear()
                except Exception as e:
                    logger.exception(f"Falha ao processar mensagem: {e}")

            # flush final
            if batch:
                cur.executemany(INSERT, batch)
                conn.commit()
                logger.info(f"Persistidos {len(batch)} registros finais.")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()

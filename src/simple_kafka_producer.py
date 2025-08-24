import csv, json, argparse, time
from kafka import KafkaProducer
from src.config import KAFKA_BOOTSTRAP_SERVERS

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--topic", required=True)
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        linger_ms=50,
        retries=3,
    )

    with open(args.csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        sent = 0
        for row in reader:
            key = row.get("pedido_id", f"semkey-{sent}")
            # garante tipos mínimos coerentes (opcional)
            payload = json.dumps({
                "data_pedido": row.get("data_pedido"),
                "cliente_id": row.get("cliente_id"),
                "produto_id": row.get("produto_id"),
                "categoria": row.get("categoria"),
                "qtde": float(row["qtde"]) if row.get("qtde") else None,
                "preco_unitario": float(row["preco_unitario"]) if row.get("preco_unitario") else None,
                "valor_total": float(row["valor_total"]) if row.get("valor_total") else None,
                "uf": row.get("uf"),
                "cidade": row.get("cidade"),
                "canal_venda": row.get("canal_venda"),
                "status_pedido": row.get("status_pedido"),
            })
            producer.send(args.topic, key=key, value=payload)
            sent += 1
        producer.flush()
    print(f"Enviadas {sent} mensagens para o tópico {args.topic}")

if __name__ == "__main__":
    main()

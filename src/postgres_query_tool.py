import argparse, csv
import psycopg2
import pandas as pd
from src.config import POSTGRES_CONN
from src.utils.logger import get_logger

logger = get_logger("pg-tool")

def run_query(sql: str, out: str | None):
    with psycopg2.connect(POSTGRES_CONN) as conn:
        df = pd.read_sql_query(sql, conn)
    if out:
        df.to_csv(out, index=False)
        logger.info(f"Resultado salvo em {out}")
    else:
        print(df.head(20).to_string(index=False))

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    top_products = sub.add_parser("top-products", help="Top produtos por volume de pedidos")
    top_products.add_argument("--limit", type=int, default=10)
    top_products.add_argument("--out", default=None)

    top_cities = sub.add_parser("top-cities", help="Top cidades por volume de pedidos")
    top_cities.add_argument("--limit", type=int, default=10)
    top_cities.add_argument("--out", default=None)

    args = ap.parse_args()

    if args.cmd == "top-products":
        sql = f"""SELECT produto_id, COUNT(*) AS pedidos, SUM(valor_total) AS receita
FROM pedidos
GROUP BY produto_id
ORDER BY pedidos DESC
LIMIT {args.limit};
"""
        run_query(sql, args.out)

    elif args.cmd == "top-cities":
        sql = f"""SELECT cidade, uf, COUNT(*) AS pedidos, SUM(valor_total) AS receita
FROM pedidos
GROUP BY cidade, uf
ORDER BY pedidos DESC
LIMIT {args.limit};
"""
        run_query(sql, args.out)

if __name__ == "__main__":
    main()

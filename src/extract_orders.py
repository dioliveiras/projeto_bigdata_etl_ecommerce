"""Exemplo de extração: demonstra uso de requests para coletar pedidos.
Para testes locais, use o CSV sintético já incluído em data/pedidos.csv.
"""
import csv, time, argparse, requests
from pathlib import Path
from src.config import ECOM_API_BASE, ECOM_API_KEY
from src.utils.logger import get_logger

logger = get_logger("extract-orders")

def fetch_orders(page: int = 1, page_size: int = 100):
    url = f"{ECOM_API_BASE}/orders?page={page}&page_size={page_size}"
    headers = {"Authorization": f"Bearer {ECOM_API_KEY}"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest", default="data/pedidos.csv")
    args = ap.parse_args()

    # Exemplo hipotético (paginado); comente caso não tenha API
    # rows, page = [], 1
    # while True:
    #     j = fetch_orders(page=page)
    #     items = j.get("items", [])
    #     if not items: break
    #     rows.extend(items)
    #     page += 1
    #     time.sleep(0.2)

    logger.info("Este script demonstra a extração. Para testes use data/pedidos.csv já incluído.")
    Path(args.dest).parent.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    main()

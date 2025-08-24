# Projeto Big Data UNIFOR – Pipeline de Dados E-commerce

Este projeto foi desenvolvido para a disciplina de Big Data na Pós-Graduação em Engenharia de Dados (UNIFOR). Ele simula um **pipeline de dados de pedidos de e-commerce** usando **Python, Kafka (KRaft), Postgres e Docker**.

A ideia é: carregar pedidos (via CSV), enviar para o Kafka, consumir os eventos com Python e salvar no Postgres. Depois é possível consultar os pedidos para análises.

---

## Estrutura do Projeto
```
pipeline_ecommerce/
├── data/
│   └── pedidos_teste.csv       # Dataset de exemplo (pedidos)
├── src/
│   ├── simple_kafka_producer.py# Producer simples (envia CSV para Kafka)
│   ├── kafka_consumer.py       # Consumer (lê do Kafka e grava no Postgres)
│   └── utils/                  # Funções utilitárias (logs, etc)
├── docker-compose.yaml         # Sobe Kafka (KRaft) e Postgres
├── start_pipeline.ps1          # Script PowerShell de automação
├── requirements.txt            # Dependências Python
└── README.md
```

---

## Pré-requisitos
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Python 3.12+](https://www.python.org/downloads/)
- Git instalado

---

## Como rodar

1. Clone o projeto:
   ```powershell
   git clone https://github.com/dioliveiras/projeto_bigdata.git
   cd projeto_bigdata/pipeline_ecommerce
   ```

2. Execute o script de inicialização:
   ```powershell
   .\start_pipeline.ps1
   ```

   O script faz automaticamente:
   - Criação/ativação do ambiente virtual Python (`.venv`)
   - Instalação das dependências (`requirements.txt`)
   - Subida dos containers (Kafka em KRaft + Postgres)
   - Criação do tópico Kafka (default: `pedidos`)
   - Envio dos dados do CSV para o Kafka (`src/simple_kafka_producer.py`)
   - Consumo dos eventos e inserção no Postgres (`src/kafka_consumer.py`)
   - Consulta de validação no Postgres

3. Conferir os dados no Postgres:
   ```powershell
   docker exec -it pipeline_ecommerce-postgres-1 psql -U app -d ecommerce -c "SELECT * FROM pedidos LIMIT 5;"
   ```

---

## Variáveis de Ambiente
O script define automaticamente:
```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POSTGRES_CONN=postgresql://app:app@localhost:15432/ecommerce
```

---

## Observações
- O Kafka está rodando em **KRaft mode (sem ZooKeeper)**.
- Caso queira usar outro CSV ou tópico, rode com parâmetros:
  ```powershell
  .\start_pipeline.ps1 -CsvPath .\data\meu_arquivo.csv -Topic meu_topico
  ```
- O repositório está preparado para evolução futura: dashboards (Power BI / Superset), Airflow para orquestração e Spark para streaming.

---


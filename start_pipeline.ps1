# start_pipeline.ps1 (v3.2 - KRaft, sem ZooKeeper)
# Executar a partir da RAIZ do projeto

[CmdletBinding()]
param(
  [string]$CsvPath = ".\data\pedidos_teste.csv",
  [string]$Topic   = "pedidos"
)

$ErrorActionPreference = "Stop"

function Write-Section($text) { Write-Host "`n=== $text ===" -ForegroundColor Cyan }
function Write-OK($text)      { Write-Host "[OK] $text" -ForegroundColor Green }
function Write-Warn($text)    { Write-Host "[!] $text" -ForegroundColor Yellow }
function Write-Err($text)     { Write-Host "[X] $text" -ForegroundColor Red }

# 0) Ir para a pasta do script (caso rode de outro lugar)
try { Set-Location -Path (Split-Path -Parent $MyInvocation.MyCommand.Path) } catch {}

# 1) Ativar (ou criar) venv e instalar dependências (tolerante a falhas)
Write-Section "Ambiente Python (venv)"
if (!(Test-Path ".\.venv\Scripts\Activate.ps1")) {
  Write-Warn "Venv não encontrado. Criando .venv..."
  try {
    python -m venv .venv
    Write-OK "Venv criado."
  } catch {
    Write-Err ("Falha ao criar venv: " + $_.Exception.Message)
    exit 1
  }
}
. ".\.venv\Scripts\Activate.ps1"
Write-OK "Venv ativado."

if (Test-Path ".\requirements.txt") {
  Write-Section "Instalando/atualizando dependências (tolerante a falhas)"
  try {
    python -m pip install --upgrade pip | Out-Null
    pip install -r requirements.txt
    Write-OK "Dependências instaladas."
  } catch {
    Write-Warn ("Falha ao instalar alguma dependência. Vou seguir mesmo assim. Detalhe: " + $_.Exception.Message)
  }
} else {
  Write-Warn "requirements.txt não encontrado. Pulando instalação de libs."
}

# 2) Subir Docker
Write-Section "Subindo containers Docker"
try {
  docker compose up -d | Out-Null
  Write-OK "docker compose up -d executado."
} catch {
  Write-Err ("Falha ao subir Docker: " + $_.Exception.Message)
  exit 1
}

# 3) Ver status inicial
Write-Section "Status dos serviços"
docker compose ps

# 3.1) Resolver nomes reais dos containers (compose pode prefixar com a pasta do projeto)
function Resolve-ContainerName([string[]]$candidates) {
  $names = docker ps --format "{{.Names}}"
  foreach ($c in $candidates) {
    if ($names -match ("^" + [regex]::Escape($c) + "$")) { return $c }
  }
  return $null
}

# Candidatos comuns:
$kfName = Resolve-ContainerName @("kafka","pipeline_ecommerce-kafka-1")
$pgName = Resolve-ContainerName @("postgres","pipeline_ecommerce-postgres-1")

if (-not $kfName) { Write-Warn "Container do Kafka não encontrado no 'docker ps'. Vou tentar 'kafka' por padrão."; $kfName = "kafka" }
if (-not $pgName) { Write-Warn "Container do Postgres não encontrado no 'docker ps'. Vou tentar 'pipeline_ecommerce-postgres-1' por padrão."; $pgName = "pipeline_ecommerce-postgres-1" }

function Get-ContainerState($name) {
  try {
    $st = docker inspect --format='{{json .State}}' $name 2>$null | ConvertFrom-Json
    return $st
  } catch { return $null }
}

function Wait-HealthyOrRunning($container, $timeoutSec=90) {
  $start = Get-Date
  while ((Get-Date) - $start -lt (New-TimeSpan -Seconds $timeoutSec)) {
    $st = Get-ContainerState $container
    if ($st) {
      if ($st.Health) {
        if ($st.Health.Status -eq "healthy") { return $true }
      } elseif ($st.Status -eq "running") {
        return $true
      }
    }
    Start-Sleep -Seconds 3
  }
  return $false
}

# 4) Garantir Kafka 'Up'
Write-Section "Garantindo Kafka 'Up'"
try {
  docker compose up -d kafka | Out-Null
  Start-Sleep -Seconds 5
} catch {
  Write-Warn ("Não foi possível subir Kafka via compose: " + $_.Exception.Message)
}

$kafkaReady = Wait-HealthyOrRunning $kfName 90
if ($kafkaReady) { Write-OK "Kafka está 'Up'." } else { Write-Err "Kafka NÃO ficou 'Up' no tempo esperado." }

# 5) Verificar/aguardar Kafka respondendo (listar tópicos)
if ($kafkaReady) {
  Write-Section "Verificando comunicação com Kafka (listar tópicos)"
  $okList = $false
  $t0 = Get-Date
  while ((Get-Date) - $t0 -lt (New-TimeSpan -Seconds 60)) {
    try {
      docker exec -it $kfName bash -lc "kafka-topics.sh --bootstrap-server localhost:9092 --list" | Out-Null
      $okList = $true
      break
    } catch {
      Start-Sleep -Seconds 3
    }
  }
  if ($okList) { Write-OK "Kafka respondeu ao listar tópicos." } else { Write-Warn "Kafka não respondeu ao listar tópicos (timeout), mas vou tentar prosseguir." }
}

# 6) Criar tópico (se Kafka estiver 'Up')
if ($kafkaReady) {
  Write-Section ("Verificando/criando tópico Kafka: " + $Topic)
  try {
    docker exec -it $kfName bash -lc "kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -x '$Topic' || kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $Topic --partitions 1 --replication-factor 1"
    Write-OK ("Tópico '" + $Topic + "' pronto.")
  } catch {
    Write-Warn ("Falha ao criar/listar tópico: " + $_.Exception.Message)
  }
} else {
  Write-Warn "Kafka 'Down' — criação de tópico pulada."
}

# 7) Variáveis de ambiente úteis
Write-Section "Configurando variáveis de conexão"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:POSTGRES_CONN = "postgresql://app:app@localhost:15432/ecommerce"
Write-Host ("KAFKA_BOOTSTRAP_SERVERS=" + $env:KAFKA_BOOTSTRAP_SERVERS)
Write-Host ("POSTGRES_CONN=" + $env:POSTGRES_CONN)

# 8) Publicar CSV no Kafka (apenas se Kafka 'Up')
if ($kafkaReady) {
  Write-Section "Publicando CSV no Kafka"
  if (Test-Path $CsvPath) {
    try {
      python -m src.simple_kafka_producer --csv $CsvPath --topic $Topic
      Write-OK "Producer executado."
    } catch {
      Write-Warn ("Falha ao enviar CSV para Kafka: " + $_.Exception.Message)
    }
  } else {
    Write-Warn ("CSV não encontrado em '" + $CsvPath + "'. Pulando envio.")
  }
} else {
  Write-Warn "Kafka 'Down' — producer pulado."
}

# 9) Rodar consumer (apenas se Kafka 'Up')
if ($kafkaReady) {
  Write-Section "Rodando consumer (upsert no Postgres)"
  $group = "ecommerce-consumers-" + (Get-Date -Format "yyyyMMddHHmmss")
  try {
    python -m src.kafka_consumer --topic $Topic --group $group
    Write-OK "Consumer executado."
  } catch {
    Write-Warn ("Falha ao rodar consumer: " + $_.Exception.Message)
  }
} else {
  Write-Warn "Kafka 'Down' — consumer pulado."
}

# 10) Consultas no Postgres (sempre tentamos)
Write-Section "Validando dados no Postgres"
try {
  docker exec -it $pgName psql -U app -d ecommerce -c 'SELECT COUNT(*) FROM pedidos;'
  docker exec -it $pgName psql -U app -d ecommerce -c 'SELECT * FROM pedidos ORDER BY data_pedido DESC NULLS LAST LIMIT 5;'
} catch {
  Write-Warn ("Falha ao consultar Postgres: " + $_.Exception.Message)
}

Write-Section "Fluxo concluído"
Write-OK "Finalizado. Veja mensagens acima para qualquer alerta."

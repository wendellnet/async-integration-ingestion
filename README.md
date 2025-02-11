# async-integration-ingestion
Pipeline assíncrono para ingestão e integração de dados em larga escala utilizando Spark, Delta Lake e APIs externas. Otimizado para processamento em lotes e envio eficiente.

## Visão Geral da Solução
Este repositório contém um pipeline para exportação de dados utilizando Apache Spark e envio assíncrono de arquivos CSV para uma API.

A solução se divide em tres partes principais:
1. **Criação da Massa de Dados**:  Utiliza uma tabela Delta para armazenar os registros a serem integrados.
2. **Processamento e Exportação**: Spark lê os registros, divide em lotes e gera arquivos CSV.
3. **Envio para API **: Os arquivos CSV são enviados para uma API FastAPI para armazenamento e processamento.

## 1. Criação da Massa de Dados
A geração inicial dos dados é feita via PySpark, utilizando uma tabela Delta como repositório.

### Estrutura do Código
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, rand, lit, when
import uuid

spark = SparkSession.builder.appName("PopulateTestData").getOrCreate()
NUM_RECORDS = 1_000_000
CATEGORIES = ["A", "B", "C", "D", "E"]
df = spark.range(0, NUM_RECORDS).select(
    (monotonically_increasing_id()).alias("id"),
    (rand() * 100).alias("valor"),  
    lit(str(uuid.uuid4().hex[:10])).alias("referencia")  
)
# Criar a coluna category com 20% valores aleatórios
df = df.withColumn(
    "category",
    when(rand() < 0.2, lit("A"))  
    .when(rand() < 0.4, lit("B")) 
    .when(rand() < 0.6, lit("C")) 
    .when(rand() < 0.8, lit("D")) 
    .otherwise(lit("E"))  
)
df.write.mode("overwrite").format("delta").saveAsTable("database.wendell.registros_gold")

print("🔹 1 milhão de registros inseridos na tabela registros_gold com categorias aleatórias!")
```

## 2. Processamento e Envio Assíncrono

### Tecnologias Utilizadas
- Apache Spark (PySpark)
- Pandas
- Aiohttp (para requisições assíncronas)
- Delta Lake (armazenamento de registros processados)

### Fluxo do Processo
1. **Leitura dos Dados**: A solução busca registros na tabela `database.wendell.registros_gold`.
2. **Filtragem**: Apenas registros que ainda não foram enviados (baseado na tabela `database.wendell.registros_enviados`) são selecionados.
3. **Divisão em Lotes**: Os registros são convertidos para um DataFrame Pandas e divididos em lotes de 10.000 registros.
4. **Geração de CSV**: Cada lote é salvo em um arquivo CSV no diretório `bulk/`.
5. **Envio para a API**: Os arquivos CSV são enviados para a API de recebimento de forma assíncrona.
6. **Atualização de Controle**: IDs dos registros enviados são armazenados na tabela `database.wendell.registros_enviados`.

### Estrutura do Código
```python
import os
import asyncio
import aiohttp
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# Criar sessão Spark
spark = SparkSession.builder.appName("ExportApiIntegrationAsync").getOrCreate()

# Configurações
BATCH_SIZE = 10000
API_URL = "https://6bfc-177-94-86-136.ngrok-free.app/bulk_upload" #link criado pelo ngrok 
OUTPUT_DIR = "/Volumes/wendell/archives/bulk/" #endereço do volume

# Criar diretório se não existir
os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_non_sent_records():
    """Lê registros não enviados e divide em batches eficientes."""
    registros_gold = spark.read.table("database.wendell.registros_gold")
    enviados = spark.read.table("database.wendell.registros_enviados").select("id")

    df = registros_gold.join(enviados, on="id", how="left_anti")  # Filtra registros não enviados

    # Usa um campo categórico para distribuir melhor os registros
    window_spec = Window.partitionBy("category").orderBy("id")
    df = df.withColumn("batch_number", (row_number().over(window_spec) / BATCH_SIZE).cast("int"))

    return df

async def send_to_api(session, csv_path):
    """Envia CSV para a API de Integração de forma assíncrona.""" 
    async with session.post(API_URL, data={"file": open(csv_path, "rb")}) as response:
        if response.status == 200:
            return csv_path  # Retorna arquivo enviado com sucesso
        else:
            print(f"Erro ao enviar {csv_path}: {await response.text()}")
            return None

def process_batch(batch_df, batch_number):
    """Processa um batch e salva como CSV."""
    csv_path = f"{OUTPUT_DIR}batch_{batch_number}.csv"
    batch_df.toPandas().to_csv(csv_path, index=False)
    return csv_path

async def process_batches():
    """Processa os registros em lotes e envia para a integração de forma assíncrona."""
    df = fetch_non_sent_records()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for batch_number in df.select("batch_number").distinct().collect():
            batch_df = df.filter(col("batch_number") == batch_number.batch_number).drop("batch_number")
            csv_path = process_batch(batch_df, batch_number.batch_number)
            tasks.append(send_to_api(session, csv_path))  # Envia async

        # Aguarda todas as requisições terminarem
        results = await asyncio.gather(*tasks)

    # Atualiza tabela de controle com IDs enviados
    if results:
        registros_enviados = []
        for result in results:
            if result:
                batch_df = spark.read.csv(result, header=True)
                registros_enviados.extend(batch_df.select("id").toPandas()["id"].tolist())

        if registros_enviados:
            enviados_df = spark.createDataFrame(pd.DataFrame({"id": registros_enviados}))
            enviados_df.write.mode("append").format("delta").saveAsTable("database.wendell.registros_enviados")

# Executar pipeline assíncrono
await process_batches()

```

---
## 3. API para Recebimento de Arquivos
A API recebe os arquivos CSV enviados pelo Spark e armazena em um diretório. Neste ponto poderia ser um endpoint do salesforce por exemplo.

### Tecnologias Utilizadas
- FastAPI
- Uvicorn

### Fluxo da API
1. **Recebimento do Arquivo**: Um endpoint POST recebe os arquivos enviados pelo Spark.
2. **Armazenamento**: Os arquivos são salvos no diretório `received_csv/`.

### Estrutura do Código api.py
```python
from fastapi import FastAPI, File, UploadFile
import shutil
import os

app = FastAPI()
OUTPUT_DIR = "received_csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.post("/bulk_upload")
async def bulk_upload(file: UploadFile = File(...)):
    file_path = os.path.join(OUTPUT_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {"status": "sucesso", "filename": file.filename}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

---
## Como Rodar a API Localmente

### Passo 1: Instalar Dependências
```sh
pip install fastapi uvicorn
```

### Passo 2: Iniciar a API
```sh
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

### Passo 3: Configurar o ngrok (Opcional)
Caso precise expor a API para um endpoint público, instale e rode o **ngrok**:
```sh
ngrok http 8000
```
Isso gerará um link semelhante a:
```
Forwarding https://abcdef1234.ngrok.io -> http://127.0.0.1:8000
```

Altere a variável `API_URL` no script do Spark para refletir o novo endpoint gerado pelo ngrok.

---
## Conclusão

Essa solução combina **Spark para processamento de grandes volumes de dados**, **FastAPI para recebimento de arquivos**, e **Aiohttp para comunicação assíncrona**, permitindo um fluxo eficiente de exportação e ingestão de dados.

### Melhorias Possíveis:
- Implementar logs estruturados para monitoramento.
- Adicionar autenticação para o endpoint da API.
- Utilizar um armazenamento distribuído (S3, GCS) para os arquivos CSV ao invés do sistema de arquivos local.

---
**Autor:** Wendell Lopes

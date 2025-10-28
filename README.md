# 🏥 ETL — SIH/SUS (DATASUS)

## 📌 Objetivo

O objetivo deste projeto é construir um **dataset limpo e padronizado** a partir dos arquivos do **Sistema de Informações Hospitalares (SIH/SUS)** disponibilizados pelo **DATASUS**.  
O resultado final é uma base de dados que contém as seguintes informações essenciais:

- **Município de residência** do paciente  
- **Município de internação** (ou de atendimento)  
- **Procedimento realizado**  
- **Procedimento solicitado**  
- **Indicação de óbito**

Esses dados são extraídos e transformados a partir dos arquivos brutos do SIH/SUS, que originalmente apresentam essas informações em formato **codificado** e com **diversas outras colunas irrelevantes para a análise**.

---

## ⚙️ Estrutura do ETL

### 1. **Fonte dos dados (raw data)**

Os arquivos brutos são obtidos diretamente do **DATASUS**, nas seguintes formas:

| Tipo de dado | Fonte | Formato | Descrição |
|---------------|--------|----------|------------|
| Internações (SIH/SUS) | `SIH_RD` | `.parquet` / `.dbc` | Contém os registros hospitalares codificados |
| Municípios | `CNV/br_municip.cnv` | `.cnv` | Contém os códigos e nomes de municípios |
| Procedimentos | `DBF/TB_SIGTAW.dbf` | `.dbf` | Contém os códigos e descrições de procedimentos médicos |

---

### 2. **Processamento do dataset principal (SIH/SUS)**

O dataset bruto do SIH/SUS contém dezenas de colunas, mas apenas algumas são relevantes para este projeto.  
O **processamento inicial** consiste em:

1. **Remover colunas não utilizadas**.  
2. **Renomear as colunas principais** para nomes mais descritivos:  

| Coluna original | Novo nome |
|-----------------|------------|
| `MUNIC_RES` | `Municipio_Residencia` |
| `MUNIC_MOV` | `Municipio_Atendimento` |
| `PROC_REA` | `Procedimento_Realizado` |
| `PROC_SOLIC` | `Procedimento_Solicitado` |
| `MORTE` | `Obito` |

3. **Converter o campo de óbito** (`MORTE`) de valores `0` e `1` para **booleano** (`False` / `True`).

---

### 3. **Decodificação dos códigos**

Após o processamento inicial, os campos de município e procedimento ainda estão codificados.  
Por isso, é realizada uma etapa de **decodificação** utilizando duas tabelas auxiliares do DATASUS.

#### a) **Municípios**

- **Fonte raw:** `CNV/br_municip.cnv`  
- **Processamento:**  
  - Extração e limpeza dos códigos e nomes de municípios.  
  - Geração de um CSV processado contendo:
  
    | codigo_municipio | nome_municipio | faixa_codigos |
    |------------------|----------------|----------------|
  
- **Arquivo resultante:** `municipios.csv`

#### b) **Procedimentos**

- **Fonte raw:** `DBF/TB_SIGTAW.dbf`  
- **Colunas originais:** `IP_COD`, `IP_DSCR`  
- **Processamento:**  
  - Renomear colunas para `codigo_procedimento` e `nome_procedimento`.  
  - Converter o formato `.dbf` para `.csv` para uso simplificado.

- **Arquivo resultante:** `procedimentos.csv`

---

### 4. **Integração final**

A etapa final do ETL consiste em **juntar** o dataset principal com as tabelas de referência (`municipios.csv` e `procedimentos.csv`) para substituir os códigos pelos **nomes legíveis** correspondentes.

O resultado é um dataset padronizado e pronto para análise, contendo os nomes de municípios e descrições completas de procedimentos.

---

## 📂 Estrutura de diretórios

```
data/
├── raw/
│   ├── RDCE2001.parquet/          # Dados brutos do SIH/SUS
│   ├── codigo_municipio.cnv       # Dicionário de municípios
│   └── codigo_procedimentos.dbf   # Dicionário de procedimentos
│
├── processed/
│   ├── municipios.csv             # Processamento dos códigos municipais
│   ├── procedimentos.csv          # Processamento dos códigos procedimentos
│   └── sih.csv                    # Processaento inicial do SIH/SUS
│
└── final/
    └── final.csv                  # Final do ETL
```

---

## 🧰 Tecnologias utilizadas

- **Python**  
- **pandas** — manipulação e transformação de dados  
- **pysus** — download de arquivos do DATASUS  
- **dbfread** — leitura de arquivos `.dbf`  

---

## 🚀 Resultado final

O produto final do pipeline é um **CSV consolidado**, com os dados do SIH/SUS prontos para análise epidemiológica e estatística, contendo:

| Municipio_Residencia | Municipio_Atendimento | Procedimento_Realizado | Procedimento_Solicitado | Obito |
|-----------------------|------------------------|-------------------------|--------------------------|--------|

---

## 🧩 Desafios do projeto

Durante o desenvolvimento do ETL, alguns desafios técnicos se destacaram:

1. **Extração dos dados**  
   O principal desafio foi **obter os dados de forma automatizada**, já que o **DATASUS não fornece uma API pensada para pipelines de ETL**.  
   Os dados são disponibilizados em formatos e estruturas voltadas ao **uso manual por pessoas (via TabWin)**, o que exigiu um esforço adicional para padronizar o processo de download e leitura.

2. **Parse do arquivo `.cnv` (municípios)**  
   O arquivo `br_municip.cnv` é um formato proprietário utilizado pelo **TabWin**, software oficial do DATASUS.  
   Esse formato não é diretamente compatível com bibliotecas padrão de leitura de dados, o que exigiu a implementação de um **parser personalizado** para extrair corretamente os códigos, nomes e faixas de municípios.

3. **Integração dos diferentes formatos**  
   O projeto precisou lidar com múltiplos formatos de arquivo (`.dbc`, `.dbf`, `.cnv`, `.csv`, `.parquet`), cada um com particularidades de codificação e leitura.  
   A padronização de todos eles para CSV foi essencial para garantir **reprodutibilidade** e **facilidade de análise**.

---

## 📌 Dependências

- **Python** — versão 3.11
- **Linux**

## ▶️ Execução

1. Clone o repositório:

```bash
git clone https://github.com/joaobgoode/SIH-SUS-ETL.git
```

2. Entre na pasta do projeto:

```bash
cd SIH-SUS-ETL
```

3. Inicie o ambiente virtual:

```bash
python -m venv .venv && source .venv/bin/activate
```

4. Instale as dependências:

```
pip install .
```

5. Rode o ETL:

```
python main.py
```

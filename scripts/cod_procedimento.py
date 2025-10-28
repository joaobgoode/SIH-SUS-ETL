import ftplib
import zipfile
import io
import pandas as pd
from dbfread import DBF
from pathlib import Path
import os
import shutil


FTP_HOST = "ftp.datasus.gov.br"
FTP_PATH = "/dissemin/publicos/SIHSUS/200801_/Auxiliar/TAB_SIH.zip"
DBF_PATH = "DBF/TB_SIGTAW.dbf"

TEMP_DBF_DIR = Path("./data/temp_dbf")
TEMP_DBF_FILE = TEMP_DBF_DIR / "TB_SIGTAW.dbf"

RAW_DIR = Path("./data/raw")
PROCESSED_DIR = Path("./data/processed")
RAW_PARQUET_PATH = RAW_DIR / "tb_sigtaw_raw.parquet"
PROCESSED_CSV_PATH = PROCESSED_DIR / "codigo_procedimento.csv"


def baixar_arquivo_ftp(ftp_host: str = FTP_HOST, ftp_path: str = FTP_PATH) -> bytes:
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    content = io.BytesIO()
    try:
        ftp.retrbinary(f"RETR {ftp_path}", content.write)
    finally:
        ftp.quit()
    return content.getvalue()


def extrair_dbf_bytes(zip_bytes: bytes, dbf_path: str = DBF_PATH) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as inner_zip:
        with inner_zip.open(dbf_path) as dbf_file:
            return dbf_file.read()


def ler_dbf_para_dataframe(dbf_file_path: Path) -> pd.DataFrame:
    dbf = DBF(str(dbf_file_path), encoding="latin-1")
    df = pd.DataFrame(iter(dbf))
    return df


def salvar_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def salvar_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def extrair_procedimentos(
    ftp_host: str = FTP_HOST,
    ftp_path: str = FTP_PATH,
    dbf_path: str = DBF_PATH,
    temp_dir: Path = TEMP_DBF_DIR,
    temp_dbf_path: Path = TEMP_DBF_FILE,
    raw_parquet_path: Path = RAW_PARQUET_PATH,
) -> Path:
    temp_dir.mkdir(parents=True, exist_ok=True)

    zip_bytes = baixar_arquivo_ftp(ftp_host, ftp_path)
    dbf_bytes = extrair_dbf_bytes(zip_bytes, dbf_path)

    with open(temp_dbf_path, "wb") as f:
        f.write(dbf_bytes)

    df_raw = ler_dbf_para_dataframe(temp_dbf_path)

    shutil.rmtree(temp_dir, ignore_errors=True)

    salvar_parquet(df_raw, raw_parquet_path)

    return raw_parquet_path


def processar_procedimentos(
    raw_parquet_path: Path = RAW_PARQUET_PATH,
    processed_csv_path: Path = PROCESSED_CSV_PATH,
) -> pd.DataFrame:
    if not raw_parquet_path.exists():
        extrair_procedimentos(raw_parquet_path=raw_parquet_path)

    df = pd.read_parquet(raw_parquet_path)

    df = df.rename(
        columns={
            "IP_COD": "codigo_procedimento",
            "IP_DSCR": "nome_procedimento",
        }
    )

    salvar_csv(df, processed_csv_path)

    return df


if __name__ == "__main__":
    df_procedimentos = processar_procedimentos()

import re
import ftplib
import zipfile
import io
import pandas as pd
from pathlib import Path

FTP_HOST = "ftp.datasus.gov.br"
FTP_PATH = "/dissemin/publicos/SIHSUS/200801_/Auxiliar/TAB_SIH.zip"
CNV_PATH = "CNV/br_municip.cnv"
RAW_DIR = Path("./data/raw")
RAW_OUTPUT_PATH = RAW_DIR / "br_municip.cnv"
PROCESSED_DIR = Path("./data/processed")
OUTPUT_PATH = PROCESSED_DIR / "codigo_municipio.csv"


def baixar_arquivo_ftp(ftp_host: str = FTP_HOST, ftp_path: str = FTP_PATH) -> bytes:
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    content = io.BytesIO()
    try:
        ftp.retrbinary(f"RETR {ftp_path}", content.write)
    finally:
        ftp.quit()
    return content.getvalue()


def extrair_cnv_de_zip(zip_bytes: bytes, cnv_path: str = CNV_PATH) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as inner_zip:
        with inner_zip.open(cnv_path) as file:
            return file.read()


def salvar_arquivo(content_bytes: bytes, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content_bytes)


def salvar_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def extrair_municipios(
    ftp_host: str = FTP_HOST,
    ftp_path: str = FTP_PATH,
    cnv_path: str = CNV_PATH,
    raw_dir: Path = RAW_DIR,
) -> Path:
    zip_bytes = baixar_arquivo_ftp(ftp_host, ftp_path)
    cnv_bytes = extrair_cnv_de_zip(zip_bytes, cnv_path)

    cnv_output_path = raw_dir / cnv_path.split("/")[-1]
    salvar_arquivo(cnv_bytes, cnv_output_path)

    return cnv_output_path


def parse_cnv(content: str) -> pd.DataFrame:
    pattern = re.compile(r"\s*\d+\s+(\d+)\s+([A-ZÁÉÍÓÚÃÕÇ\s]+?)\s+([\d, \-]+)")
    data = []

    for match in pattern.finditer(content):
        codigo = match.group(1).strip()
        nome = match.group(2).strip().title()
        codigos_extra = match.group(3).strip()
        data.append(
            {
                "codigo_municipio": codigo,
                "nome_municipio": nome,
                "faixa_codigos": codigos_extra,
            }
        )

    df = pd.DataFrame(data).dropna()
    return df


def processar_municipios(
    raw_path: Path = RAW_OUTPUT_PATH, output_path: Path = OUTPUT_PATH
) -> pd.DataFrame:
    with open(raw_path, "rb") as f:
        content = f.read().decode("latin-1")

    df = parse_cnv(content)
    salvar_csv(df, output_path)

    return df


if __name__ == "__main__":
    cnv_raw_path = RAW_DIR / CNV_PATH.split("/")[-1]

    path_cnv_bruto = extrair_municipios(raw_dir=RAW_DIR, cnv_path=CNV_PATH)

    df_municipios = processar_municipios(
        raw_path=path_cnv_bruto, output_path=OUTPUT_PATH
    )
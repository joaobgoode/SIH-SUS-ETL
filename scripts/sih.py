from pysus import SIH
import pandas as pd
from pathlib import Path

UF_DEFAULT = "CE"
ANO_DEFAULT = 2020
RAW_DIR_DEFAULT = Path("data/raw")
PROCESSED_PATH_DEFAULT = Path("data/processed/sih.csv")


def extrair_sih(
    uf: str = UF_DEFAULT, ano: int = ANO_DEFAULT, local_raw: Path = RAW_DIR_DEFAULT
) -> pd.DataFrame:
    sih = SIH().load()
    files = sih.get_files("RD", uf=uf, year=ano)

    sih.download(files[0], local_dir=str(local_raw))

    parquet = sih.download(files[0])
    return parquet.to_dataframe()


def processar_sih(df: pd.DataFrame) -> pd.DataFrame:
    df_processado = df[
        ["MUNIC_RES", "MUNIC_MOV", "PROC_REA", "PROC_SOLIC", "MORTE"]
    ].copy()

    df_processado = df_processado.assign(
        Obito=(df_processado["MORTE"].astype(int) == 1)
    )

    df_processado = df_processado.rename(
        columns={
            "MUNIC_RES": "Municipio_Residencia",
            "MUNIC_MOV": "Municipio_Atendimento",
            "PROC_REA": "Procedimento_Realizado",
            "PROC_SOLIC": "Procedimento_Solicitado",
        }
    )
    df_processado.drop(columns=["MORTE"], inplace=True)
    return df_processado


def salvar_sih(df: pd.DataFrame, caminho: Path = PROCESSED_PATH_DEFAULT):
    """Salva o DataFrame processado no caminho especificado."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(caminho, index=False)


if __name__ == "__main__":
    df_raw = extrair_sih()
    df_proc = processar_sih(df_raw)
    salvar_sih(df_proc)

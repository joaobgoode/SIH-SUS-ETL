import pandas as pd


def expandir_faixas(faixa: str) -> list[int]:
    if not faixa or str(faixa).strip() == "":
        return []

    codigos = []
    partes = [p.strip() for p in faixa.split(",") if p.strip()]

    for parte in partes:
        if "-" in parte:
            inicio, fim = parte.split("-")
            if inicio.strip().isdigit() and fim.strip().isdigit():
                codigos.extend(range(int(inicio), int(fim) + 1))
        elif parte.strip().isdigit():
            codigos.append(int(parte))
    return codigos


def carregar_dados(
    sih_path="./data/processed/sih.csv",
    municipios_path="./data/processed/codigo_municipio.csv",
    procedimentos_path="./data/processed/codigo_procedimento.csv",
):
    df = pd.read_csv(sih_path)
    mun_ref = pd.read_csv(municipios_path)
    proc_ref = pd.read_csv(procedimentos_path)
    return df, mun_ref, proc_ref


def expandir_municipios(mun_ref: pd.DataFrame) -> pd.DataFrame:
    linhas_expandidas = []
    for _, row in mun_ref.iterrows():
        for codigo in expandir_faixas(row["faixa_codigos"]):
            linhas_expandidas.append(
                {"codigo_municipio": codigo, "nome_municipio": row["nome_municipio"]}
            )
    return pd.DataFrame(linhas_expandidas)


def mapear_codigos(df: pd.DataFrame, mun_ref: pd.DataFrame, proc_ref: pd.DataFrame):
    mun_ref_expandido = expandir_municipios(mun_ref)
    mapa_mun = dict(
        zip(mun_ref_expandido["codigo_municipio"], mun_ref_expandido["nome_municipio"])
    )
    mapa_proc = dict(
        zip(proc_ref["codigo_procedimento"], proc_ref["nome_procedimento"])
    )

    df["Municipio_Residencia"] = df["Municipio_Residencia"].map(mapa_mun)
    df["Municipio_Atendimento"] = df["Municipio_Atendimento"].map(mapa_mun)
    df["Procedimento_Realizado"] = df["Procedimento_Realizado"].map(mapa_proc)
    df["Procedimento_Solicitado"] = df["Procedimento_Solicitado"].map(mapa_proc)
    return df


def salvar_final(df: pd.DataFrame, caminho="./data/final/final.csv"):
    df.to_csv(caminho, index=False)


if __name__ == "__main__":
    df, mun_ref, proc_ref = carregar_dados()
    df_final = mapear_codigos(df, mun_ref, proc_ref)
    salvar_final(df_final)

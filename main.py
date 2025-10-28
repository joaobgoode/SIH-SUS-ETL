import scripts.cod_municipio as cm
import scripts.cod_procedimento as cp
import scripts.sih as sih
import scripts.process as process


def etl_municipios():
    cm.extrair_municipios()
    cm.processar_municipios()


def etl_procedimentos():
    cp.processar_procedimentos()


def etl_sih():
    df_raw = sih.extrair_sih()
    df_proc = sih.processar_sih(df_raw)
    sih.salvar_sih(df_proc)


def etl_process():
    df, mun_ref, proc_ref = process.carregar_dados()
    df_final = process.mapear_codigos(df, mun_ref, proc_ref)
    process.salvar_final(df_final)


def run_etl():
    etl_municipios()
    etl_procedimentos()
    etl_sih()
    etl_process()


if __name__ == "__main__":
    run_etl()

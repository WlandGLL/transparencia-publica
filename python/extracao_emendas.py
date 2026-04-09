import requests
import pandas as pd
import time
import getpass

# anos desde a posse da última legislatura
ANOS = [2023, 2024, 2025, 2026]

# token da API do Portal da Transparência
# acesse: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
token = getpass.getpass("Cole seu token da API (invisível): ")


def extrair_emendas_ano(ano, token_api):
    """Extrai todas as emendas parlamentares de um ano específico"""

    headers = {
        "accept": "*/*",
        "chave-api-dados": token_api
    }

    url_base = "https://api.portaldatransparencia.gov.br/api-de-dados/emendas"
    dados_acumulados = []
    pagina = 1

    print(f"\nExtraindo emendas de {ano}...")

    while True:
        try:
            url = f"{url_base}?ano={ano}&pagina={pagina}"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                lista = response.json()

                if not lista:
                    print(f"Ano {ano} finalizado — {len(dados_acumulados)} registros")
                    break

                dados_acumulados.extend(lista)
                pagina += 1
                time.sleep(0.1)

            elif response.status_code == 429:
                print("Limite da API atingido, aguardando 10s...")
                time.sleep(10)

            else:
                print(f"Erro na página {pagina}: {response.status_code}")
                break

        except Exception as e:
            print(f"Erro inesperado: {e}")
            break

    return pd.DataFrame(dados_acumulados)


def extrair_emendas():
    dfs = []

    for ano in ANOS:
        df_ano = extrair_emendas_ano(ano, token)

        if not df_ano.empty:
            dfs.append(df_ano)

    if not dfs:
        print("Nenhum dado encontrado.")
        return None

    df_total = pd.concat(dfs, ignore_index=True)

    # renomeia colunas para o padrão do banco
    df_total = df_total.rename(columns={
        "codigoEmenda":        "codigo_emenda",
        "ano":                 "ano",
        "tipoEmenda":          "tipo_emenda",
        "autor":               "autor",
        "nomeAutor":           "nome_autor",
        "numeroEmenda":        "numero_emenda",
        "localidadeDoGasto":   "localidade_gasto",
        "funcao":              "funcao",
        "subfuncao":           "subfuncao",
        "valorEmpenhado":      "valor_empenhado",
        "valorLiquidado":      "valor_liquidado",
        "valorPago":           "valor_pago",
        "valorRestoInscrito":  "valor_resto_inscrito",
        "valorRestoCancelado": "valor_resto_cancelado",
        "valorRestoPago":      "valor_resto_pago"
    })

    # converte colunas de valor para numérico
    colunas_valor = [
        "valor_empenhado", "valor_liquidado", "valor_pago",
        "valor_resto_inscrito", "valor_resto_cancelado", "valor_resto_pago"
    ]

    for col in colunas_valor:
        if col in df_total.columns:
            df_total[col] = pd.to_numeric(
                df_total[col].astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False),
                errors="coerce"
            )

    # remove duplicatas pelo código da emenda
    df_total = df_total.drop_duplicates(subset=["codigo_emenda"], keep="first")

    print(f"\nTotal de emendas extraídas: {len(df_total)}")
    return df_total


# executa
df_emendas = extrair_emendas()

if df_emendas is not None:
    df_emendas.to_csv("emendas_parlamentares.csv", index=False, encoding="utf-8-sig")
    print("Arquivo salvo: emendas_parlamentares.csv")
    print(df_emendas.head())

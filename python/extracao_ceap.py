import requests
import pandas as pd
import time

# API base da câmara
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

# anos desde a posse da última legislatura
ANOS = [2023, 2024, 2025, 2026]


def buscar_ids_deputados():
    """Pega os IDs de todos os deputados para buscar as despesas de cada um"""
    print("Buscando IDs dos deputados...")
    ids = []
    pagina = 1

    while True:
        url = f"{BASE_URL}/deputados"
        params = {"itens": 100, "pagina": pagina}
        response = requests.get(url, params=params)

        if response.status_code != 200:
            break

        dados = response.json().get("dados", [])
        if not dados:
            break

        ids.extend([d["id"] for d in dados])
        pagina += 1
        time.sleep(0.15)

    print(f"Total de deputados encontrados: {len(ids)}")
    return ids


def buscar_despesas_deputado(id_deputado, ano):
    """Busca todas as despesas de um deputado em um determinado ano"""
    despesas = []
    pagina = 1

    while True:
        url = f"{BASE_URL}/deputados/{id_deputado}/despesas"
        params = {
            "ano": ano,
            "itens": 100,
            "pagina": pagina
        }

        response = requests.get(url, params=params)

        if response.status_code != 200:
            break

        dados = response.json().get("dados", [])
        if not dados:
            break

        # adiciona o id do deputado em cada registro
        for item in dados:
            item["idDeputado"] = id_deputado

        despesas.extend(dados)
        pagina += 1
        time.sleep(0.1)

    return despesas


def extrair_ceap():
    ids = buscar_ids_deputados()
    todos_registros = []

    for ano in ANOS:
        print(f"\nExtraindo despesas de {ano}...")
        contador = 0

        for id_dep in ids:
            despesas = buscar_despesas_deputado(id_dep, ano)
            todos_registros.extend(despesas)
            contador += 1

            # mostra progresso a cada 50 deputados
            if contador % 50 == 0:
                print(f"  {contador}/{len(ids)} deputados processados — {len(todos_registros)} registros acumulados")

        print(f"Ano {ano} finalizado.")

    df = pd.DataFrame(todos_registros)

    if df.empty:
        print("Nenhum dado encontrado.")
        return None

    # renomeia para o padrão do banco
    df = df.rename(columns={
        "idDeputado":        "id_deputado",
        "ano":               "ano",
        "mes":               "mes",
        "tipoDespesa":       "tipodespesa",
        "codDocumento":      "coddocumento",
        "tipoDocumento":     "tipodocumento",
        "codTipoDocumento":  "codtipodocumento",
        "dataDocumento":     "datadocumento",
        "numDocumento":      "numdocumento",
        "valorDocumento":    "valordocumento",
        "urlDocumento":      "urldocumento",
        "nomeFornecedor":    "nomefornecedor",
        "cnpjCpfFornecedor": "cnpjcpffornecedor",
        "valorLiquido":      "valorliquido",
        "valorGlosa":        "valorglosa",
        "numRessarcimento":  "numressarcimento",
        "codLote":           "codlote",
        "parcela":           "parcela"
    })

    df = df.drop_duplicates()

    print(f"\nTotal de despesas extraídas: {len(df)}")
    return df


# executa
df_ceap = extrair_ceap()

if df_ceap is not None:
    df_ceap.to_csv("ceap_despesas.csv", index=False, encoding="utf-8-sig")
    print("Arquivo salvo: ceap_despesas.csv")
    print(df_ceap.head())

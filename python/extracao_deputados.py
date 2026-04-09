import requests
import pandas as pd
import time

# API base da câmara
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

def buscar_deputados():
    """Busca todos os deputados da legislatura atual"""
    
    print("Buscando lista de deputados...")
    
    deputados = []
    pagina = 1

    while True:
        url = f"{BASE_URL}/deputados"
        params = {
            "itens": 100,
            "pagina": pagina,
            "ordem": "ASC",
            "ordenarPor": "nome"
        }

        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f"Erro na página {pagina}: {response.status_code}")
            break

        dados = response.json().get("dados", [])

        if not dados:
            print(f"Extração finalizada na página {pagina - 1}")
            break

        deputados.extend(dados)
        print(f"Página {pagina} OK — {len(deputados)} deputados até agora")
        pagina += 1
        time.sleep(0.2)

    return deputados


def buscar_detalhes_deputado(id_deputado):
    """Busca detalhes de um deputado pelo ID"""
    url = f"{BASE_URL}/deputados/{id_deputado}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json().get("dados", {})
    return {}


def extrair_deputados():
    lista = buscar_deputados()

    if not lista:
        print("Nenhum deputado encontrado.")
        return None

    # monta o dataframe com os campos principais
    registros = []
    for dep in lista:
        registros.append({
            "id":           dep.get("id"),
            "nome":         dep.get("nome"),
            "siglaPartido": dep.get("siglaPartido"),
            "siglaUf":      dep.get("siglaUf"),
            "email":        dep.get("email"),
            "urlFoto":      dep.get("urlFoto"),
            "uri":          dep.get("uri")
        })

    df = pd.DataFrame(registros)
    df = df.drop_duplicates(subset=["id"])

    print(f"\nTotal de deputados extraídos: {len(df)}")
    return df


# executa
df_deputados = extrair_deputados()

if df_deputados is not None:
    df_deputados.to_csv("deputados.csv", index=False, encoding="utf-8-sig")
    print("Arquivo salvo: deputados.csv")
    print(df_deputados.head())

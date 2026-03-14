import psycopg2
import pandas as pd
from IPython.display import display



CREDENCIAIS = dict(user="aluno_readonly", password="Alunos2026!", dbname="neondb", sslmode="require")

HOSTS = {
    "vendas":  "ep-raspy-block-ainzo20a-pooler.c-4.us-east-1.aws.neon.tech",
    "estoque": "ep-dry-frog-ail6dwj9-pooler.c-4.us-east-1.aws.neon.tech",
    "rh":      "ep-noisy-unit-aiyj66lx-pooler.c-4.us-east-1.aws.neon.tech",
}
def extrair_tabelas(host, tabelas):
    """Conecta no Neon e retorna um dict {nome_tabela: DataFrame}."""
    conn = psycopg2.connect(host=host, **CREDENCIAIS)
    resultado = {}
    for tabela in tabelas:
        resultado[tabela] = pd.read_sql(f"SELECT * FROM {tabela}", conn)
        print(f"   📥 {tabela}: {len(resultado[tabela])} linhas, {len(resultado[tabela].columns)} colunas")
    conn.close()
    return resultado

print("🔵 Extraindo VENDAS...")
dados_vendas = extrair_tabelas(HOSTS["vendas"], ["filiais", "formas_pagamento", "vendas", "itens_venda"])

vd_filiais = dados_vendas["filiais"]
vd_formas  = dados_vendas["formas_pagamento"]
vd_vendas  = dados_vendas["vendas"]
vd_itens   = dados_vendas["itens_venda"]

print("🟢 Extraindo ESTOQUE...")
dados_estoque = extrair_tabelas(HOSTS["estoque"], [
    "categorias", "produtos", "filiais_estoque", "fornecedores",
    "estoque", "entradas_mercadoria", "itens_entrada"
])

es_categorias = dados_estoque["categorias"]
es_produtos   = dados_estoque["produtos"]
es_filiais    = dados_estoque["filiais_estoque"]
es_fornecedores = dados_estoque["fornecedores"]
es_estoque    = dados_estoque["estoque"]
es_entradas   = dados_estoque["entradas_mercadoria"]
es_itens_ent  = dados_estoque["itens_entrada"]

print("🟠 Extraindo RH...")
dados_rh = extrair_tabelas(HOSTS["rh"], [
    "cargos", "departamentos", "filiais_rh", "funcionarios",
    "escalas", "historico_salario"
])

rh_cargos   = dados_rh["cargos"]
rh_deptos   = dados_rh["departamentos"]
rh_filiais  = dados_rh["filiais_rh"]
rh_funcs    = dados_rh["funcionarios"]
rh_escalas  = dados_rh["escalas"]
rh_hist_sal = dados_rh["historico_salario"]


# print("Visualizar problema das filiais")
# print("=== Comparativo de filiais entre sistemas (UNION) ===")

# vd_filiais["sistema"] = "vendas"
# es_filiais["sistema"] = "estoque"
# rh_filiais["sistema"] = "rh"

# display(pd.concat([vd_filiais, es_filiais, rh_filiais], ignore_index=True))

# print("----"*65)

# print("Visualizar problema dos números")
# print("=== Comparativo de valores entre sistemas (UNION) ===")
# display(pd.concat([vd_vendas, es_estoque], ignore_index=True))

print("----"*65)
print("Visualizar problema dos status")
print("=== Comparativo de valores entre sistemas (UNION) ===")

display(vd_vendas.head(3))
display(es_produtos.head(3))
display(rh_funcs[["matricula", "nome_completo", "nr_cpf", "data_nascimento", "data_admissao", "status"]].head(5))


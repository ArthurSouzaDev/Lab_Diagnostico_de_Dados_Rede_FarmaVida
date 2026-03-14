import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import os
import re
from datetime import datetime
from IPython.display import display

# Configurações de exibição
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 60)
pd.set_option("display.width", 200)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams["figure.figsize"] = (12, 5)
plt.rcParams["figure.dpi"] = 100

print("✅ Bibliotecas carregadas")

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

# Teste rápido de conectividade
for nome, host in HOSTS.items():
    try:
        c = psycopg2.connect(host=host, **CREDENCIAIS)
        c.close()
        print(f"✅ {nome}: conectado")
    except Exception as e:
        print(f"❌ {nome}: {e}")

print("🔵 Extraindo VENDAS...")
dados_vendas = extrair_tabelas(HOSTS["vendas"], ["filiais", "formas_pagamento", "vendas", "itens_venda"])

vd_filiais = dados_vendas["filiais"]
vd_formas  = dados_vendas["formas_pagamento"]
vd_vendas  = dados_vendas["vendas"]
vd_itens   = dados_vendas["itens_venda"]
print(f"\n📋 Colunas vendas:      {vd_vendas.columns.tolist()}")
print(f"📋 Colunas itens_venda: {vd_itens.columns.tolist()}")
print(f"📋 Colunas formas_pgto: {vd_formas.columns.tolist()}")
display(vd_vendas.head(3))

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

print(f"\n📋 Colunas produtos:    {es_produtos.columns.tolist()}")
print(f"📋 Colunas estoque:     {es_estoque.columns.tolist()}")
print(f"📋 Colunas fornecedores:{es_fornecedores.columns.tolist()}")
print(f"📋 Colunas entradas:    {es_entradas.columns.tolist()}")
print(f"📋 Colunas itens_ent:   {es_itens_ent.columns.tolist()}")
display(es_produtos.head(3))

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

print(f"\n📋 Colunas funcionarios: {rh_funcs.columns.tolist()}")
print(f"📋 Colunas filiais_rh:   {rh_filiais.columns.tolist()}")
display(rh_funcs[["matricula", "nome_completo", "nr_cpf", "data_nascimento", "data_admissao", "status"]].head(5))

# ── Fechar conexão do DW ──────────────────────────────────────
# try:
#     conn_dw.close()
#     print("✅ Conexão DW (SQLite) fechada.")
# except Exception:
#     print("⚠️  Conexão DW já estava fechada.")

# print(f"\n📁 Arquivo gerado: {DW_PATH}")
# print(f"   Tamanho: {os.path.getsize(DW_PATH) / 1024:.1f} KB")
# print("\n🎉 Pipeline ETL concluído com sucesso!")
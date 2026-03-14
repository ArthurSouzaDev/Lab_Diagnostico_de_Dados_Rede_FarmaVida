# 🏥 Lab — Diagnóstico de Dados · Rede FarmaVida

Laboratório de análise e diagnóstico de inconsistências de dados em ambiente multi-sistema. O cenário simula uma rede farmacêutica com **7 filiais** e **3 sistemas independentes** originados de aquisições distintas, cada um com seu próprio banco PostgreSQL no NeonDB.

---

## 📌 Contexto

A Rede FarmaVida opera com 3 sistemas que não conversam entre si:

| Sistema | Descrição |
|---|---|
| **Vendas (PDV)** | Registro de vendas, clientes e formas de pagamento |
| **Estoque** | Controle de produtos, entradas e movimentações |
| **RH** | Funcionários, cargos, escalas e histórico salarial |

Por terem origens diferentes, os dados apresentam diversas inconsistências que inviabilizam análises cruzadas, views consolidadas e integrações futuras.

---

## 🎯 Objetivo

Conectar nos 3 bancos, extrair as tabelas, identificar inconsistências e propor correções — documentando cada problema com: sistema, tabela, coluna, problema e solução proposta.

---

## 🗂️ Estrutura do Repositório

```
├── notebook.ipynb       # Análise principal com todo o diagnóstico
├── README.md            # Este arquivo
```

---

## 🔌 Conexões

**Credenciais comuns:** banco `neondb` · porta `5432` · usuário `aluno_readonly`

| Sistema | Host |
|---|---|
| Vendas | `ep-raspy-block-ainzo20a-pooler.c-4.us-east-1.aws.neon.tech` |
| Estoque | `ep-dry-frog-ail6dwj9-pooler.c-4.us-east-1.aws.neon.tech` |
| RH | `ep-noisy-unit-aiyj66lx-pooler.c-4.us-east-1.aws.neon.tech` |

---

## 🔍 Inconsistências Encontradas

| Sistema | Tabela | Coluna | Problema | Correção proposta |
|---|---|---|---|---|
| Vendas | filiais | codigo_filial | Fora de padronização: identificador como inteiro impede JOIN direto com Estoque e RH | Criar tabela de-para central: id → sigla → nome descritivo |
| Estoque | filiais_estoque | sigla_filial | Fora de padronização: identificador como sigla sem correspondência direta com os demais sistemas | Mapear sigla → id e → nome via tabela de-para |
| RH | filiais_rh | nome_filial | Fora de padronização: identificador como nome descritivo impossibilita JOINs sem normalização | Incluir na tabela de-para central como terceira chave |
| Vendas | vendas | data_venda | Fora de padronização: TEXT com DD/MM/YYYY impede ordenação e filtros temporais | Migrar para DATE com `TO_DATE(data_venda, 'DD/MM/YYYY')` |
| Estoque | movimentacoes | data_movimentacao | Fora de padronização: TEXT com YYYY-MM-DD impede uso direto em views temporais | Migrar para DATE com `TO_DATE(data_movimentacao, 'YYYY-MM-DD')` |
| Vendas | vendas | valor_total_centavos | Fora de padronização: valor em centavos diverge 100× do padrão decimal do Estoque | Converter na integração: `valor_total_centavos / 100.0` |
| Vendas | vendas | status | Fora de padronização: status como char ('A'/'C') incompatível com os demais sistemas | Criar tabela de-para: 'A' → 'ativo', 'C' → 'cancelado' |
| Estoque | produtos | status | Padrão de referência: status como texto ('ativo'/'inativo') | Adotar como padrão da integração |
| RH | funcionarios | status | Fora de padronização: status como inteiro (1/0) incompatível com os demais sistemas | Criar tabela de-para: 1 → 'ativo', 0 → 'inativo' |
| Vendas | itens_venda | nome_produto | Fora de padronização: digitação livre com grafias variadas impede JOIN com cadastro oficial do Estoque | Usar `cod_produto` como chave de ligação em vez do nome |
| Estoque | produtos | descricao | Padrão de referência: nomes em MAIÚSCULAS com descrição completa | Adotar como padrão e usar `id_produto` como chave |
| Vendas | vendas | id_vendedor | Fora de padronização: nem todos os id_vendedor possuem matrícula correspondente no RH | Criar tabela de-para e investigar os sem match |
| Vendas | vendas | cpf_cliente | Fora de padronização: CPF com máscara e registros nulos | Remover máscara e padronizar para apenas números |
| RH | funcionarios | nr_cpf | Padrão de referência: CPF sem máscara | Adotar como padrão da integração |

---

## 🛠️ Tecnologias

- Python 3
- psycopg2
- pandas
- PostgreSQL · NeonDB

---

## ▶️ Como executar

1. Clone o repositório
```bash
git clone https://github.com/ArthurSouzaDev/Lab_Diagnostico_de_Dados_Rede_FarmaVida.git
```

2. Instale as dependências
```bash
pip install psycopg2-binary pandas
```

3. Abra e execute o `notebook.ipynb`

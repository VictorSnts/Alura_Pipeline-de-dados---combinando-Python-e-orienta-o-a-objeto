from processamento_dados import Dados


def print_info(info):
    print(f"INFO: {info}")
    print("--")


# # Leitura dos dados
path_json = "data_raw/dados_empresaA.json"
path_csv  = "data_raw/dados_empresaB.csv"

empresaA_data = Dados(path=path_json,data_format="json")
empresaB_data = Dados(path=path_csv, data_format="csv")

print_info(f"Caminho do arquivo da empresaA: {empresaA_data.path}")
print_info(f"Caminho do arquivo da empresab: {empresaB_data.path}")

print_info(f"Campos do arquivo da empresaA: {empresaA_data.columns}")
print_info(f"Numero de registros do arquivo da empresaA: {empresaA_data.size}")
print_info(f"Campos do arquivo da empresaB: {empresaB_data.size} {empresaB_data.columns}")
print_info(f"Numero de registros do arquivo da empresaB: {empresaB_data.size}")


# Transformação dos dados
key_mapping = {
    'Nome do Item' : 'Nome do Produto',
    'Classificação do Produto' : 'Categoria do Produto',
    'Valor em Reais (R$)' : 'Preço do Produto (R$)',
    'Quantidade em Estoque' : 'Quantidade em Estoque',
    'Nome da Loja' : 'Filial',
    'Data da Venda' : 'Data da Venda'
}

print_info("Renomeando campos dos dados da empresaB.")
empresaB_data.rename_columns(key_mapping)
print_info(f"Campos dos dados da empressaB renomeados. Novos campos: {empresaB_data.columns}")

print_info("Incluindo campo 'Data da Venda' nos dados da empresaA.")
empresaA_data.add_field("Data da Venda", "Indisponivel")
print_info(f"Campo 'Data da Venda' incluido nos dados do JSON. Novos Campos: {empresaA_data.columns}")


# Agrupando os dados
grouped_data = Dados.join_data([empresaA_data.data, empresaB_data.data])
print_info(f"Campos dos Dados Agrupados: {grouped_data.columns}")
print_info(f"Numero de registros dos Dados Agrupados: {grouped_data.size}")


# Salvando dados
path_grouped_data = 'data_prossesed/dados_agrupados.csv'
print_info(f"Salvando dados agrupados.")
grouped_data.write_data(path_grouped_data)
print_info(f"Dados salvos com sucesso em {path_grouped_data}")
import json
import csv

def print_info(info):
    print(f"INFO: {info}")
    print("--")

def read_json(path):
    with open(path, 'r') as file:
        json_data = json.load(file)
    return json_data

def read_csv(path):
    csv_data = []

    with open(path, 'r') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        for row in spamreader:
            csv_data.append(row)
    return csv_data

def read_data(path, type):
    if type == "json": data = read_json(path)
    elif type == "csv": data = read_csv(path)
    return data

def get_columns(data):
    return list(data[0].keys())

def rename_columns(data, key_mapping):
    new_data = []

    for old_data in data:
        tmp_dict = {}
        for key, value in old_data.items():
            tmp_dict[key_mapping[key]] = value
        new_data.append(tmp_dict)
    return new_data

def get_size_data(data):
    return len(data)

def join_data(data_list):
    grouped_data = []
    for data in data_list:
        grouped_data.extend(data)
    return grouped_data

def add_field(data, filed, value):
    tmp_data = []
    for row in data:
        row[filed] = value
        tmp_data.append(row)
    return tmp_data

def transform_data_table(header, data):
    data_table = [header]
    for row in data:
        new_row = []
        for field in grouped_data_columns:
            new_row.append(row.get(field, "Indispoinivel"))
        data_table.append(new_row)
    return data_table

def write_data(path, data):
    with open(path, 'w') as file_to_save:
        writer = csv.writer(file_to_save)
        writer.writerows(data)

# Leitura dos dados
path_json = "data_raw/dados_empresaA.json"
path_csv  = "data_raw/dados_empresaB.csv"

json_data = read_data(path_json, "json")
csv_data = read_data(path_csv, "csv")

json_columns = get_columns(json_data)
csv_columns = get_columns(csv_data)
print_info(f"Campos do arquivo JSON: {len(json_columns)} {json_columns}")
print_info(f"Campos do arquivo CSV:  {len(csv_columns)} {csv_columns}")


# Transformação dos dados
key_mapping = {
    'Nome do Item' : 'Nome do Produto',
    'Classificação do Produto' : 'Categoria do Produto',
    'Valor em Reais (R$)' : 'Preço do Produto (R$)',
    'Quantidade em Estoque' : 'Quantidade em Estoque',
    'Nome da Loja' : 'Filial',
    'Data da Venda' : 'Data da Venda'
}

print_info("Renomeando campos dos dados CSV")
csv_data = rename_columns(csv_data, key_mapping)
csv_columns = get_columns(csv_data)
print_info(f"Campos do arquivo CSV renomeados. Novos campos: {csv_columns}")

print_info("Incluindo campo 'Data da Venda' nos dados do JSON.")
json_data = add_field(json_data, "Data da Venda", "Indisponivel")
json_columns = get_columns(json_data)
print_info(f"Campo 'Data da Venda' incluido nos dados do JSON. Novos Campos: {json_columns}")


# Agrupando os dados
print_info(f"Numero de registros JSON: {get_size_data(json_data)}")
print_info(f"Numero de registros CSV: {get_size_data(csv_data)}")

grouped_data = join_data([json_data, csv_data])
print_info(f"Numero de registros dos Dados Agrupados: {get_size_data(grouped_data)}")

grouped_data_columns = get_columns(grouped_data)
print_info(f"Campos dos Dados Agrupados: {grouped_data_columns}")


# Salvando dados
grouped_data_table = transform_data_table(grouped_data_columns, grouped_data)

path_grouped_data = 'data_prossesed/dados_agrupados.csv'
print_info(f"Salvando dados agrupados.")
write_data(path_grouped_data, grouped_data_table)
print_info(f"Dados salvos com sucesso em {path_grouped_data}")
import json
import csv

class Dados:

    def __init__(self, path=None, data_format=None, data=None):
        self.path = path
        self.data_format = data_format
        self.data = self.read_data() if data is None else data
        self.columns = self.get_columns()
        self.size = self.get_size_data()
    
    def read_json(self):
        with open(self.path, 'r') as file:
            json_data = json.load(file)
        return json_data

    def read_csv(self):
        csv_data = []

        with open(self.path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                csv_data.append(row)
        return csv_data

    def read_data(self):
        if self.data_format == "json": data = self.read_json()
        elif self.data_format == "csv": data = self.read_csv()
        return data
    
    def get_columns(self):
        return list(self.data[0].keys())

    def get_size_data(self):
        return len(self.data)
    
    def add_field(self, filed, value):
        tmp_data = []
        for row in self.data:
            row[filed] = value
            tmp_data.append(row)
        self.data = tmp_data
        self.columns = self.get_columns()

    def rename_columns(self, key_mapping):
        new_data = []

        for old_data in self.data:
            tmp_dict = {}
            for key, value in old_data.items():
                tmp_dict[key_mapping[key]] = value
            new_data.append(tmp_dict)
        self.data = new_data
        self.columns = self.get_columns()

    def join_data(data_list):
        grouped_data = []
        for data in data_list:
            grouped_data.extend(data)
        return Dados(data=grouped_data)

    def transform_data_table(self):
        data_table = [self.columns]
        for row in self.data:
            new_row = []
            for field in self.columns:
                new_row.append(row.get(field, "Indispoinivel"))
            data_table.append(new_row)
        return data_table

    def write_data(self, path):
        data_table = self.transform_data_table()
        with open(path, 'w') as file_to_save:
            writer = csv.writer(file_to_save)
            writer.writerows(data_table)

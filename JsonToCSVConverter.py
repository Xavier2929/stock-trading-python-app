import csv


class JsonToCSVConverter:
    def Convert(self, schema_object_example, jsonData, csv_filename):
        field_names = list(schema_object_example.keys())
        with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=field_names)
            for data in jsonData:
                row = {key: data.get(key, '') for key in field_names}
                writer.writerow(row)
        print(f'Wrote {len(jsonData)} rows to {csv_filename}')

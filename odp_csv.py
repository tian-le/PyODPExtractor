import csv,gzip

class odp_csv:
    def __init__(self, file_name, t_fields):
        self.file = gzip.open(file_name, 'wt', newline='', encoding="utf-8")
        self.writer = csv.writer(self.file)
        flds = ['REQID', 'PAKID', 'RECORD']
        for col in t_fields:
            flds.append(col['NAME'])
        self.writer.writerow(flds)

    def write_data(self, rows):
        self.writer.writerows(rows)
        self.file.close()

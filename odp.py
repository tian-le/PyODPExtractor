# coding: utf-8

from odp_extractor import extractor
from odp_db_snowflake import db_snowflake
from odp_csv import odp_csv
# import getpass
# from pyodbc import connect

# import boto3
# # Retrieve the list of existing buckets
# s3 = boto3.client('s3')
# response = s3.list_buckets()
#
# # Output the bucket names
# print('Existing buckets:')
# for bucket in response['Buckets']:
#     print(f'  {bucket["Name"]}')
# exit(0)

# src = 'R1D'
src = 'R1I'
# datasource = u'0COMP_CODE_ATTR'
# datasource = u'0CUSTOMER_ATTR'
# datasource = u'0CUSTOMER_TEXT'
# datasource = u'2LIS_11_VAHDR'
datasource = u'2LIS_11_VAITM'
mode = u'F'
#mode = u'D'

ext = extractor(src)
ext.get_details(datasource)
ext.open(mode)

db = db_snowflake(connection_name = "myconnection")
db.prepare('%s_%s' % (src, datasource), ext.t_fields)
db.create_table()

# csv = odp_csv('%s_%s_%s_%s.csv' % (src, datasource, mode, ext.pointer), ext.t_fields)

m = False
i = 1
while not m:
    m, rows = ext.fetch()
    if not rows:
        continue
    csv = odp_csv('%s_%s_%s_%s_%d.csv.gz' % (src, datasource, mode, ext.pointer, i), ext.t_fields)
    csv.write_data(rows)
    # o = 0
    # while o < len(rows):
    #     db.insert_data(rows[o:o+16000])
    #     o += 16000
    i += 1

print(ext.tt)
print(db.tt)

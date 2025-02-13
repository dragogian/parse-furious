import os

# csv_directory = './'  # Directory contenente i file CSV
# output_file = 'import_data.sql'
#
# with open(output_file, 'w') as f:
#     for filename in os.listdir(csv_directory):
#         if filename.endswith('.csv'):
#             table_name = os.path.splitext(filename)[0]
#             f.write(f"""
# LOAD DATA INFILE '/docker-entrypoint-initdb.d/{filename}'
# INTO TABLE sh_{table_name}
# FIELDS TERMINATED BY ','
# ENCLOSED BY '"'
# LINES TERMINATED BY '\\n'
# IGNORE 1 ROWS;
# """)
#
# print(f"Script SQL generato: {output_file}")
#
#
# import os

import os

csv_directory = './'  # Directory contenente i file CSV

volume_strings = []

for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        volume_strings.append(f"      - ./{filename}:/docker-entrypoint-initdb.d/{filename}")

for volume_string in volume_strings:
    print(volume_string)
import pandas as pd
import os

columns = ['id1', 'id2', 'name1', 'id3', 'pay_id', 'amount', 'cgi_id', 'date']

def get_filenames(data_folder, extention):
    files = []
    for filename_short in os.listdir(data_folder):
        if filename_short.endswith(f".{extention}"):
            filename = os.path.join(data_folder, filename_short)
            files.append((filename, filename_short))
    return files

def file_to_df(filename, filename_short):
    df = pd.read_csv(filename, delimiter=';', header=None)
    df.columns = columns
    df['amount'] = df.apply(lambda row: float(str(row['amount']).replace(',', '.')), axis=1)
    df['file'] = filename_short
    return df

def calculate_totals(data_folder, extention, columns, result_file, result_grouped_file):
    files = get_filenames(data_folder, extention)
    if len(files)>0:
        filenames = '\n'.join([x[0] for x in files])
        print(f'{len(files)} files discovered:\n{filenames}\n')

        df = file_to_df(*files[0])
        for file in files[1:]:
            df_new = file_to_df(*file)
            df = df.append(df_new)

        result_df = df.aggregate({'amount':['sum', 'count']})
        result_df.to_csv(result_file)

        result_grouped_df = df.groupby(by='file').aggregate({'amount':['sum', 'count']})
        result_grouped_df.to_csv(result_grouped_file)

        print('Results ->\n')
        print(result_df)
        print(result_grouped_df)
        print(f'\nResults are written into {result_file}, {result_grouped_file}')
    else:
        'No files discovered'

### these fields can be changed 
data_folder = 'data'
extention = 'csv'
result_file = 'result.csv'
result_grouped_file = 'result_grouped.csv'
###

calculate_totals(data_folder, extention, columns, result_file, result_grouped_file)
import numpy as np
from numpy import add
import pandas as pd

####################################################
# DataFrame add JSON for API
#####################################################

def json_api (csv):
    ## make a copy of csv dataframe to apply json for API
    data_json_tmp = pd.DataFrame(csv_file_data)

    ## Creat new JSON column from the CSV dataframe
    data_json_tmp['json'] = data_json_tmp.to_json(orient='records', lines=True).splitlines()

    ## Take out json column and put it into new json dataframe
    json_columns = data_json_tmp['json']
 
    ## print out new json dataframe to file
    np.savetxt(r'./json.txt', json_columns.values, fmt='%s')
    print('File has been created!')

json_api(csv_file_data)
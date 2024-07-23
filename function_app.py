import azure.functions as func
import logging
import time
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
 
 
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
 
@app.route(route="capex_files_tratamento_automation")
def capex_files_tratamento_automation(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
 
    #  ------------------------------------------------------------------------ Principal function -----------------------------------------------------------------------------------------
    #enter credentials
    account_name = '**********************'
    account_key = '***************************'
    container_name = '***********'
    container_output_name = 'bronze'
 
    #create a client to interact with blob storage
    connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
 
    #use the client to connect to the container
    container_client = blob_service_client.get_container_client(container_name)
    container_client
 
    # Selecione o blob
    blob_output_name = "***********.csv"
 
    #downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
    #blob_text = downloader.readall()
 
    blob_list = []
    final_columns = dict(Account=[], Description=[], Comit=[], variable=[],value=[],Projeto=[],Order=[])
    df_final = pd.DataFrame(final_columns)
    for blob_i in container_client.list_blobs():
        blob_list.append(blob_i.name)
       
        sas_i = generate_blob_sas(account_name = account_name,
                                        container_name = container_name,
                                        blob_name = blob_i.name,
                                        account_key=account_key,
                                        permission=BlobSasPermissions(read=True),
                                        expiry=datetime.utcnow() + timedelta(hours=1))
 
        sas_url = 'https://' + account_name+'.blob.core.windows.net/' + container_name + '/' + blob_i.name + '?' + sas_i
        df = pd.read_excel(sas_url,sheet_name='Schedule')
 
                    #variables
        project_name = df.loc[1, 'Unnamed: 19']
        order = df.loc[0, 'Unnamed: 2']
        next_year = df.loc[3, 'Unnamed: 16']
        current_year = df.loc[89, 'Unnamed: 16']

        #Dropping columns)
        df = df.drop(labels = 'Unnamed: 0',axis =1)
        df = df.drop(labels = 'Unnamed: 4',axis =1)
        df = df.drop(labels = 'Unnamed: 5',axis =1)
        df = df.drop(labels = 'Unnamed: 6',axis =1)
        df = df.drop(labels = 'Unnamed: 7',axis =1)
        df = df.drop(labels = 'Unnamed: 8',axis =1)
        df = df.drop(labels = 'Unnamed: 9',axis =1)
        df = df.drop(labels = 'Unnamed: 10',axis =1)
        df = df.drop(labels = 'Unnamed: 11',axis =1)
        df = df.drop(labels = 'Unnamed: 12',axis =1)
        df = df.drop(labels = 'Unnamed: 13',axis =1)
        df = df.drop(labels = 'Unnamed: 14',axis =1)
        df = df.drop(labels = 'Unnamed: 15',axis =1)
        df = df.drop(labels = 'Unnamed: 17',axis =1)
        df = df.drop(labels = 'Unnamed: 18',axis =1)
        df = df.drop(labels = 'Unnamed: 31',axis =1)
        df = df.drop(labels = 'Unnamed: 32',axis =1)
        df = df.drop(labels = 'Unnamed: 33',axis =1)
        df = df.drop(labels = 'Unnamed: 34',axis =1)
        #Dropping Lines
        df = df.drop(labels = [0,1,4,5,6],axis = 0)
        df.reset_index(drop=True)
        maxindex = df.index.max()
        for i in range(172,maxindex+1):
            df = df.drop(labels =[i],axis = 0)
        #adicionando mês como cabeçalho
        df = df.rename(columns=df.iloc[1]).drop(df.index[1])
        df.rename(columns={np.nan: 'Comit'}, inplace=True)
        df.rename(columns={'variable    ': 'Month'}, inplace=True)
        #Adicionando colune de ano baseado no INdex e variaveis previamente criadas
        df['Ano'] = df.index.to_series().apply(lambda x: next_year if x <=86 else current_year)
        #preenchendo valores nulos
        df['Account'].fillna(method='ffill', inplace=True)
        df['Description'].fillna(method='ffill', inplace=True)
        #Exluding total column and dropping lines
        df = df.drop(df.columns[3], axis = 1)
        df = df.drop(labels = [87,88,89,90,92],axis = 0)
        #unpivot
        df = df.melt(id_vars=['Account','Description','Comit','Ano'], value_vars=['Jan', 'Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
        #creating columns
        df['Projeto'] = project_name
        df['Order'] = order
        #Filtering tb comit
        df = df[df["Comit"] == "tb"]
        df = df[df["Account"] != "Total"]
        df = df[df["Account"] != 1]
        df = df[df["Account"] != 2]
        df = df[df["Account"] != 3]
        df = df[df["Account"] != 4]
        df = df[df["Account"] != 5]
        df = df[df["Account"] != 6]
        df = df[df["Account"] != 7]
        df = df[df["Account"] != 8]
        df = df[df["Account"] != 9]
        df = df[df["Account"] != 10]

        #Reset Index
        df.reset_index(inplace=True)

        time.sleep(0.5)

        df_final = pd.concat([df, df_final])
    df_final['variable'].replace({'Jan': '01', 
                    'Feb': '02',
                    'Mar': '03',
                    'Apr': '04',
                    'May': '05',
                    'Jun': '06',
                    'Jul': '07',
                    'Aug': '08',
                    'Sep': '09',
                    'Oct': '10',
                    'Nov': '11',
                    'Dec': '12'
                    }, inplace = True)
    #SAVING IN BLOB STORAGE
    csv_string = df_final.to_csv(index=False)
    # Convert the CSV string to a bytes object
    csv_bytes = str.encode(csv_string)
 
    # Upload the bytes object to Azure Blob Storage
    blob_client = blob_service_client.get_blob_client(container=container_output_name, blob=blob_output_name)
    blob_client.upload_blob(csv_bytes, overwrite=True)
 
 
    return func.HttpResponse(
             "The ETL had completed",
             status_code=200
        )
 
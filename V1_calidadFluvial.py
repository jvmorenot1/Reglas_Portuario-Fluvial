#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system('pip install pyspark')


# In[12]:


get_ipython().system('pip install pandera')


# In[73]:


from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType


# In[4]:


import datetime


# In[36]:


import pickle
import os.path
import matplotlib.pyplot as plt
from operator import length_hint
import numpy as np
import pandas as pd
import seaborn as sns
import calendar
import sklearn
# import ydata_profiling
from scipy.stats import chi2_contingency
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.impute import KNNImputer, SimpleImputer
#from sklearn.linear_model import Lasso, LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import FeatureUnion, make_pipeline, make_union
from sklearn.preprocessing import (
    MinMaxScaler,
    OneHotEncoder,
    OrdinalEncoder,
    PolynomialFeatures,
    QuantileTransformer,
    StandardScaler,
)


# In[13]:


import pandera as pa
import yaml
from pandera import DataFrameSchema, Column, Check, Index, MultiIndex
# envio de notificaciones
import smtplib 
from email.message import EmailMessage 
# logs asociados
import logging


# In[5]:


import json


# In[16]:


# Ruta al archivo JSON que contiene la configuración
json_file = "config1.json"

# Leer el archivo JSON
with open(json_file, 'r') as file:
    config = json.load(file)

# Acceder a las configuraciones
File=config["File"]


# In[14]:


# Configurar el módulo de registro
logging.basicConfig(filename='registro.log', level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Crear un registro
logger = logging.getLogger('mi_aplicacion')


# In[63]:


def mount_container_safe(mount_point, container):
    if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source = f"wasbs://{container}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            mount_point = mount_point,
            extra_configs = {f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net":"a4M4PeecNVL378jZGZ7zLubgaPLkCDdNDc1UnTajnQ5QOyX2qhfuMphExwbRHl5dFRJbrQQpg/9V+ASt1ZVG2g=="})


# In[7]:


def enviarCorreo(message_send):
    email_subject = config['email_subject'] 
    sender_email_address = config['sender_email_address']
    receiver_email_address = config["receiver_email_address"] 
    email_smtp = config["email_smtp"] 
    email_password = config['email_password']

    # Create an email message object 
    message = EmailMessage() 

    # Configure email headers 
    message['Subject'] = email_subject 
    message['From'] = sender_email_address 
    message['To'] = receiver_email_address 

    # Set email body text 
    message.set_content(message_send) 

    # Set smtp server and port 
    server = smtplib.SMTP(email_smtp, '587') 

    # Identify this client to the SMTP server 
    server.ehlo() 

    # Secure the SMTP connection 
    server.starttls() 

    # Login to email account 
    server.login(sender_email_address, email_password) 

    # Send email 
    server.send_message(message) 

    # Close connection to server 
    server.quit()


# In[65]:


def xlsx_read_sheet(file_path, schema):
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .schema(schema) \
        .load(file_path)
    return df


# In[8]:


def readAllSheets(filename):
    if not os.path.isfile(filename):
        return None
    
    xls = pd.ExcelFile(filename)
    sheets = xls.sheet_names
    results = {}
    for sheet in sheets:
        results[sheet] = xls.parse(sheet)
        
    xls.close()
    return results, sheets


# In[69]:


def readCsv(path,schema):
    df = spark.read.option("header", "true") \
          .schema(schema) \
          .csv(path)
    return df


# In[71]:


def readingFiles(name,typeFile):

    if(typeFile == 'csv'):
        result = readCsv(name,schema_read)
    elif(typeFile == "xlsx"):
        result= readAllSheets(name,schema_read)
    return result
    


# In[53]:


schema_analyse = DataFrameSchema.from_yaml(File['yaml_path'])


# In[62]:


schema_read = StructType([
    StructField("NIT", IntegerType()),
    StructField("Razon_social", StringType()),
    StructField("Sigla", StringType()),
    StructField("Cod_Depto", StringType()),
    StructField("Cod_Mun", StringType()),
    StructField("Departamento", StringType()),
    StructField("Municipio", StringType()),
    StructField("Estado", StringType()),
    StructField("Fecha_inclusion", DateType(), True),
    StructField("Fecha_baja", DateType(), True),
    StructField("Cuerpo_agua", StringType()),
    StructField("Zona_operacion", StringType()),
    StructField("Nota", StringType()),
])


# In[75]:


df = readingFiles(File['Path'],File['type'])


# In[ ]:


df.info()


# In[ ]:


try:
    schema(df)
    print("El dataframe"+ sheets[1]+" se valido correctamente, ahora se convertira en formato parquet")
    message_ok = "El dataframe"+ sheets[1] + " se valido correctamente, ahora se convertira en formato parquet para enviarse a la zona Silver"
    #logs
    logging.debug(message_ok)
except pa.errors.SchemaError as e:
    print(f"El DataFrame no cumple con el esquema de Pandera:\n{e}")
    message_bad = f"El dataframe no cumple con el esquema, sera procesado a la zona silver con los errores visualizados debido a: {e}"
    logging.warning(message_bad)
    logging.error(e)
    enviarCorreo(message_bad)


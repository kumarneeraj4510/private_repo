# Importing relevant python packages
import requests
from pprintpp import pprint
import pandas as pd
import time
import csv

# Reading the input file and storing in DataFrame object in pandas #
cust = pd.read_csv('input_dataset.csv')
cust1 = cust

# Declaring the lists
all_customers = []
missed_customer_all = []
customer_Details = []

# iteration for processing each row from input file
for index, row in cust.iterrows():
    if index >= 0:
        LEIVALUE = row['lei']
        transaction_uti = row['transaction_uti']
        LEIVALUE = LEIVALUE.strip()
        url = f"https://api.gleif.org/api/v1/lei-records?filter[lei]={LEIVALUE}"
        # Calling the API
        response = requests.get(url)
        #Handling too many requests in a short period of time
        if response.status_code == 429:
            time.sleep(int(response.headers["Retry-After"]))
            time.sleep(1)
            missed_customer = [LEIVALUE, transaction_uti]
            missed_customer_all.append(missed_customer)
            continue
        data1 = response.json()
        try:
            data = data1["data"][0]["attributes"]

            # Extracting legalName
            legalName = data["entity"]["legalName"]["name"]

            # Extracting BIC balues
            bic = data["bic"]
            bic_string = ','.join(bic)

            # Handling for trnasaction cost calculation
            legalCountry = data["entity"]["legalAddress"]["country"]
            if legalCountry == 'GB':
                transaction_costs = row['notional'] * row['rate'] - row['notional']
            if legalCountry == 'NL':
                transaction_costs = abs(row['notional'] * (1 / row['rate']) - row['notional'])

            customer = [transaction_uti,LEIVALUE, legalName, transaction_costs, bic_string]
            all_customers.append(customer)

        except Exception as e:
            missed_customer = [LEIVALUE,transaction_uti]
            missed_customer_all.append(missed_customer)


#Creating csv file from for output
df1=pd.DataFrame(all_customers)
df1.columns = ['transaction_uti','lei','legalName','transaction_costs','bic']
df_merged = cust.merge(df1[['transaction_uti','legalName','transaction_costs','bic']], on='transaction_uti')
df_merged.to_csv('Output_Data.csv')

#Creating csv file from for missed records
df2=pd.DataFrame(missed_customer_all)
#df2.columns = ['LEI','transaction_uti']
df2.to_csv('Missed_Error_Transactions.csv')
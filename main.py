import requests
import json
import pandas
from pandas.io.json import json_normalize
from tabulate import tabulate


try:
    # API configurations are stored in separate JSON file. Read that json to extract the details
    f = open('config.json',)
    config = json.load(f)

    headers = config["headers"]

    # Get the response from API url
    response = requests.request("GET", config["api_name"]["url"], headers=headers, params=config["api_name"]["querystring"])
    data = response.text

    # Parse the response text
    parsed_data = json.loads(data)

    # Extract the method name from url
    file_name=config["api_name"]["url"].split('/')[-1]

    # Convert the json response to dataframe
    df_result = json_normalize(parsed_data['finance']['result'])

    # Convert epoch timestamp to Datetime
    convert = lambda x: datetime.fromtimestamp(x / 1e3)
    df_result['startDateTime'] = df_result['startDateTime'].apply(convert)

    # Display dataframe in tabular format
    print(tabulate(df_result, headers='keys', tablefmt='psql'))
    print("Creating file")
    # Create a CSV from dataframe
    df_result.to_csv(file_name+'.csv',index=False)
    print("File has been created")

    # Raise error in case of failure
    response.raise_for_status()

except requests.exceptions.HTTPError as httpErr:
    print ("Http Error:", httpErr)
except requests.exceptions.ConnectionError as connErr:
    print ("Error Connecting:", connErr)
except requests.exceptions.Timeout as timeOutErr:
    print ("Timeout Error:", timeOutErr)
except requests.exceptions.RequestException as reqErr:
    print ("Something Else:", reqErr)

import requests
import sys
import json
import pandas
from tabulate import tabulate
from datetime import datetime


class ExtractData:
    """ This class contains methods to extract the data from an API and perform some data manipulations """

    def get_data(self, config):
        """
        Extracts data from an API and returns a parsed json
        :param config
        :return: Parsed json
        """
        try:
            headers = config["headers"]
            # Get the response from API url
            response = requests.request("GET", config["api_name"]["url"], headers=headers,
                                        params=config["api_name"]["querystring"])
            data = response.text
            # Parse the response text
            parsed_data = json.loads(data)
            return parsed_data

        except requests.exceptions.HTTPError as httpErr:
            print("Http Error:", httpErr)
        except requests.exceptions.ConnectionError as connErr:
            print("Error Connecting:", connErr)
        except requests.exceptions.Timeout as timeOutErr:
            print("Timeout Error:", timeOutErr)
        except requests.exceptions.RequestException as reqErr:
            print("Something Else:", reqErr)

    def generate_dataframe(self, data):
        """
        Converts a JSON to Dataframe
        :param data
        :return: Dataframe
        """
        return pandas.json_normalize(data['finance']['result'])

    def transform_data(self, data):
        """
        Convert epoch timestamp to Datetime
        :param data:
        :return: Dataframe with updated datetime
        """

        convert = lambda x: datetime.fromtimestamp(x / 1e3)
        data['startDateTime'] = data['startDateTime'].apply(convert)
        return data

    def generate_file(self, data, file_name):
        """
        Create a CSV from dataframe
        :param data
        :param file_name
        """
        print("Creating file....")
        data.to_csv(file_name + '.csv', index=False)
        print("File has been created..!!")


def main():
    if len(sys.argv) < 1:
        print("Expected config file name")
    else:
        f = open(sys.argv[1])
        config = json.load(f)
        extract_data = ExtractData()

        data = extract_data.get_data(config)
        result_df = extract_data.generate_dataframe(data)

        transformed_df = extract_data.transform_data(result_df)
        # Extract the method name from url
        file_name = config["api_name"]["url"].split('/')[-1]
        extract_data.generate_file(transformed_df, file_name)

if __name__ == "__main__":
    main()

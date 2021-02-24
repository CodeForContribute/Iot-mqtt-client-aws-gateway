import csv

import pandas as pd


class Util:
    """
    This is a helper class consisting of all the helper functions to be imported and used in
    other modules.
    """

    def __init__(self):
        pass

    @staticmethod
    def convert_csv_to_json(csv_file_path):
        """
        :param csv_file_path:
        :return json data converted from CSV file.
        """
        q, json_data = [], {}

        with open(csv_file_path, encoding="utf-8") as csvf:
            csv_reader = csv.DictReader(csvf)
            # convert each row into a dictionary and add it to the data
            for rows in csv_reader:
                q.append(rows)
            json_data.update({"sensor_data": q})
        return json_data

    @staticmethod
    def save_the_data_to_csv_file(json_data, csv_file_path='sensor_data/output_sensor_datasets.csv'):
        # Opening JSON file and loading the data
        # into the variable data
        sensor_data = json_data
        # now we will open a file for writing
        with open(csv_file_path, 'a') as data_file:
            # create the csv writer object
            csv_writer = csv.writer(data_file)
            csv_writer.writerow(sensor_data.values())


if __name__ == '__main__':
    util_obj = Util()
    input_csv_file_path = "sensor_data/input_sensor_datasets.csv"
    json_data = util_obj.convert_csv_to_json(csv_file_path)
    print(json_data)
    output_csv_file_path = "sensor_data/output_sensor_datasets.csv"
    util_obj.convert_json_to_csv(json_data, csv_file_path)

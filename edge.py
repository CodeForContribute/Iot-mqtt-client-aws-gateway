import json
import queue
import requests
import threading
import time
from queue import Queue
from threading import Thread

from util import Util


class Edge:
    def __init__(self):
        # queues to process the sensor data
        #used 2 queues as we have to tell the count of buffered data at any point of time
        # otherwise 1 queue would be sufficient for the requirements
        self.normal_data_q = Queue()
        self._buffered_data_queue = Queue()

        # msg count at any point of time
        self._successfully_transmitted_data_count = 0

        # read_data_thread will read the data from the sensor and populate the data in the normal_q,
        # which will be sent to http server to be processed.
        self.read_data_thread = threading.Thread(target=self.read_data_from_sensor)
        # normal_data_thread will send the sensor data to http server once it is populated in to the q
        self.normal_data_thread = threading.Thread(target=self.publish_data_to_http_server_normally)
        # buffered_data_thread will send the sensor data to http server once it is populated in to the buffered q
        self.buffered_data_thread = threading.Thread(target=self.publish_buffered_data_to_http_server)

    def edge_start(self):
        """
        starts all the consumer and producer threads in the Edge class
        """
        self.read_data_thread.start()
        self.normal_data_thread.start()
        self.buffered_data_thread.start()

    def edge_stop(self):
        """
        stops all the consumer and producer threads in the Edge class
        """
        self.read_data_thread.join()
        self.normal_data_thread.join()
        self.buffered_data_thread.join()

    def read_data_from_sensor(self):
        """
        function that produces data to be consumed by consumer thread
        In this case this function will read the data from the input datasets and put into the Queue(),which
        will be processed by other consumer functions like - publish_data_to_http_server_normally
        """
        json_data = Util.convert_csv_to_json("sensor_data/input_sensor_datasets.csv")
        # we are using queue.Queue() as it is a better candidate as it will wait
        # till the data will be available in the queue and queue.get() call is generally the blocking call by default,
        for d in json_data["sensor_data"]:
            # to mimic the scenario to read the data every 60 secs as per the requirement
            time.sleep(60)
            self.normal_data_q.put(d)

    def publish_data_to_http_server_normally(self):
        """
        consumes the sensor data from the normal_data_q and sends the sensor data to http end-point
        """
        while self.normal_data_q:
            # q.get() function is a blocking queue- wait for the data to be populated into the queue, will not give
            # any error, if there is not sensor data to processed
            data = self.normal_data_q.get()
            response = self.publish_data_to_server(data)
            if response == 200:
                print("message - %s sent to server successfully!" % data)
            else:
                print("message - %s sending failed!!!!" % data)
                # if sending of a message to http server fails, add the sensor data to the buffered queue, to be
                # processed paralley in a seperate thread
                self._buffered_data_queue.put(data)

    def publish_buffered_data_to_http_server(self):
        """
        consumes the sensor data from the buffered_data_q and sends the sensor data to http end-point to be
        consumed every 5 sec by the server
        """
        while self._buffered_data_queue:
            # as per the requirement, the buffered data is to be sent every 5 sec
            time.sleep(5)
            data = self._buffered_data_queue.get()
            response = self.publish_data_to_server(data)
            if response == 200:
                print("message - %s sent to server successfully!" % data)
            else:
                print("message - %s sending failed again so buffering the data into the buffered q!" % data)
                # if the sending of same message fails again, then put the data back into the buffered q
                # to persists the data, as per the requirement/discussion
                self._buffered_data_queue.put(data)

    def publish_data_to_server(self, data):
        """
         publish the data on the http server
        :return:response code 200 for successful case, other than 200 is considered as for failure case
        """
        try:
            # http end-point to which the client(edge) will send the sensor data to
            api_url = 'http://127.0.0.1:5000/sensor_data'
            r = requests.post(url=api_url, data=json.dumps(data).encode("utf-8"))
            if r.status_code == 200:
                # if the data processed by the server successfully, increase the count of
                # successfully transmitted data
                self._successfully_transmitted_data_count += 1
            return r.status_code
        except Exception as err:
            # if the data could not be processed due to exception send the 400 response code
            return 400

    def count_successfully_transmitted_data(self):
        """
        :return: count of successfully transmitted data at any point of time
        """
        return self._successfully_transmitted_data_count

    def buffered_data(self):
        """
        :return: count of buffered data at any point of time
        """
        return self._buffered_data_queue.qsize()


if __name__ == '__main__':
    obj = Edge()
    obj.edge_start()

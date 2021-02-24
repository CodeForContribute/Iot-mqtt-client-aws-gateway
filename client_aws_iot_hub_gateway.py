import logging
import ssl

import paho.mqtt.client as paho

logging.basicConfig(level=logging.INFO)


class PubSub(object):
    def __init__(self, listener=False, topic="default"):
        self.__connect = False
        self.__listener = listener
        self.__topic = topic
        self._logger = logging.getLogger(repr(self))

    def __on_connect(self, client, userdata, flags, rc):
        self.__connect = True
        if self.__listener:
            pass

    def bootstrap_mqtt(self):
        # paho mqtt client
        self._mqtt_client = paho.Client()

        # register callback functions
        self._mqtt_client.on_connect = self.__on_connect
        self._mqtt_client.on_message = self.__on_message
        self._mqtt_client.on_log = self.__on_log

        # aws credentials
        aws_host = ""
        aws_port = 8883
        ca_path = ""
        cert_path = ""
        key_path = ""

        self._mqtt_client.tls_set(ca_path,
                                  certfile=cert_path,
                                  keyfile=key_path,
                                  cert_reqs=ssl.CERT_REQUIRED,
                                  tls_version=ssl.PROTOCOL_TLSv1,
                                  ciphers=None)
        rc = self._mqtt_client.connect(aws_host, aws_port, keepalive=120)
        if rc == 0:
            self.__connect = True
        return self

    def start(self):
        self._mqtt_client.loop_start()
        while True:
            sleep(2)
            if self.__connect:
                self._mqtt_client.publish(self.__topic, json.dumps({"message": "Hello world"}))
            else:
                self._logger.debug("Attempting to connect")


if __name__ == "__main__":
    PubSub(listener=True, topic="gaetway_data").bootstrap_mqtt().start()

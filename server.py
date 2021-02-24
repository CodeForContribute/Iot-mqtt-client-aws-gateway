from flask import Flask, request

app = Flask(__name__)
import json
import random
from util import Util


@app.route('/sensor_data', methods=["POST"])
def hello():
    if request.method == 'POST':
        data = json.loads(request.data.decode("utf-8"))
        print("data from sensor==", data)
        # used random.choice to bring randomness in the response as per the requirement
        response_codes = [200, 401, 501, 600]
        response = random.choice(response_codes)
        if response == 200:
            try:
                # save the response data to the csv file
                Util.save_the_data_to_csv_file(data)
            except Exception as err:
                print("could not convert the json data to csv format due to :%s" % err)

        return ('', response)


if __name__ == '__main__':
    app.run(debug=True)

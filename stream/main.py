# Copyright 2019 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import sys

from flask import Flask, request
from google.cloud import secretmanager_v1beta1, pubsub_v1
from hashlib import sha1


app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    body = request.data
    print(f"Received request: {body}")
    print(f"Received request: {request}")
    # TODO: verified the signature
    publish_to_pubsub(body)

    sys.stdout.flush()
    return ("", 204)


def publish_to_pubsub(msg):
    """
    Publishes the message to Cloud Pub/Sub
    """
    try:
        publisher = pubsub_v1.PublisherClient()
        # topic_path = publisher.topic_path(
        #     os.environ.get("projects/resonant-craft-381206/"), os.environ.get("topics/nuts_streaming_morail")
        # )

        # Pub/Sub data must be bytestring, attributes must be strings
        future = publisher.publish(topic='projects/resonant-craft-381206/topics/nuts_streaming_morail', data=msg)

        exception = future.exception()
        if exception:
            raise Exception(exception)

        print(f"Published message: {future.result()}")

    except Exception as e:
        # Log any exceptions to stackdriver
        entry = dict(severity="WARNING", message=e)
        print(entry)


if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host="127.0.0.1", port=PORT, debug=True)
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

from flask import Flask, request, jsonify
from google.cloud import pubsub_v1, bigquery
from hashlib import sha1


app = Flask(__name__)

project_id = 'nft-dashboard-381202'
client = bigquery.Client(project=project_id)
topic = 'projects/nft-dashboard-381202/topics/Pudgy_Penguins'

@app.route("/pptoken", methods=["POST"])
def index():
    body = request.data
    print(f"Received request: {body}")
    print(f"Received request: {request}")
    # TODO: verified the signature
    publish_to_pubsub(body)

    sys.stdout.flush()
    return ("", 204)

@app.route('/nftport-all-time')
def get_nft_rank_all():
    return get_nft_data('nftport_all_time')

@app.route('/nftport-daily')
def get_nft_rank_daily():
    return get_nft_data('nftport_daily')

@app.route('/nftport-weekly')
def get_nft_rank_weekly():
    return get_nft_data('nftport_weekly')

@app.route('/nftport-monthly')
def get_nft_rank_monthly():
    return get_nft_data('nftport_monthly')

def get_nft_data(tablename):
    # Query data from BigQuery
    query = f"""
        SELECT *
        FROM `nft-dashboard-381202.nftport_pipeline.{tablename}`
        ORDER BY CAST(rank AS INT64)
    """
    query_job = client.query(query)
    rows = query_job.result()

    # Format data as JSON
    data = []
    for row in rows:
        item = {
            'chain': row.chain,
            'contract_address': row.contract_address,
            'name': row.name,
            'description': row.description,
            'picture': row.picture,
            'rank': row.rank
        }
        data.append(item)

    # Return data as JSON
    return jsonify(data)

def publish_to_pubsub(msg):
    """
    Publishes the message to Cloud Pub/Sub
    """
    try:
        publisher = pubsub_v1.PublisherClient()
        # TODO: change
        future = publisher.publish(topic='topic', data=msg)
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
    app.run(host="127.0.0.1", port=PORT, debug=True)
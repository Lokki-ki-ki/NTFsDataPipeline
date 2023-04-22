from flask import Flask, jsonify
from google.cloud import bigquery

app = Flask(__name__)

# Configure Google Cloud credentials
# Replace `project_id` with your Google Cloud project ID
project_id = 'nft-dashboard-381202'
client = bigquery.Client(project=project_id)

@app.route('/', methods=['GET'])
def index():
    return{
        'name': 'Hello World'
    }

@app.route('/nftport-all-time')
def get_nft_rank_all():
    # Query data from BigQuery
    query = """
        SELECT *
        FROM `nft-dashboard-381202.nftport_pipeline.nftport_all_time`
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

@app.route('/nftport-daily')
def get_nft_rank_daily():
    # Query data from BigQuery
    query = """
        SELECT *
        FROM `nft-dashboard-381202.nftport_pipeline.nftport_daily`
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

@app.route('/nftport-weekly')
def get_nft_rank_weekly():
    # Query data from BigQuery
    query = """
        SELECT *
        FROM `nft-dashboard-381202.nftport_pipeline.nftport_weekly`
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

@app.route('/nftport-monthly')
def get_nft_rank_monthly():
    # Query data from BigQuery
    query = """
        SELECT *
        FROM `nft-dashboard-381202.nftport_pipeline.nftport_monthly`
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

@app.route('/nft_data')
def get_nft_data():
    client = bigquery.Client()
    query = """
        SELECT *
        FROM `nft-dashboard-381202.nfts_pipeline.nfts_pipeline_collection_Cryptopunks`
        UNION ALL
        SELECT *
        FROM `nft-dashboard-381202.nfts_pipeline.nfts_pipeline_collection_Mutant_Alien_Ape_Yacht_Club`
        UNION ALL
        SELECT *
        FROM `nft-dashboard-381202.nfts_pipeline.nfts_pipeline_collection_Perky_Platypus`
        UNION ALL
        SELECT *
        FROM `nft-dashboard-381202.nfts_pipeline.nfts_pipeline_collection_Pudgy_Penguins`
        ORDER BY price
    """
    nft_data = client.query(query).to_dataframe()
    return jsonify(nft_data.to_dict())



if __name__ == '__main__':
    app.run(debug=True)
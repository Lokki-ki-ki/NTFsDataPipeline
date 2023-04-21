import requests
from google.cloud import bigquery
import pandas as pd

# TODO: destroy the apiKey and generate a new one after the project.
class FetchData:
    def __init__(self):
        self.network = "eth-mainnet"
        self.apiKey = "Fr-VZGooptPuK1cZslqGIIFBcYXnyEZG"
        self.headers = {"accept": "application/json"}
        self.num_of_trans = 500

    def get_last_block_number_by_collection(self, collection_name):
        client = bigquery.Client()
        query = f'''
            SELECT block_number
            FROM nfts_pipeline.nfts_pipeline_collection_{collection_name}
            ORDER BY block_number DESC
            LIMIT 1
        '''
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return row.block_number

    # Fun hourly and fetch data of a list of collections
    def fetch_transactions_for_collections(self, collections_dict):
        """
        INPUT: NUll
        OUTPUT: dataframe of transactions for a list of collections
        """
        collections = list(collections_dict.values())
        collections_name = list(collections_dict.keys())
        results = pd.DataFrame()
        for i in range(len(collections)):
            df = self.fetch_transactions_for_a_collection(collections[i], collections_name[i])
            results = pd.concat([results, df], ignore_index=True, sort=False)
        results.reset_index(drop=True, inplace=True)
        path = "/tmp/fetch_transactions_for_collections.csv"
        results.to_csv(path, index=False)
        return path
    
    def initial_transactions_for_collections(self, collections):
        """
        INPUT: NUll
        OUTPUT: dataframe of transactions for a list of collections
        """
        results = pd.DataFrame()
        for collection in collections:
            df = self.initial_transactions_for_a_collection(collection)
            results = pd.concat([results, df], ignore_index=True, sort=False)
        results.reset_index(drop=True, inplace=True)
        path = "/tmp/fetch_transactions_for_collections.csv"
        results.to_csv(path, index=False)
        return path
    
    def transformation(self, trans, collection):
        """
        INPUT: json of transactions fetched from api
        OUTPUT: dataframe of transactions with needed information
        """
        df = pd.DataFrame()
        for tran in trans:
            dic = {}
            dic["collection_address"] = collection
            dic["market_place"] = tran["marketplace"]
            dic["token_id"] = tran["tokenId"]
            dic["seller"] = tran["sellerAddress"]
            dic["buyer"] = tran["buyerAddress"]
            # TODO: transform the price from wei to ether
            dic["price_in_wei"] = int(tran["sellerFee"]["amount"]) if "amount" in tran["sellerFee"] else 0
            dic["price_decimal"] = int(tran["sellerFee"]["decimals"]) if "decimals" in tran["sellerFee"] else 1
            dic["price_currency"] = tran["sellerFee"]["symbol"] if "symbol" in tran["sellerFee"] else None
            dic["price"] = round(dic["price_in_wei"] / 10 ** dic["price_decimal"], 5)
            dic["protocol_fee_in_wei"] = int(tran["protocolFee"]["amount"]) if "amount" in tran["protocolFee"] else 0
            dic["protocol_fee_decimal"] = int(tran["protocolFee"]["decimals"]) if "decimals" in tran["protocolFee"] else 1
            dic["protocol_fee_currency"] = tran["protocolFee"]["symbol"] if "symbol" in tran["protocolFee"] else None
            dic["protocol_fee"] = round(dic["protocol_fee_in_wei"] / 10 ** dic["protocol_fee_decimal"], 5)
            dic["transaction_hash"] = tran["transactionHash"]
            dic["block_number"] = tran["blockNumber"]
            df = pd.concat([df, pd.DataFrame(dic, index=[0])])
        df.drop(columns=["price_in_wei", "price_decimal", "protocol_fee_in_wei", "protocol_fee_decimal"], inplace=True)
        return df
        
    def fetch_transactions_for_a_collection(self, collection, collection_name):
        """
        INPUT: number of transactions to fetch, and collection address
        OUTPUT: dataframe of transactions
        """
        fromBlock = self.get_last_block_number_by_collection(collection_name)
        url_trans = f"https://{self.network}.g.alchemy.com/nft/v2/{self.apiKey}/getNFTSales?fromBlock={fromBlock}&toBlock=latest&order=asc&contractAddress={collection}&limit=1000"
        response = requests.get(url_trans, headers=self.headers).json()
        trans = response["nftSales"]
        if len(trans) == 0:
            return pd.DataFrame()
        df = self.transformation(trans, collection)
        # TODO: save to csv just for reference -> shld save to db
        df.to_csv("nfts_prices_fetch.csv", index=False)
        return df
    
    def initial_transactions_for_a_collection(self, collection, block_number=17000):
        """
        INPUT: number of transactions to fetch, and collection address
        OUTPUT: dataframe of transactions
        """
        # ti = kwargs['ti']
        url_trans = f"https://{self.network}.g.alchemy.com/nft/v2/{self.apiKey}/getNFTSales?fromBlock=0&toBlock=latest&order=asc&contractAddress={collection}&limit=1000"
        response = requests.get(url_trans, headers=self.headers).json()
        trans = response["nftSales"]
        df = self.transformation(trans, collection)
        # TODO: save to csv just for reference -> shld save to db
        df.to_csv("nfts_prices_initialize.csv", index=False)
        return df
    
if __name__ == "__main__":
    fetch_data = FetchData()
    # print(fetch_data.get_last_block_number())
    fetch_data.initial_transactions_for_a_collection('0x793f969bc50a848efd57e5ad177ffa26773e4b14', block_number=17000)
    # fetch_data.fetch_transactions_for_collections(["0x793f969bc50a848efd57e5ad177ffa26773e4b14", "0xb66a603f4cfe17e3d27b87a8bfcad319856518b8", "0xd774557b647330C91Bf44cfEAB205095f7E6c367",])
    # fetch_data.fetch_transactions_for_a_collection("0x793f969bc50a848efd57e5ad177ffa26773e4b14")
    # url_trans = f"https://{fetch_data.network}.g.alchemy.com/nft/v2/{fetch_data.apiKey}/getNFTSales?fromBlock=latest&order=desc&contractAddress=0x793f969bc50a848efd57e5ad177ffa26773e4b14&limit={fetch_data.num_of_trans}"
    # response = requests.get(url_trans, headers=fetch_data.headers)
    # print(url_trans)
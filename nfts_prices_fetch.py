import requests
import pandas as pd

# TODO: destroy the apiKey and generate a new one after the project.
class FetchData:
    def __init__(self):
        self.network = "eth-mainnet"
        self.apiKey = "Fr-VZGooptPuK1cZslqGIIFBcYXnyEZG"
        self.headers = {"accept": "application/json"}
        self.collections = ["0x793f969bc50a848efd57e5ad177ffa26773e4b14", "0xb66a603f4cfe17e3d27b87a8bfcad319856518b8", "0xd774557b647330C91Bf44cfEAB205095f7E6c367",]
        self.num_of_trans = 500

    # Fun hourly and fetch data of a list of collections
    def fetch_transactions_for_collections(self, collections):
        """
        INPUT: NUll
        OUTPUT: dataframe of transactions for a list of collections
        """
        results = pd.DataFrame()
        # url = f"https://{self.network}.g.alchemy.com/nft/v2/{self.apiKey}/getContractMetadata?contractAddress={collection}"
        # collections = ["0x793f969bc50a848efd57e5ad177ffa26773e4b14", "0xb66a603f4cfe17e3d27b87a8bfcad319856518b8", "0xd774557b647330C91Bf44cfEAB205095f7E6c367",]
        for collection in collections:
            df = self.fetch_transactions_for_a_collection(collection)
            results = pd.concat([results, df], ignore_index=True, sort=False)
        results.reset_index(drop=True, inplace=True)
        path = "/tmp/fetch_transactions_for_collections.csv"
        results.to_csv(path, index=False)
        return path
        
    def fetch_transactions_for_a_collection(self, collection, block_number=17000):
        """
        INPUT: number of transactions to fetch, and collection address
        OUTPUT: dataframe of transactions
        """
        # ti = kwargs['ti']
        url_trans = f"https://{self.network}.g.alchemy.com/nft/v2/{self.apiKey}/getNFTSales?fromBlock={block_number}&toBlock=latest&order=asc&contractAddress={collection}&limit={self.num_of_trans}"
        response = requests.get(url_trans, headers=self.headers).json()
        trans = response["nftSales"]
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
        # TODO: save to csv just for reference -> shld save to db
        df.to_csv("nfts_prices.csv", index=False)
        # df.to_csv("/tmp/fetch_transactions_for_collection.csv", index=False)
        return df
    
if __name__ == "__main__":
    fetch_data = FetchData()
    fetch_data.fetch_transactions_for_collections(["0x793f969bc50a848efd57e5ad177ffa26773e4b14", "0xb66a603f4cfe17e3d27b87a8bfcad319856518b8", "0xd774557b647330C91Bf44cfEAB205095f7E6c367",])
    # fetch_data.fetch_transactions_for_a_collection("0x793f969bc50a848efd57e5ad177ffa26773e4b14")
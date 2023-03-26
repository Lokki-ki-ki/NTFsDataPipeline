import requests
import pandas as pd


def fetch_transactions_for_collection():
        """
        INPUT: number of transactions to fetch, and collection address
        OUTPUT: dataframe of transactions
        """
        # ti = kwargs['ti']
        apiKey = "Fr-VZGooptPuK1cZslqGIIFBcYXnyEZG"
        network = "eth-mainnet"
        headers = {"accept": "application/json"}
        num_of_trans = 100
        collection = "0x793f969bc50a848efd57e5ad177ffa26773e4b14"
        url_trans = f"https://{network}.g.alchemy.com/nft/v2/{apiKey}/getNFTSales?fromBlock=0&toBlock=latest&order=asc&contractAddress={collection}&limit={num_of_trans}"
        response = requests.get(url_trans, headers=headers).json()
        trans = response["nftSales"]
        df = pd.DataFrame()
        for tran in trans:
            dic = {}
            dic["collection_address"] = collection
            dic["marketplace"] = tran["marketplace"]
            dic["token_id"] = tran["tokenId"]
            dic["seller"] = tran["sellerAddress"]
            dic["buyer"] = tran["buyerAddress"]
            dic["price"] = tran["sellerFee"]["amount"] if "amount" in tran["sellerFee"] else 0
            dic["price_decimal"] = tran["sellerFee"]["decimals"] if "decimals" in tran["sellerFee"] else None
            dic["price_currency"] = tran["sellerFee"]["symbol"] if "symbol" in tran["sellerFee"] else None
            dic["protocol_fee"] = tran["protocolFee"]["amount"] if "amount" in tran["protocolFee"] else 0
            dic["protocol_fee_decimal"] = tran["protocolFee"]["decimals"] if "decimals" in tran["protocolFee"] else None
            dic["protocol_fee_currency"] = tran["protocolFee"]["symbol"] if "symbol" in tran["protocolFee"] else None
            dic["taker"] = tran["taker"]
            df = pd.concat([df, pd.DataFrame(dic, index=[0])])
        # TODO: save to csv just for reference -> shld save to db
        df.to_csv("/tmp/fetch_transactions_for_collection.csv", index=False)
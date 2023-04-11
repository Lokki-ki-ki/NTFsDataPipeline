#Fetches top selling NFTs in the ethereum and polygon blockchain over a period
import requests
import pandas as pd

class FetchTopSellingNFTs:

    def __init__(self):
        self.url = "https://api.nftport.xyz/v0/contracts/top"
        self.headers = {
            "accept": "application/json",
            "Authorization": "cbcd347f-f20a-415d-ac79-f38932ddcfa3" #API Key
        }


    def fetch_nfts(self, period):
        params = {
            "period": period,
            "page_size": 50,
            "page_number": 100,
            "order_by": "volume",
            "chain": ["ethereum", "polygon"]
        }
        response = requests.get(self.url, headers=self.headers, params=params)
        data = response.json()
        nfts = data["contracts"]
        df = pd.DataFrame()
        rank = 1

        for nft in nfts:
            dic = {}
            dic["chain"] = nft["chain"]
            dic["contract_address"] = nft["contract_address"]
            dic["name"] = nft["name"]
            dic["description"] = nft["metadata"]["description"]
            dic["rank"] = rank
            df = pd.concat([df, pd.DataFrame(dic, index=[rank-1])])
            rank += 1
        

        path = "/tmp/fetch_nfts_top_daily.csv"
        results = df
        results.to_csv(path, index=False)
        return path


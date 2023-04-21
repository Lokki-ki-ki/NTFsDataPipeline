# NFTsDataPipeline

## Background
NFTs, or non-fungible tokens, are a hot topic in the world of cryptocurrencies and digital assets. NFTs are distinct digital assets that are kept on a blockchain network, are not divisible, and cannot be copied. NFTs are important to collectors, investors, and artists because to their distinct qualities. Digital artwork, collectibles, music, films, and other digital assets are now represented by NFTs. As a result, millions of dollars have been spent purchasing and selling NFTs on various online marketplaces over the past few years, fueling the NFT market's exponential growth.

## Motivation
Tools that can assist buyers, sellers, and investors in navigating the complicated world of NFTs are needed while the NFT market is still in its infancy. In order to track NFT sales, trends, and other crucial information that can assist users in making wise decisions, an NFT Dashboard project might be a useful tool. Users can examine and study NFT data from multiple marketplaces and collections using the project's user-friendly interface. To help consumers stay current on their NFT investments, the dashboard can also incorporate features like alerts, notifications, and portfolio tracking.

In general, an NFT Dashboard can enable consumers easily traverse the complicated world of NFTs and offer a valuable service to the expanding NFT market.

## Pipeline Design
![alt text](https://github.com/Lokki-ki-ki/NTFsDataPipeline/blob/main/nft-dashboard/public/IS3107_pipeline_design.png)

## Stack
1. Alchemy NFT API
2. Yahoo Finance API
3. NFTPort API
4. Google Cloud Storage
5. Google Cloud BigQuery
6. Google Cloud Pub/Sub
7. MatPlotLib
8. TensorFlow
9. Flask
10. Pandas


### Tips for running our dags
### Step1 - Pls clone this repo under your airflow/dags/, then the airflow would be able to detect this folder

### Step2 - Before run the dag, please auth your google cloud. 
If don't have gcloud cli, please install and follwoing Local development environmen in the link https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to. 
(For our grp member)Once initiate gcloud successfully, can use `gcloud projects list` and `gcloud config set project nft-dashboard-381202` to set the proj to our current proj.
(For non-admin)use `export GOOGLE_APPLICATION_CREDENTIALS='nft-dashboard-381202-0f0a9018c1a0.json'` in gcloud cli to use our service account.

### Step3 - Run the dags test
`airflow dags test project_initialize` and `airflow dags test nfts_price_etl_hourly` currently the dag_id is airlflow_draft can you can check the data under gcs/nfts_pipeline_test and bigquery/nfts_pipeline

### Other Tips
More details of the Project can be found here: 
https://docs.google.com/document/d/14X3D21B-_jq4ibKd1-gIIURfoAJetyzs48kOGLADYfY/edit?usp=sharing


 

# NFTsDataPipeline


### Tips for running our dags
### Step1 - Pls clone this repo under your airflow/dags/, then the airflow would be able to detect this folder

### Step2 - Before run the dag, please auth your google cloud. 
If don't have gcloud cli, please install and follwoing Local development environmen in the link https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to. Once initiate gcloud successfully, can use `gcloud projects list` and `gcloud config set project nft-dashboard-381202` to set the proj to our current proj.

### Step3 - Run the dags test
`airflow dags test nfts_price_etl_hourly` and `airflow dags test nfts_price_etl_daily` currently the dag_id is airlflow_draft can you can check the data under gcs/nfts_pipeline_test and bigquery/nfts_pipeline

### Other Tips
Design of dags pls check https://docs.google.com/document/d/1Cg8LuNmWu1hdsvHbumb2-7xJl61iMut_jWvDCJ_Q3gc/edit
Things to be improved has been commented as #TODO: which can be detected by vscode extension TODO Tree


 

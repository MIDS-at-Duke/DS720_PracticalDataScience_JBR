import dask.dataframe as dd
from dask.distributed import LocalCluster, Client

# Step 1: Set up a Dask cluster
cluster = LocalCluster()
client = Client(cluster)
client

# Step 2: Define the data file path and data types
file_path = "C:/Users/wonny/Downloads/arcos_all_washpost/arcos_all_washpost.tsv"
dtype_mapping = {
    "BUYER_STATE": "object",
    "BUYER_COUNTY": "object",
    "DRUG_NAME": "object",
    "TRANSACTION_DATE": "object",
    "MME_Conversion_Factor": "float",
    "REPORTER_DEA_NO": "object",
    "NDC_NO": "object",
    "REPORTER_ADDL_CO_INFO": "object",
}

# Step 3: Read data into a Dask DataFrame
opioid_df = dd.read_csv(file_path, sep="\t", dtype=dtype_mapping)

# Step 4: Define data processing tasks
opioid_df["year_month"] = opioid_df["TRANSACTION_DATE"].str.slice(0, 7)

opioid_df_grouped = (
    opioid_df.groupby(["BUYER_STATE", "BUYER_COUNTY", "DRUG_NAME", "year_month"])
    .agg({"MME_Conversion_Factor": "sum", "REPORTER_DEA_NO": "count"})
    .reset_index()
    .rename(
        columns={
            "MME_Conversion_Factor": "total_morphine_mg",
            "REPORTER_DEA_NO": "total_transactions",
        }
    )
)

# Step 5: Execute the tasks
result = client.compute(opioid_df_grouped)
result = client.gather(result)

# Step 6: Close the cluster
cluster.close()

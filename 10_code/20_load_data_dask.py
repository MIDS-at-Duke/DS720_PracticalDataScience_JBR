import pandas as pd
import dask.dataframe as dd


file_path = "../../arcos_all_washpost.tsv"
arcos_all_washpost = pd.read_csv(file_path, sep="\t", nrows=100)
arcos_all_washpost.head()

arcos_all_washpost_dask = dd.read_csv(
    file_path,
    sep="\t",
    dtype={
        "REPORTER_DEA_NO": "str",
        "REPORTER_BUS_ACT": "str",
        "REPORTER_NAME": "str",
        "REPORTER_ADDL_CO_INFO": "str",
        "REPORTER_ADDRESS1": "str",
        "REPORTER_ADDRESS2": "str",
        "REPORTER_CITY": "str",
        "REPORTER_STATE": "str",
        "REPORTER_ZIP": "str",
        "REPORTER_COUNTY": "str",
        "BUYER_DEA_NO": "str",
        "BUYER_BUS_ACT": "str",
        "BUYER_NAME": "str",
        "BUYER_ADDL_CO_INFO": "str",
        "BUYER_ADDRESS1": "str",
        "BUYER_ADDRESS2": "str",
        "BUYER_CITY": "str",
        "BUYER_STATE": "str",
        "BUYER_ZIP": "str",
        "BUYER_COUNTY": "str",
        "TRANSACTION_CODE": "str",
        "DRUG_CODE": "str",
        "NDC_NO": "str",
        "DRUG_NAME": "str",
        "Measure": "str",
        "MME_Conversion_Factor": "float64",
        "Dosage_Strength": "str",
        "TRANSACTION_DATE": "str",
        "Combined_Labeler_Name": "str",
        "Reporter_family": "str",
        "CALC_BASE_WT_IN_GM": "float64",
        "DOSAGE_UNIT": "str",
        "MME": "float64",
    },
)

arcos_all_washpost_dask = arcos_all_washpost_dask[
    [
        "REPORTER_DEA_NO",
        "REPORTER_NAME",
        "BUYER_STATE",
        "BUYER_COUNTY",
        "MME_Conversion_Factor",
        "CALC_BASE_WT_IN_GM",
        "TRANSACTION_DATE",
        "MME",
    ]
]


arcos_all_washpost_dask["Total_Shipments"] = (
    arcos_all_washpost_dask["MME_Conversion_Factor"]
    * arcos_all_washpost_dask["CALC_BASE_WT_IN_GM"]
)

arcos_all_washpost_dask["year_month"] = arcos_all_washpost_dask["TRANSACTION_DATE"].str[
    0:7
]

groupped_arcos_all_washpost_dask = (
    arcos_all_washpost_dask.groupby(["year_month", "BUYER_STATE", "BUYER_COUNTY"])[
        "Total_Shipments", "MME"
    ]
    .sum()
    .compute()
)

groupped_arcos_all_washpost_dask = groupped_arcos_all_washpost_dask.reset_index()

groupped_arcos_all_washpost_dask.to_parquet(
    "../20_intermediate_files/arcos_all_washpost_collapsed.parquet"
)

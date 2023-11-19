import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the file path
file_path = "https://gfx-data.news-engineering.aws.wapo.pub/ne-static/arcos/v2/bulk/arcos_all_washpost.zip"  # Change to your file path

# Read the data
df = spark.read.csv(file_path, sep="\t", header=True, compression="zip")

# List of states you want to filter
# states_to_filter = ["FL", "TX", "WA"]

# Filter the DataFrame
# filtered_df = df.filter(df["BUYER_STATE"].isin(states_to_filter))

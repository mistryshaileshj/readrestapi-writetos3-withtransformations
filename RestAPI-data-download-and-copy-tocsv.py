import requests
import pandas as pd
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import col, trim

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

def main():
    # API configuration
    base_url = "https://www.anapioficeandfire.com/api/characters"
    headers = {"User-Agent": "aws-glue-script/1.0"}
    page = 1
    page_size = 50
    max_records = 1000  # Customize this as needed
    all_characters = []

    try:
        print("🚀 Starting API fetch...")
        
        while True:
            params = {"page": page, "pageSize": page_size}
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
    
            characters = response.json()
            if not characters:
                print("✅ No more characters to fetch.")
                break
    
            all_characters.extend(characters)
            print(f"📦 Fetched {len(characters)} characters on page {page}. Total: {len(all_characters)}")
    
            if len(all_characters) >= max_records:
                print("🔚 Reached max record limit.")
                break
    
            page += 1
    
        # Convert to Pandas DataFrame
        df_pandas = pd.DataFrame(all_characters)
        print(f"🧾 Total characters collected: {df_pandas.shape[0]} rows")
    
        # Optional: select some useful columns
        if not df_pandas.empty:
            df_pandas = df_pandas[["name", "gender", "culture", "born", "died", "titles", "aliases", "playedBy", "tvSeries"]]
    
        # Convert to Spark DataFrame
        df_spark = spark.createDataFrame(df_pandas)
        print(f"🔎 Spark row count: {df_spark.count()}")
    
        # Show some data
        # df_spark.show(20, truncate=False)
    
        # Optional: Write to S3
        output_path = "s3://vertica-data/iceandfire/characters/"
        df_spark.write.mode("overwrite").parquet(output_path)
        
        # Read the parquet files
        parquet_df = spark.read.parquet("s3://vertica-data/iceandfire/characters/")
        
        print(f"✅ Rows in input Parquet: {parquet_df.count()}")
        
        # Optional: coalesce into a single partition (i.e., one output file)
        # Convert array columns to comma-separated strings
        flat_df = parquet_df \
            .withColumn("titles", concat_ws(", ", "titles")) \
            .withColumn("aliases", concat_ws(", ", "aliases")) \
            .withColumn("playedBy", concat_ws(", ", "playedBy")) \
            .withColumn("tvSeries", concat_ws(", ", "tvSeries"))
            
        single_file_df = flat_df.coalesce(1)
        
        # Filter out rows where name is null or blank
        filtered_df = single_file_df.filter((col("name").isNotNull()) & (trim(col("name")) != ""))
        
        # Write as a single CSV file (no header can also be added)
        filtered_df.write.mode("overwrite").option("header", "true").csv("s3://vertica-data/iceandfire/characters_csv/")
    
    except Exception as e:
        print(f"❌ Error during API fetch: {e}")

if __name__ == '__main__':
    main()

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

if __name__ == '__main__':
    parser = ArgumentParser(description="qlog parquet sampler")
    parser.add_argument("-i",  "--input", required=True, help="@Required input folder")
    parser.add_argument("-o",  "--output", required=True, help="@Required output folder")
    args, _ = parser.parse_known_args()

    try:
        import configparser
    except ImportError:
        import ConfigParser as configparser
    
    config = configparser.ConfigParser()
    config.read("../config/sampling.properties")
    properties = dict(config.items("properties"))
    app_name = properties.get("application_name")
    sample_fraction = float(properties.get("sample_fraction"))
    reducers = int(properties.get("reducers"))

    spark = SparkSession.builder.appName(app_name).master('local').getOrCreate()

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    # read parquet file
    df = sqlContext.read.parquet(args.input)
    
    # sample & write parquet file
    df.sample(False, sample_fraction, 9876).coalesce(reducers).write.parquet(args.output)

from extract import extract
from transform import transform
from load import load

def run():
    spark, artists_df = extract()
    datacleaner = transform(spark, artists_df)
    load(datacleaner)

if __name__=="__main__":
    run()
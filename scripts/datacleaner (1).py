def data_cleaner():


   from pyspark.sql import SparkSession
                 
def create_session ():

  spk = SparkSession.builder \
              .master("local") \
              .appName("exam_spark") \
              .getOrCreate()
  return spk
         


def read_data(Path):


  Path="/home/redwane/airflow/data/maires-candidats.csv"
  df = spk.read.csv(Path,header=True,inferSchema=True)
         
  return df
     
def delete_missing_values(df):
    df1 = df.dropna(how="any")
         
    return df1

    

    
    df1.to_csv('~/home/redwane/airflow/data/cleaned_maires-candidats.csv', index=False)

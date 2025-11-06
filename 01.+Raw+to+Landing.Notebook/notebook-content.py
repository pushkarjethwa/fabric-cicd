# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ### 01. Raw to Landing

# PARAMETERS CELL ********************

today_file = 'file' #'LMS_09-02-2023.csv'
processed_Date = '9999-99-99'#'2024-09-18'
source_account = 'datalake11212' # fill in your primary account name 
source_container = 'fabricproject' # fill in your container name 
src_relative_path = 'raw' # fill in your relative folder path 
destination_account = 'datalake11212'
destination_container = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (source_container, source_account, src_relative_path) 
print('Source storage account path is ', adls_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reading csv file of Today

# CELL ********************

latest_path = f"{adls_path}/{today_file}"
print(latest_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit
latest_path = f"{adls_path}/{today_file}"
df = spark.read.csv(path= latest_path,header=True,inferSchema=True)

if df.count() > 1:
    print("The file has data.")
    
    df_new = df.withColumn("Processing_Date",lit(processed_Date))
    df_new.write.format('csv').option('header','true').partitionBy('Processing_Date').mode('append').save(f'abfss://{destination_container}@{destination_account}.dfs.core.windows.net/landing/')
    print("Data written to landing zone successfully !")

else:
    print('This file contains only header row and no data.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

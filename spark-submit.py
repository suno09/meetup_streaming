import os

os.system("powershell.exe $env:PYSPARK_PYTHON='python'; "
          "spark-submit --packages "
          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
          "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 "
          "stream_processing_app.py")

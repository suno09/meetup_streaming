[comment]: <> (learn more: https://www.mitrais.com/news-updates/a-guide-to-setup-a-kafka-environment/)
[comment]: <> (start zookeeper)
[comment]: <> (.\kafka_2.13-2.6.0\bin\windows\zookeeper-server-start.bat .\kafka_2.13-2.6.0\config\zookeeper.properties<br/>)
[comment]: <> (start kafka)
[comment]: <> (.\kafka_2.13-2.6.0\bin\windows\kafka-server-start.bat .\kafka_2.13-2.6.0\config\server.properties)
[comment]: <> (https://www.youtube.com/playlist?list=PLe1T0uBrDrfPKzCDz7p_bEDWW9PwilwrE)
# Meetup RSVP Stream Processing

>Reading data from [Meetup RSVP](https://stream.meetup.com/2/rsvps) and transform to MySQL and MongoDB database using Kafka, Spark and Python 3<br>The original source code is from [DataMaking Youtube](https://www.youtube.com/playlist?list=PLe1T0uBrDrfPKzCDz7p_bEDWW9PwilwrE)

Please open an issue if you have any questions or comments at all !

## Installation
Before enter to the code, make sure install [Zookeeper](https://zookeeper.apache.org/releases.html#download), [Kafka](https://kafka.apache.org/downloads) and [Spark](https://spark.apache.org/downloads.html) on your system

Use the package manager pip to install the necessary packeges.
```shell
pip install -r requirements.txt
```
Make sure configuring MySQL and MongoDB on config.properties

After install the packages, run to start the following commands

Read data from [Meetup RSVP](https://stream.meetup.com/2/rsvps) and add to kafka topic
```shell
python kafka_producer_demo.py 
```

Mapping data from kafka topic to MySQL and MongoDB database
```shell
python stream_processing_app.py 
```

Plot MySQL data in real time
```shell
python stream_plot.py 
```

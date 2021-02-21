# start hdfs
/Users/pedromorfeu/hadoop/hadoop-2.10.1/start.sh

# start spark


# start airflow
/Users/pedromorfeu/airflow/start.sh
/Users/pedromorfeu/airflow/start-scheduler.sh

/Users/pedromorfeu/airflow/run-spark.sh

# kafka
/Users/pedromorfeu/kafka/kafka_2.12-2.7.0/start-zookeeper.sh
/Users/pedromorfeu/kafka/kafka_2.12-2.7.0/start-kafka.sh

/Users/pedromorfeu/kafka/kafka_2.12-2.7.0/read-pipeline-ok.sh
/Users/pedromorfeu/kafka/kafka_2.12-2.7.0/read-pipeline-ko.sh

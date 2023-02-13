# eventhub_spark_long_processing_test
Test case to expose TCP dead connections issue with EventHub and Spark Streaming[Kafka]

To run this test, start with a fresh virtual environment and install the requirements:
```shell
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

You will also need to provide a few arguments at runtime:

```shell
long_processing_kafka.py --eh_namespace <your EH namespace>.servicebus.windows.net --eh_name <EH name> --eh_sas_name <EH SAS name> --eh_sas_key <EH SAS key> --checkpoint_path <HDFS path>
```
where `<HDFS path>` is a path that will be use by Spark as a checkpoint, and it **will be deleted once the program starts**.

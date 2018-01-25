# zabbix-kafka-consumers-monitor
Monitor Kafka Consumer groups in Zabbix using Python

Information about Kafka Consumers groups and consumers LAG are retrieved using Kafka API. ( [Kafka API Documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol) 
)

This monitoring tool is working for Kafka Broker version > 0.9.0 and consumers that are using Consumer API which are committing the offset into Kafka.

### Usage
Install kafka-python
```bash
pip install kafka-python
```

Zabbix Consumer groups discovery
```bash
python monitor_consumers.py --bootstrap-server <kafka_broker_list> --list 
```

Check if a consumer group exists
```bash
python monitor_consumers.py --bootstrap-server <kafka-broker-list> --group <consumer-group> --is-present
```

Get Lag for a specific consumer group
```bash
python monitor_consumers.py --bootstrap-server <kafka-broker-list> --group <consumer-group> --lag
```

Get number of consumers from a group
```bash
python monitor_consumers.py --bootstrap-server <kafka-broker-list> --group <consumer-group> --members
```

### All possible arguments
```
Monitor Kafka Consumer Groups

optional arguments:
  -h, --help            show this help message and exit
  --bootstrap-server BOOTSTRAP_SERVER
                        Kafka brokers list
  --list                List Kafka Consumer Groups
  --group               CONSUMER_GROUP
      --lag                 Lag consumer group
      --members             Members consumer group
      --is-present          Check if a consumer group exists
  --timeout TIMEOUT     Kafka API timeout in ms
```
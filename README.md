# zabbix-kafka-consumers-monitor
Monitor Kafka Consumer groups in Zabbix using Python 

### Usage
Install kafka-python
```bash
pip install kafka-python
```

Zabbix Consumer groups discovery
```bash
python monitor_consumers.py --bootstrap-server <kafka_broker_list> --list 
```

Get Lag for a specific consumer group
```bash
python monitor_consumers.py --bootstrap-server <kafka-broker-list> --lag --group <consumer-group> 
```

Get number of consumers from a group
```bash
python monitor_consumers.py --bootstrap-server <kafka-broker-list> --members --group <consumer-group> 
```

### All possible arguments
```
Monitor Kafka Consumer Groups

optional arguments:
  -h, --help            show this help message and exit
  --bootstrap-server BOOTSTRAP_SERVER
                        Kafka brokers list
  --list                List Kafka Consumer Groups
  --lag                 Lag consumer group
  --members             Members consumer group
  --group CONSUMER_GROUP
                        Consumer group name
  --timeout TIMEOUT     Kafka API timeout in ms
  --check_without_members CHECK_WITHOUT_MEMBERS
                        Describe consumer groups without members.
```
import logging
import argparse
import json
from kafka_consumer_groups import KafkaConsumerGroups

# Setup Logger
logger = logging.getLogger(__name__)
consoleHandler = logging.StreamHandler()
logFormatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
logger.setLevel(logging.INFO)

parser = argparse.ArgumentParser(
    description="Monitor Kafka Consumer Groups",
    formatter_class=argparse.RawTextHelpFormatter
)

parser.add_argument('--bootstrap-server', required=True,
                    help='Kafka brokers list')
parser.add_argument('--list', dest='list', action='store_true', required=False, default=False,
                    help='List Kafka Consumer Groups')
parser.add_argument('--group', required=False, dest="consumer_group", default=None,
                    help='Consumer group name')
parser.add_argument('--lag', dest='lag', action='store_true', required=False, default=False,
                    help='Lag consumer group')
parser.add_argument('--members', dest='members', action='store_true', required=False, default=False,
                    help='Members consumer group')
parser.add_argument('--is-present', dest='is_present', action='store_true', required=False, default=False,
                    help='Check if a consumer group exists')
parser.add_argument('--timeout', required=False, dest="timeout", default=5000,
                    help='Kafka API timeout in ms')

args = parser.parse_args()

bootstrap_server = args.bootstrap_server
timeout = args.timeout
list_groups = args.list
group = args.consumer_group
lag = args.lag
members = args.members
is_present = args.is_present

if list_groups is True:
    cg = KafkaConsumerGroups(bootstrap_server, timeout)
    l = cg.list()
    zabbix_items = []
    for key, value in l.items():
        zabbix_items.append({"{#CONSUMER_GROUP}": key})

    print(json.dumps({"data": zabbix_items}, indent=4))
elif group is not None:
    if lag is True:
        cg = KafkaConsumerGroups(bootstrap_server, timeout)
        l = cg.list()
        g = cg.describe(node_id=l[group], group_name=group)
        print(g["lag"])
    elif members is True:
        cg = KafkaConsumerGroups(bootstrap_server, timeout)
        l = cg.list()
        members = cg.get_members(node_id=l[group], group_name=group)
        print(len(members))
    elif is_present is True:
        cg = KafkaConsumerGroups(bootstrap_server, timeout)
        l = cg.list()
        if group in l:
            print("1")
        else:
            print("0")
    else:
        print("One of the options, --lag or --members are mandatory when a group was passed. Please see help.")
        exit(1)
else:
    print("One of the options, --list or --describe are mandatory. Please see help.")
    exit(1)

exit(0)

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
parser.add_argument('--lag', dest='lag', action='store_true', required=False, default=False,
                    help='Lag consumer group')
parser.add_argument('--members', dest='members', action='store_true', required=False, default=False,
                    help='Members consumer group')
parser.add_argument('--group', required=False, dest="consumer_group", default=None,
                    help='Consumer group name')
parser.add_argument('--timeout', required=False, dest="timeout", default=5000,
                    help='Kafka API timeout in ms')
parser.add_argument('--check_without_members', required=False, dest="check_without_members", default=False,
                    help='Describe consumer groups without members.')

args = parser.parse_args()

bootstrap_server = args.bootstrap_server
timeout = args.timeout
list_groups = args.list
lag = args.lag
members = args.members
group = args.consumer_group
check_without_members = args.check_without_members

if list_groups is True:
    cg = KafkaConsumerGroups(bootstrap_server, timeout)
    l = cg.list()
    zabbix_items = []
    for key, value in l.items():
        zabbix_items.append({"{#CONSUMER_GROUP}": key})

    print(json.dumps({"data": zabbix_items}, indent=4))
elif lag is True and group is not None:
    cg = KafkaConsumerGroups(bootstrap_server, timeout)
    l = cg.list()
    g = cg.describe(node_id=l[group], group_name=group, check_without_members=check_without_members)
    print(g["lag"])
elif members is True and group is not None:
    cg = KafkaConsumerGroups(bootstrap_server, timeout)
    l = cg.list()
    g = cg.describe(node_id=l[group], group_name=group, check_without_members=check_without_members)
    print(len(g["members"]))
else:
    print("One of the options list or describe are mandatory. Please see help.")
    exit(1)

exit(0)

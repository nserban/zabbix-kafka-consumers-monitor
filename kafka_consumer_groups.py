from kafka import KafkaConsumer, TopicPartition
from kafka.client_async import KafkaClient
from kafka.protocol import admin
import logging
import time

from kafka.protocol.group import MemberAssignment

logger = logging.getLogger(__name__)

'''
This class retrieve all consumer groups and information about members and lag using Kafka API.
Documentation about Kafka API
https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol
'''


class KafkaConsumerGroups:
    kafka_brokers = None
    client = None
    timeout = None

    def __init__(self, kafka_brokers, timeout=5000):
        self.kafka_brokers = kafka_brokers
        self.client = KafkaClient(bootstrap_servers=kafka_brokers)
        self.timeout = timeout

    def list(self):
        list_groups_request = admin.ListGroupsRequest_v0(
            timeout=self.timeout
        )
        kafka_nodes = [broker.nodeId for broker in self.client.cluster.brokers()]
        consumers_grp = {}
        for node_id in kafka_nodes:
            current_of_tries = 0
            max_of_tries = 5
            data_from_node = False

            while not data_from_node and current_of_tries <= max_of_tries:
                future = self.client.send(node_id, list_groups_request)
                self.client.poll(timeout_ms=self.timeout, future=future)
                if future.value is not None:
                    result = future.value.groups
                    for i in result:
                        consumers_grp.update({i[0]: node_id})
                    data_from_node = True
                else:
                    current_of_tries += 1
                    time.sleep(0.5)

        return consumers_grp

    def describe(self, node_id, group_name, check_lag=False):
        describe_groups_request = admin.DescribeGroupsRequest_v0(
            groups=[(group_name)]
        )
        future = self.client.send(node_id, describe_groups_request)
        self.client.poll(timeout_ms=self.timeout, future=future)

        (error_code, group_id, state, protocol_type, protocol, members) = future.value.groups[0]

        if error_code != 0:
            logger.error(
                "Kafka API - RET admin.DescribeGroupsRequest, error_code={}, group_id={}, state={}, protocol_type={}, protocol={}, members_count={}".format(
                    error_code, group_id, state, protocol_type, protocol, len(members)))
            exit(1)

        metadata_consumer_group = {'id': group_name, 'state': state, 'topics': [], 'lag': 0, 'members': []}

        if not check_lag:
            metadata_consumer_group = len(members)
            return metadata_consumer_group
            
        if len(members) != 0:
            for member in members:
                (member_id, client_id, client_host, member_metadata, member_assignment) = member
                member_topics_assignment = []
                for (topic, partitions) in MemberAssignment.decode(member_assignment).assignment:
                    member_topics_assignment.append(topic)

                metadata_consumer_group['members'].append(
                    {'member_id': member_id, 'client_id': client_id, 'client_host': client_host,
                     'topic': member_topics_assignment})

                metadata_consumer_group['topics'] += member_topics_assignment
                (lag_total, topics_found) = self.get_lag(group_name,
                                                         topics=metadata_consumer_group['topics'])
                metadata_consumer_group['lag'] = metadata_consumer_group['lag'] + lag_total

        return metadata_consumer_group

    def get_lag(self, group_name, topics):
        lag_total = 0
        topics_found = []
        for topic in topics:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_brokers,
                group_id=group_name,
            )
            partitions_per_topic = consumer.partitions_for_topic(topic)

            for partition in partitions_per_topic:
                tp = TopicPartition(topic, partition)
                consumer.assign([tp])
                committed = consumer.committed(tp)
                consumer.seek_to_end(tp)
                last_offset = consumer.position(tp)
                if committed is not None and int(committed) and last_offset is not None and int(last_offset):
                    lag_total += (last_offset - committed)
                    topics_found.append(topic)

            consumer.close(autocommit=False)
            topics_found = list(set(topics_found))
        return lag_total, topics_found

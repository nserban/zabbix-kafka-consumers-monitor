from kafka import KafkaConsumer, TopicPartition
from kafka.client_async import KafkaClient
from kafka.protocol import admin
import time
import threading
import ssl
from kafka.protocol.group import MemberAssignment
import logging
logging.basicConfig(level=logging.DEBUG)

'''
This class retrieve all consumer groups and information about members and lag using Kafka API.
Documentation about Kafka API
https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol
'''


class KafkaConsumerGroups:
    kafka_brokers = None
    client = None
    timeout = None
    security_protocol = None
    sasl_mechanism = None
    sasl_plain_username = None
    sasl_plain_password = None
    ssl_certfile = None
    ssl_keyfile = None
    ssl_context = None

    def __init__(self, kafka_brokers, security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password, ssl_context, timeout=5000):
        self.kafka_brokers = kafka_brokers
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.ssl_context = ssl_context
        self.timeout = timeout
        self.client = KafkaClient(bootstrap_servers=kafka_brokers, security_protocol=security_protocol, sasl_mechanism=sasl_mechanism, sasl_plain_username=sasl_plain_username, sasl_plain_password=sasl_plain_password, ssl_context=ssl_context, timeout=timeout)
        self.lag_topics_found = []
        self.lag_total = 0

    def list(self):
        list_groups_request = admin.ListGroupsRequest_v0(
            timeout=self.timeout
        )
        kafka_broker_ids = [broker.nodeId for broker in self.client.cluster.brokers()]
        consumers_grp = {}
        for broker_id in kafka_broker_ids:
            current_of_tries = 0
            max_of_tries = 5
            data_from_node = False

            while not data_from_node and current_of_tries <= max_of_tries:
                future = self.client.send(broker_id, list_groups_request)
                self.client.poll(timeout_ms=self.timeout, future=future)
                if future.value is not None:
                    result = future.value.groups
                    for i in result:
                        consumers_grp.update({i[0]: broker_id})
                    data_from_node = True
                else:
                    current_of_tries += 1
                    time.sleep(0.5)

        return consumers_grp

    def get_members(self, node_id, group_name):
        describe_groups_request = admin.DescribeGroupsRequest_v0(
            groups=[(group_name)]
        )
        future = self.client.send(node_id, describe_groups_request)
        self.client.poll(timeout_ms=self.timeout, future=future)

        (error_code, group_id, state, protocol_type, protocol, members) = future.value.groups[0]

        if error_code != 0:
            print(
                "Kafka API - RET admin.DescribeGroupsRequest, error_code={}, group_id={}, state={}, protocol_type={}, protocol={}, members_count={}".format(
                    error_code, group_id, state, protocol_type, protocol, len(members)))
            exit(1)

        lmembers=[]
        for member in members:
            (member_id, client_id, client_host, member_metadata, member_assignment) = member
            lmembers.append({'member_id': member_id, 'client_id': client_id, 'client_host': client_host})

        return lmembers

    def describe(self, node_id, group_name):
        describe_groups_request = admin.DescribeGroupsRequest_v0(
            groups=[(group_name)]
        )
        future = self.client.send(node_id, describe_groups_request)
        self.client.poll(timeout_ms=self.timeout, future=future)

        (error_code, group_id, state, protocol_type, protocol, members) = future.value.groups[0]

        if error_code != 0:
            print(
                "Kafka API - RET admin.DescribeGroupsRequest, error_code={}, group_id={}, state={}, protocol_type={}, protocol={}, members_count={}".format(
                    error_code, group_id, state, protocol_type, protocol, len(members)))
            exit(1)

        metadata_consumer_group = {'id': group_name, 'state': state, 'topics': [], 'lag': 0, 'members': []}

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
                (lag_total, topics_found) = self.get_lag_by_topic_list(group_name,
                                                                       topics=metadata_consumer_group['topics'])
                metadata_consumer_group['lag'] = metadata_consumer_group['lag'] + lag_total
        else:
            all_topics = self.client.cluster.topics()

            while '__consumer_offsets' in all_topics: all_topics.remove('__consumer_offsets')
            (lag_total, topics_found) = self.get_lag_by_topic_list(group_name, topics=all_topics)

            metadata_consumer_group['lag'] = metadata_consumer_group['lag'] + lag_total
            metadata_consumer_group['topics'] = topics_found

        return metadata_consumer_group

    def get_lag_by_topic_list(self, group_name, topics):
        self.lag_topics_found = []
        self.lag_total = 0

        topics = list(topics)
        no_threads = 16

        batches = [topics[i:i + no_threads] for i in range(0, len(topics), no_threads)]
        for batch_topics in batches:
            threads = []
            for topic in batch_topics:
                t = threading.Thread(target=self.get_lag_by_topic, args=(group_name, topic,))
                threads.append(t)
                t.start()

            for t in threads:
                if t.isAlive():
                    t.join()

        return self.lag_total, list(set(self.lag_topics_found))

    def get_lag_by_topic(self, group_name, topic):
        consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_brokers,
            group_id=group_name,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            ssl_context=self.ssl_context
        )
        partitions_per_topic = consumer.partitions_for_topic(topic)

        for partition in partitions_per_topic:
            tp = TopicPartition(topic, partition)
            consumer.assign([tp])
            committed = consumer.committed(tp)
            consumer.seek_to_end(tp)
            last_offset = consumer.position(tp)
            if committed is not None and int(committed) and last_offset is not None and int(last_offset):
                self.lag_topics_found.append(topic)
                self.lag_total += (last_offset - committed)

        consumer.close(autocommit=False)
        return

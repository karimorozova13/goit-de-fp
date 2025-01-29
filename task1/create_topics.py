from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

my_name = "kari"

athlete_event_results_topic_name = f'{my_name}_athlete_event_results'
athlete_summary_topic_name = f'{my_name}_athlete_summary'

num_partitions = 2
replication_factor = 1


athlete_event_results_topic = NewTopic(name=athlete_event_results_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
athlete_summary_topic = NewTopic(name=athlete_summary_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)


try:
    admin_client.create_topics(new_topics=[athlete_event_results_topic, athlete_summary_topic], validate_only=False)
    print(f"Topic '{athlete_event_results_topic_name}' created successfully.")
    print(f"Topic '{athlete_summary_topic_name}' created successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

print(topic for topic in admin_client.list_topics() if "my_name" in topic)

admin_client.close()
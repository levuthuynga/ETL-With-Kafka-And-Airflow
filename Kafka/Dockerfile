# Start from the base Confluent Kafka Connect image
FROM confluentinc/cp-kafka-connect:7.4.0

# Define a variable for the plugins
ARG PLUGIN_PATH=/usr/share/confluent-hub-components

# Use the confluent-hub tool to install the connectors.
# We use the && operator to ensure each command completes successfully
# before moving on to the next one.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest && \
    confluent-hub install --no-prompt debezium/debezium-connector-mysql:latest && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:latest

# The base image already sets up the entrypoint, so no need to add a CMD here.
# The default entrypoint /etc/confluent/docker/run will now be executed without
# the installation step, making startup much faster and more reliable.
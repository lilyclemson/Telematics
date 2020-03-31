IMPORT $.Types AS Types;
IMPORT Std;
IMPORT kafka;

#OPTION('pickBestEngine', FALSE);

//A client that consumes records from a Kafka cluster.
c := kafka.KafkaConsumer(Types.Topic, brokers := Types.broker);
c.ResetMessageOffsets();

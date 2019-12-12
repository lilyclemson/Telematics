IMPORT $.Types AS Types;
IMPORT Std;
IMPORT kafka;

#OPTION('pickBestEngine', FALSE);

c := kafka.KafkaConsumer(Types.Topic, brokers := Types.broker);
c.ResetMessageOffsets();

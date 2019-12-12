IMPORT $.Types AS Types;
IMPORT $.Utils;

#OPTION('pickBestEngine', FALSE);

/** 
  * Consumer process to retrieve Kafka messages and update to the superFile
  */

KAFKA_MSG_BATCH_SIZE := 500000; // 100000;

#WORKUNIT('name', 'Consumer (batch size ' + (STRING)KAFKA_MSG_BATCH_SIZE + ')');

// Step 1: Consumer retrievel information from broker
raw := Utils.consumer(Types.Topic, Types.broker, KAFKA_MSG_BATCH_SIZE);

// Step 2: Parse the raw Kafka message
parsed := Utils.parseMsg(raw);

// Step 3: clean the parsed messages
cleaned := Utils.clean(parsed) : INDEPENDENT;

// Step 4: update the cleaned messages to superFile
updateSuperfileAction := Utils.maintenance(cleaned, Types.Topic).updateSuperFile;

// Update superFile every second by scheduling CRON job
IF(EXISTS(cleaned), updateSuperfileAction); //  : WHEN ( CRON ( '* * * * *' ) );
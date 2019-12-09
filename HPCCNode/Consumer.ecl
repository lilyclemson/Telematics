IMPORT HPCCNode.Types AS Types;
IMPORT HPCCNode.Utils;

/** 
  * Consumer process to retrieve Kafka messages and update to the superFile
  */

// Step 1: Consumer retrievel information from broker
raw := Utils.consumer(Types.Topic, Types.broker);

// Step 2: Parse the raw Kafka message
parsed := Utils.parseMsg(raw);

// Step 3: clean the parsed messages
cleaned := Utils.clean(parsed);

// Step 4: update the cleaned messages to superFile
cronjob := Utils.maintenance(cleaned, Types.Topic).updateSuperFile;
// Update superFile every second by scheduling CRON job
notEmpty := EXISTS(raw);
waitjob := OUTPUT('wait...');
IF(notEmpty, waitjob, cronjob) : WHEN ( CRON ( '* * * * *' ) );
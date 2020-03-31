IMPORT kafka;
IMPORT Telematics;
IMPORT $.Types AS Types;
IMPORT STD.Date;
IMPORT STD;

EXPORT Utils := MODULE

// This module is an override of the built-in Kafka module that provides better performance for multi-slave
// reads.
SHARED MyKafkaConsumer(VARSTRING topic, VARSTRING brokers = 'localhost', VARSTRING consumerGroup = 'hpcc') := MODULE
    SHARED localKafka := SERVICE : plugin('kafka'), namespace('KafkaPlugin')

        BOOLEAN PublishMessage(CONST VARSTRING brokers, CONST VARSTRING topic, CONST VARSTRING message, CONST VARSTRING key) : cpp,action,context,entrypoint='publishMessage';
        INTEGER4 getTopicPartitionCount(CONST VARSTRING brokers, CONST VARSTRING topic) : cpp,action,context,entrypoint='getTopicPartitionCount';
        STREAMED DATASET(KafkaMessage) GetMessageDataset(CONST VARSTRING brokers, CONST VARSTRING topic, CONST VARSTRING consumerGroup, INTEGER4 partitionNum, INTEGER8 maxRecords) : cpp,action,context,entrypoint='getMessageDataset';
        INTEGER8 SetMessageOffset(CONST VARSTRING brokers, CONST VARSTRING topic, CONST VARSTRING consumerGroup, INTEGER4 partitionNum, INTEGER8 newOffset) : cpp,action,context,entrypoint='setMessageOffset';

    END;

    /**
     * Get the number of partitions currently set up for this topic
     *
     * @return  The number of partitions or zero if either the topic does not
     *          exist or there was an error
     */
    EXPORT INTEGER4 GetTopicPartitionCount() := localKafka.getTopicPartitionCount(brokers, topic);

    /**
     * Consume previously-published messages from the current topic.
     *
     * @param   maxRecords  The maximum number of records to retrieve; pass
     *                      zero to return as many messages as there are
     *                      queued (dangerous); REQUIRED
     *
     * @return  A new dataset containing the retrieved messages
     */
    EXPORT DATASET(kafka.KafkaMessage) GetMessages(INTEGER8 maxRecords) := FUNCTION

        numberOfPartitions := GetTopicPartitionCount();
        maxRecordsPerNode := MAX(maxRecords DIV numberOfPartitions, 1);

        // Container holding messages from all partitions; in a multi-node setup
        // the work will be distributed among the nodes (at least up to the
        // number of partitions); note that 'COUNTER - 1' is actually the
        // Kafka partition number that will be read
        partitionTracker := DATASET
        	(
        		numberOfPartitions,
        		TRANSFORM
        			(
        				{UNSIGNED2 partitionNum},
        				SELF.partitionNum := COUNTER -1
        			),
        		DISTRIBUTED
        	);

        // Map messages from multiple partitions back to final record structure
        resultDS := NORMALIZE
            (
                partitionTracker,
                localKafka.GetMessageDataset(brokers, topic, consumerGroup, LEFT.partitionNum, maxRecordsPerNode),
                TRANSFORM
                    (
                        KafkaMessage,
                        SELF := RIGHT
                    ),
                LOCAL
            ) : INDEPENDENT;

        RETURN resultDS;

    END;

END; // Local Kafka Consumer module

/**
  * consumer function retrieves the messages from Kafka topic.
  * @param topic The Kafka Topic where the messages are retrieve from.
  * @param brokers The IP/IPs of the Kafka brokers.
  * @param batchSize The number of message to consume at once via one consumer function call.
  * @return raw The raw telematics messages in JSON format.
  */
EXPORT consumer(STRING topic, STRING brokers, UNSIGNED4 batchSize=100000) := FUNCTION
  c:= MyKafkaConsumer(topic, brokers := brokers);
  batch := c.GetMessages(batchSize);
  raw := PROJECT(batch, TRANSFORM(Types.KafkaMessageFormat,SELF:= LEFT));
  RETURN raw;
END;

/**
  * parseMsg function parses the raw telematics messages to working dataset.
  * @param raw The raw telematics messages.
  * @return ds The parsed telematcis data.
  */
EXPORT parseMsg(DATASET(Types.KafkaMessageFormat) raw) := FUNCTION
  dsRaw := PROJECT(raw, TRANSFORM(Types.l_journey, 
																SELF.Message_len   := LENGTH(LEFT.message), // min max avg
                                SELF := FROMJSON(Types.l_journey,
                                                  LEFT.message,
                                                  ONFAIL(TRANSFORM(Types.l_journey, SELF:= [])))));
	ds := PROJECT(dsRaw, TRANSFORM(Types.l_journey,
																SELF.WaypointCount := COUNT(LEFT.way_points),
																SELF := LEFT));
																									
	
  // RETURN WHEN(ds, OUTPUT(CHOOSEN(dsRaw, 100), NAMED('dsRaw')));
  RETURN ds;
END;

/**
  * clean function cleans and processed the parsed telematics messages to remove unnecessary information and 
  * add useful info for further analysis.
  * @param raw The raw telematics messages.
  * @return ds The parsed telematcis data.
  */
EXPORT clean(DATASET(Types.l_journey) ds) := FUNCTION
// Step1: Normalize the dataset and reverse it to the original KOLN
Types.l_accel normRaw(Types.l_journey L, Types.l_waypoints R) := TRANSFORM
  SELF := L;
  SELF := R;
END;
normDS := NORMALIZE(ds, LEFT.way_points, normRaw(LEFT, RIGHT), LOCAL);

accelDS := ITERATE
    (
        normDS,
        TRANSFORM
            (
                Types.l_accel,
                SELF.accel := IF(LEFT.vehicle_id = RIGHT.vehicle_id AND LEFT.journey_id = RIGHT.journey_id, RIGHT.speed - LEFT.speed, RIGHT.speed),
                SELF := RIGHT
            ),
        LOCAL
    );

clean0 := TABLE
    (
        accelDS,
        {
            UNSIGNED4 duration := MAX(GROUP, time_offset) - MIN(GROUP, time_offset),
            UNSIGNED4 HB := COUNT(GROUP, accel < -2.90576),
            UNSIGNED4 HA := COUNT(GROUP, accel > 3.79984),
            DECIMAL9_6 highestSpeed := MAX(GROUP, speed),
            UNSIGNED4 speed_cnt := COUNT(GROUP, speed > 35.7632),
            REAL time_sent := MIN(GROUP, time_sent),
            vehicle_id,
            journey_id,
						WaypointCount,
						Message_len
        },
        vehicle_id, journey_id,
        LOCAL, UNSORTED, MANY
    );

clean := PROJECT
    (
        clean0,
        TRANSFORM
            (
                Types.l_clean,
                SELF.speedingratio := LEFT.speed_cnt / LEFT.duration,
                SELF.time_processed := Date.CurrentTimestamp() / 1000000,
                SELF.messagelife := SELF.time_processed - LEFT.time_sent,
                SELF := LEFT
            )
    );
/*		
 sideActions := PARALLEL
     (
         OUTPUT(CHOOSEN(DS, 100), NAMED('DS'));
         OUTPUT(CHOOSEN(normDS, 100), NAMED('normDS'));
         OUTPUT(CHOOSEN(clean0, 100), NAMED('clean0'));
     );*/
// RETURN WHEN(clean, sideActions);
RETURN clean;
END;


/**
  * maintenance module includes the attributes related to the maintenance of telematics data, such as
  * the subFile and the superFile.
  * @param ds The cleaned telematics data.
  * @param topic The Kafka Topic of the telematics data to be maintenaned.
  */
EXPORT maintenance(DATASET(Types.l_clean) ds, STRING topic, STRING UID) := MODULE
  // Generate subFile:  OUTPUT the current cleaned batch messages
  curTime := Date.TimestampToString(Date.CurrentTimestamp(),'%Y-%m-%d-%H:%M:%S.%#'): INDEPENDENT;
  EXPORT subFileName := '~' + topic + '::subFile_' + curTime;
  // EXPORT subFileName := '~' + topic + '::subFile_' + UID;
  EXPORT outSubFile := OUTPUT(ds,,subFileName, COMPRESSED);

  // Increamental update to Superfile
  EXPORT superFileName := '~' + topic + '::superfile';
  // STD.File.CreateSuperFile(superFileName);

  EXPORT updateSuperFile := ORDERED(
    outSubFile;
    STD.File.StartSuperFileTransaction();
    STD.File.AddSuperFile(superFileName, subFileName);
    STD.File.FinishSuperFileTransaction();

  );
END;

/**
  * analysis functon analysis the performance of telematics pipeline 
  * @param topic The Kafka Topic of the telematics data to be analyzed.
  */
EXPORT analysis(STRING topic) := FUNCTION
ds := DATASET('~' + topic + '::superfile', Types.l_clean, THOR);
stats := TABLE(ds, {journeys := COUNT(ds),
                    minLife := MIN(GROUP, messagelife),
                    maxLife := MAX(GROUP, messagelife),
                    avgLife := AVE(GROUP, messagelife),
                    // varLife := VARIANCE(GROUP, messagelife),
                    SDLife := SQRT(VARIANCE(GROUP, messagelife))
                    }, 
                    MERGE);
report := PROJECT(stats, TRANSFORM(Types.l_analysis, SELF.topic := topic, SELF := LEFT));


RETURN report;
END;

END;
IMPORT kafka;
IMPORT Telematics;
IMPORT HPCCNode.Types AS Types;
IMPORT STD.Date;
IMPORT STD;

EXPORT Utils := MODULE

/**
  * consumer function retrieves the messages from Kafka topic.
  * @param topic The Kafka Topic where the messages are retrieve from.
  * @param brokers The IP/IPs of the Kafka brokers.
  * @param batchSize The number of message to consume at once via one consumer function call.
  * @return raw The raw telematics messages in JSON format.
  */
EXPORT consumer(STRING topic, STRING brokers, UNSIGNED4 batchSize=100000) := FUNCTION
  c:= kafka.KafkaConsumer(topic, brokers := brokers);
  batch := c.GetMessages(batchSize);
  raw := PROJECT(DISTRIBUTE(batch), TRANSFORM(Types.KafkaMessageFormat,SELF:= LEFT));
  RETURN raw;
END;

/**
  * parseMsg function parses the raw telematics messages to working dataset.
  * @param raw The raw telematics messages.
  * @return ds The parsed telematcis data.
  */
EXPORT parseMsg(DATASET(Types.KafkaMessageFormat) raw) := FUNCTION
  ds := PROJECT(raw, TRANSFORM(Types.l_journey, 
                                SELF := FROMJSON(Types.l_journey,
                                                  LEFT.message,
                                                  ONFAIL(TRANSFORM(Types.l_journey, SELF:= [])))));
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
Types.l_norm normRaw(Types.l_journey L, Types.l_waypoints R) := TRANSFORM
  SELF := L;
  SELF := R;
END;
normDS := NORMALIZE(DISTRIBUTE(ds, HASH(vehicle_id, journey_id)), LEFT.way_points, normRaw(LEFT, RIGHT), LOCAL);

// Step 2: Add useful info
locDS := GROUP(SORT(normDS, vehicle_id, time_offset, LOCAL), vehicle_id, journey_id, LOCAL);

// Add accel attribute: brake (-) or acceleration (+) in meter/second
accelDS := ITERATE(PROJECT(locDS, TRANSFORM(Types.l_accel, SELF := LEFT), LOCAL),
                    TRANSFORM(Types.l_accel, 
                              SELF.accel := IF(LEFT.vehicle_id <> '',RIGHT.speed -LEFT.speed, RIGHT.speed),                                                                                               
                              SELF := RIGHT));
// Add HB, HA, duration and other info
Types.l_clean0 rollupStats(Types.l_accel L, DATASET(Types.l_accel) R) := TRANSFORM
  speeding := R( speed >35.7632 ); 
  cntSpeeding := COUNT( speeding );
  HAs := R(accel >  3.79984);
  HBs := R(accel < -2.90576);
  cntHA := COUNT(HAs);
  cntHB := COUNT(HBs);
  maxTime := MAX(R, time_offset);
  minTime := MIN(R, time_offset);
  SELF.duration := maxTime - minTime + 1;
  SELF.HA := cntHA;
  SELF.HB := cntHB;
  SELF.highestSpeed := MAX(R, speed);
  SELF.speedingratio := cntSpeeding/SELF.duration;
  SELF := L;
END;
clean0 := ROLLUP(accelDS, GROUP, rollupStats(LEFT, ROWS(LEFT)));

// Add messagelife for each message: the time it's sent by messenger/producer to the time it's updated to the superFile
clean := PROJECT(clean0, TRANSFORM(Types.l_clean,
                                    SELF.time_processed :=  Date.CurrentTimestamp()/1000000,
                                    SELF.messagelife := SELF.time_processed - LEFT.time_sent,
                                    SELF := LEFT));
RETURN clean;
END;


/**
  * maintenance module includes the attributes related to the maintenance of telematics data, such as
  * the subFile and the superFile.
  * @param ds The cleaned telematics data.
  * @param topic The Kafka Topic of the telematics data to be maintenaned.
  */
EXPORT maintenance(DATASET(Types.l_clean) ds, STRING topic) := MODULE
  // Generate subFile:  OUTPUT the current cleaned batch messages
  curTimestamp :=Date.CurrentTimestamp(): INDEPENDENT;
  curTime := Date.TimestampToString(curTimestamp,'%Y-%m-%d-%H:%M:%S.%#'): INDEPENDENT;
  EXPORT subFileName := '~' + topic + '::subFile_' + curTime;
  EXPORT outSubFile := OUTPUT(ds,,subFileName, OVERWRITE, COMPRESSED);

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
                    varLife := VARIANCE(GROUP, messagelife),
                    SDLife := SQRT(VARIANCE(GROUP, messagelife))
                    }, 
                    MERGE);
report := PROJECT(stats, TRANSFORM(Types.l_analysis, SELF.topic := topic, SELF := LEFT));
RETURN report;
END;

END;
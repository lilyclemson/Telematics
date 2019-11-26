IMPORT Telematics;
IMPORT kafka;
IMPORT STD;
IMPORT STD.Date;
IMPORT HPCCNode.Types AS Types;

c:= kafka.KafkaConsumer('Telematics', brokers := '172.31.42.181');


//500,000 hits the memory limit
// c.ResetMessageOffsets();
ds := c.GetMessages(100000);
offsets := c.LastMessageOffsets(ds);
partitionCount := c.GetTopicPartitionCount();

// OUTPUT(ds, NAMED('MessageSample'));
// OUTPUT(COUNT(ds), NAMED('MessageCount'));
// OUTPUT(offsets, NAMED('LastMessageOffsets'));
// OUTPUT(partitionCount, NAMED('PartitionCount'));

// Message format 2: line
ds1 :=PROJECT(DISTRIBUTE(ds), TRANSFORM(Types.KafkaMessageFormat,SELF:= LEFT));
// OUTPUT(ds1,,'~thor::Telematics_test', THOR, OVERWRITE); 

// Data Enhancement
jds:= DATASET('~thor::Telematics_test', Types.KafkaMessageFormat, THOR);

// analysisDS := jds;
analysisDS := ds1;

//Parse the received raw JSON messages
l_trip := RECORD
  UNSIGNED4       time_offset{xpath('time_offset')};    
  DECIMAL11_6     x_pos{xpath('x_pos')};          
  DECIMAL11_6     y_pos{xpath('y_pos')};          
  DECIMAL9_6      speed{xpath('speed')};          
END;
l_json := RECORD
  STRING vehicle_id{xpath('vehicle_id')};
  UNSIGNED4 journey_id{xpath('journey_id')};
  REAL time_sent{xpath('time')};
  DATASET(l_trip) way_points{xpath('way_points')};
END;

ds2 := PROJECT(analysisDS, TRANSFORM(l_json, SELF := FROMJSON(l_json, LEFT.message, ONFAIL(TRANSFORM(l_json, SELF:= [])))));
// OUTPUT(ds2);

// Data Analytics
// Step1: Normalize the dataset and reverse it to the original KOLN
l_norm := RECORD
  Telematics.Files.KOLN.ExpandedData.Layout;
  UNSIGNED4 journey_id;
  REAL time_sent;
END;

l_norm normRaw(l_json L, l_trip R) := TRANSFORM
  SELF := L;
  SELF := R;
END;
ds3 := NORMALIZE(DISTRIBUTE(ds2, HASH(vehicle_id, journey_id)), LEFT.way_points, normRaw(LEFT, RIGHT), LOCAL);
// OUTPUT(ds3,, '~telematics::normalized_koln', THOR, OVERWRITE);
// OUTPUT(ds3);

curDS := ds3;
// curDS := DATASET('~telematics::normalized_koln', l_norm, THOR);

// Step 2: analysis stats
// DISTRIBUTE the KOLN dataset, then SORT & GROUP
locDS := GROUP(SORT(curDS, vehicle_id, time_offset, LOCAL), vehicle_id, journey_id, LOCAL);

// Add accel attribute: brake (-) or acceleration (+) in meter/second
l_accel := RECORD
  l_norm;
  DECIMAL9_6 accel := 0;       //  brake (-) or acceleration (+) in meter/second
END;

accelDS := ITERATE(PROJECT(locDS, TRANSFORM(l_accel, SELF := LEFT), LOCAL),
                    TRANSFORM(l_accel, 
                              SELF.accel := IF(LEFT.vehicle_id <> '',RIGHT.speed -LEFT.speed, RIGHT.speed),                                                                                               
                              SELF := RIGHT)
                );
// OUTPUT(accelDS);

// Analysis report
l_report := RECORD
  STRING vehicle_id;        // vehicle identifier
  UNSIGNED4 journey_id;     // journey identifier per vehicle
  UNSIGNED4 duration;       // duration (second) per journey
  UNSIGNED4 HB;             // Hard-break events per journey
  UNSIGNED4 HA;             // Hard-acceleration events per journey
  DECIMAL9_6 highestSpeed;  // Highest speed per journey
  DECIMAL9_6 speedingratio; // (speeding time) / (journey duration)
  REAL time_sent;
END;

l_report rollupStats(l_accel L, DATASET(l_accel) R) := TRANSFORM
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
stats := ROLLUP(accelDS, GROUP, rollupStats(LEFT, ROWS(LEFT)));
// OUTPUT(stats);

// add lifetime per message from sent by messengernode to update to superFile
l_time := RECORD
  STRING vehicle_id;        // vehicle identifier
  UNSIGNED4 journey_id;     // journey identifier per vehicle
  UNSIGNED4 duration;       // duration (second) per journey
  UNSIGNED4 HB;             // Hard-break events per journey
  UNSIGNED4 HA;             // Hard-acceleration events per journey
  DECIMAL9_6 highestSpeed;  // Highest speed per journey
  DECIMAL9_6 speedingratio; // (speeding time) / (journey duration)
  REAL time_sent;
  REAL time_processed;
  REAL messagelife;
END;

timeDS := PROJECT(stats, TRANSFORM(l_time,
                                    SELF.time_processed :=  Date.CurrentTimestamp()/1000000,
                                    SELF.messagelife := SELF.time_processed - LEFT.time_sent,
                                    SELF := LEFT));


// Update to superfile
curTimestamp :=Date.CurrentTimestamp(): INDEPENDENT;
curTime := Date.TimestampToString(curTimestamp,'%Y-%m-%d-%H:%M:%S.%#'): INDEPENDENT;
subFileName := '~Telematics::subFile_' + curTime;
outSubFile := OUTPUT(timeDS,,subFileName, OVERWRITE, COMPRESSED);

// Increamental update to Superfile
// Create superfile
superFileName := '~Telematics::superfile';
// STD.File.CreateSuperFile(superFileName);

updateSuperFile := ORDERED(
  outSubFile;
  STD.File.StartSuperFileTransaction();
  STD.File.AddSuperFile(superFileName, subFileName);
  STD.File.FinishSuperFileTransaction();
);

updateSuperFile : WHEN ( CRON ( '* * * * *' ) );


// OUTPUT(timeDS,,subFileName, OVERWRITE, COMPRESSED);
// STD.File.StartSuperFileTransaction();
// STD.File.AddSuperFile(superFileName, subFileName);
// STD.File.FinishSuperFileTransaction();
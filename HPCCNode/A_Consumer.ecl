IMPORT kafka;
IMPORT STD;
IMPORT STD.Date;
IMPORT HPCCNode.Types AS Types;

c:= kafka.KafkaConsumer('Telematics', brokers := '172.31.42.181');

// c.ResetMessageOffsets();
ds := c.GetMessages(1000000);
offsets := c.LastMessageOffsets(ds);
partitionCount := c.GetTopicPartitionCount();

OUTPUT(ds, NAMED('MessageSample'));
OUTPUT(COUNT(ds), NAMED('MessageCount'));
OUTPUT(offsets, NAMED('LastMessageOffsets'));
OUTPUT(partitionCount, NAMED('PartitionCount'));


// Message format 1:  csvReader
// ds1 :=PROJECT(ds, TRANSFORM(Types.KafkaMessageFormat, SELF.message:= STD.str.FindReplace(LEFT.message[2..((LENGTH(LEFT.message)-1))], 'way_points', '"way_points"'),SELF:= LEFT));

// Message format 2: line : faster
ds1 :=PROJECT(ds, TRANSFORM(Types.KafkaMessageFormat,SELF:= LEFT));
// OUTPUT(ds1,,'~thor::Telematics_test', THOR, OVERWRITE); 

curTimestamp :=Date.CurrentTimestamp(): INDEPENDENT;
curTime := Date.TimestampToString(curTimestamp,'%Y-%m-%dT%H:%M:%S.%#'): INDEPENDENT;
subFileName := '~Telematics::subFile_' + curTime+'.csv';
outSubFile := OUTPUT(ds1,,subFileName, CSV , OVERWRITE, COMPRESSED);
// output(ds1);

// Increamental update to Superfile
// Create superfile
superFileName := '~Telematics::superfile';
STD.File.CreateSuperFile(superFileName);

updateSuperFile := ORDERED(
  outSubFile;
  STD.File.StartSuperFileTransaction();
  STD.File.AddSuperFile(superFileName, subFileName);
  STD.File.FinishSuperFileTransaction();
);

updateSuperFile : WHEN ( CRON ( '0-59/20 * * * *' ) );
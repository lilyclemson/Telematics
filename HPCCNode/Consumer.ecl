IMPORT $.Types AS Types;
IMPORT $.Utils;
IMPORT STD.Date;
IMPORT STD.System;
IMPORT STD;

#OPTION('pickBestEngine', FALSE);

/** 
  * Consumer process to retrieve Kafka messages and update to the superFile
  */

UID := Date.TimestampToString(Date.CurrentTimestamp(),'%Y-%m-%d-%H:%M:%S.%#') : INDEPENDENT;
KAFKA_MSG_BATCH_SIZE := 110000; // 100000;

#WORKUNIT('name', 'Consumer (batch size ' + (STRING)KAFKA_MSG_BATCH_SIZE + ')');

// Step 1: Consumer retrievel information from broker
raw := Utils.consumer(Types.Topic, Types.broker, KAFKA_MSG_BATCH_SIZE);

// Step 2: Parse the raw Kafka message
parsed := Utils.parseMsg(raw);
// Step 3: clean the parsed messages
cleaned := Utils.clean(parsed) : INDEPENDENT;


// Step 4: update the cleaned messages to superFile
P1 := Utils.maintenance(cleaned, Types.Topic, UID).updateSuperFile;

// P1 := Utils.maintenance(cleaned, Types.Topic, UID).outSubFile;

Rec := Record

	STRING TimeStamp;
	STRING CronJob;
	INTEGER Batch_Size;
	INTEGER Record_Count;
	REAL Min_message_len;
	REAL Max_message_len;
	REAL Avg_message_len;
	INTEGER WaypointCount;
	REAL Min_Waypoint;
	REAL Max_Waypoint;
	REAL Avg_Waypoint;
	REAL Real_NumberOf_Processes;
END;

DS_Stats := DATASET([{ UID,
								'1 Min', 
								KAFKA_MSG_BATCH_SIZE,
								COUNT(cleaned), 
								 MIN(cleaned, message_len),
								 MAX(cleaned, message_len),
								 AVE(cleaned, message_len),
								 SUM(cleaned, WaypointCount),
							   MIN(cleaned, WaypointCount),
							   MAX(cleaned, WaypointCount),
							   AVE(cleaned, WaypointCount),
							   COUNT(cleaned) *  SUM(cleaned, WaypointCount)}], Rec);

FileName := '~' + UID + 'telematics::Stats_';
outStatFile := OUTPUT(DS_Stats,, FileName, COMPRESSED);
superFileName := '~' + 'superfile::Stats';		
					 
P2 := ORDERED(
    outStatFile;
    STD.File.StartSuperFileTransaction();
    STD.File.AddSuperFile(superFileName, FileName);
    STD.File.FinishSuperFileTransaction();

  );

updateSuperfileAction := PARALLEL(P1, P2);

// Update superFile every second by scheduling CRON job
IF(EXISTS(cleaned), updateSuperfileAction): WHEN ( CRON ( '* * * * *' ) );
//catch batch size for 1.2.5.10 min
// then set the cron job


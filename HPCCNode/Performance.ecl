IMPORT HPCCNode.Utils;
IMPORT HPCCNode.Types;


/**
  * Generate report for the performance of our pipeline.
  * It includes stats of the superFile of the choosen topic, such as 
  * the MAX, MIN, AVG, VARIANCE, SD of the messageLife.
  */
report := Utils.analysis(Types.TOPIC);
OUTPUT(report, NAMED('Performance_Analysis'));

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

 
ds := DATASET('~' + 'superfile::Stats', Rec, THOR);
ds;
// OUTPUT(ds,,'This::needs::tobe::looked::at::1_Min::50K', THOR);

// ds(Record_Count > 110000);
// count(ds(Record_Count < 110000));
// ds(Record_Count < 110000);
// ds(Record_Count = 110000);
// ds(Record_Count = 32065);

ResRec := RECORD
	STRING	CronJob;
	INTEGER Avg_Batch_Size;
	INTEGER Num_Jobs_Ran;
	REAL Batch_Min_Msg_len;
	REAL Batch_Max_Msg_len;
	REAL Batch_Avg_Msg_len;
	REAL Batch_Min_Waypoint;
	REAL Batch_Max_Waypoint;
	REAL Batch_Avg_Waypoint;										
END;

stats := DATASET([{ds[1].CronJob,
									AVE(ds, Record_Count),
									COUNT(ds),
                  MIN(ds, Min_message_len),
                  MAX(ds, Max_message_len),
                  AVE(ds, Avg_message_len),
									MIN(ds, Min_Waypoint),
									MAX(ds, Max_Waypoint),
									AVE(ds, Avg_Waypoint)
                  }], ResRec);
	
OUTPUT(stats, NAMED('Stats_Analysis'));

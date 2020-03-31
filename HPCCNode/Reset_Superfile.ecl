IMPORT $.Types AS Types;
IMPORT Std;

superFileName := '~' + Types.Topic + '::superfile';

Std.File.ClearSuperFile(superFileName, del := TRUE);


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


superStatFileName := '~' + 'superfile::Stats';		
Std.File.ClearSuperFile(superStatFileName, del := TRUE);

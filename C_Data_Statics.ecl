IMPORT LevinVehicleTelematics as LV;
IMPORT LV.B_Data_Ingestion;
IMPORT Std;
IMPORT Visualizer;

Layout_tripDevice := RECORD
    B_Data_Ingestion.Layout.deviceID;
	B_Data_Ingestion.Layout.tripID;
END;

deviceTripFile :=  PROJECT(B_Data_Ingestion.raw, TRANSFORM(Layout_tripDevice, SELF := LEFT));
deviceTripFileTable := TABLE(deviceTripFile);
SortedDeviceTripFileTable := SORT(deviceTripFileTable,deviceID,tripID);
uniqueDeviceTrip := TABLE(SortedDeviceTripFileTable,{deviceID,tripID,Cnt:= COUNT(GROUP)},deviceID,tripID);

//uniqueDeviceCountTime;
uniqueDeviceCountTime := TABLE(SortedDeviceTripFileTable,{deviceID,Cnt:= COUNT(GROUP)},deviceID);

OUTPUT(uniqueDeviceCountTime, NAMED('uniqueDeviceCountTime'));
uniqueDeviceCountTimeMappings := DATASET([{'Device ID','deviceID'},{'Total Time(s)','cnt'}],Visualizer.KeyValueDef);

Visualizer.MultiD.area('uniqueDeviceCountTimeChart', /*datasource*/, 'uniqueDeviceCountTime', uniqueDeviceCountTimeMappings, /*filteredBy*/, /*dermatologyProperties*/ );


//uniqueDeviceCountTrip;
uniqueDeviceCountTrip := TABLE(uniqueDeviceTrip,{deviceID,CntTrip:= COUNT(GROUP)},deviceID);

OUTPUT(uniqueDeviceCountTrip, NAMED('uniqueDeviceCountTrip'));
uniqueDeviceCountTripMappings := DATASET([{'Device ID','deviceID'},{'Total Trip','cntTrip'}],Visualizer.KeyValueDef);

Visualizer.MultiD.area('uniqueDeviceCountTripChart', /*datasource*/, 'uniqueDeviceCountTrip', uniqueDeviceCountTripMappings, /*filteredBy*/, /*dermatologyProperties*/ );

//uniqueDeviceCountTrip;
uniqueDeviceTripAvgTime := TABLE(uniqueDeviceTrip,{deviceID,AvgTripTime:= AVE(GROUP,cnt)},deviceID);

OUTPUT(uniqueDeviceTripAvgTime, NAMED('uniqueDeviceTripAvgTime'));
uniqueDeviceTripAvgTimeMappings := DATASET([{'Device ID','deviceID'},{'Average Time for trip','avgTripTime'}],Visualizer.KeyValueDef);

Visualizer.MultiD.area('uniqueDeviceTripAvgTimeChart', /*datasource*/, 'uniqueDeviceTripAvgTime', uniqueDeviceTripAvgTimeMappings, /*filteredBy*/, /*dermatologyProperties*/ );


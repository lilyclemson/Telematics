IMPORT LevinVehicleTelematics as LV;
IMPORT LV.B_Data_Ingestion;
IMPORT Std;
IMPORT Visualizer;

Layout_tripDevice := RECORD
    B_Data_Ingestion.Layout.deviceID;
	B_Data_Ingestion.Layout.tripID;
    B_Data_Ingestion.Layout.gps_speed;
    B_Data_Ingestion.Layout.rpm;
END;

deviceTripFile :=  PROJECT(B_Data_Ingestion.raw, TRANSFORM(Layout_tripDevice, SELF := LEFT));
deviceTripFileTable := TABLE(deviceTripFile);
//Sort deviceTripFileTable by Device ID and Trip ID
SortedDeviceTripFileTable := SORT(deviceTripFileTable,deviceID,tripID);
OUTPUT(SortedDeviceTripFileTable,,'~levin::SortedDeviceTripFileTable.csv',OVERWRITE);
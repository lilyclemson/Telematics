IMPORT $;

EXPORT E_Data_Ingestion := MODULE

EXPORT Layout := RECORD
  $.B_Data_Ingestion.Layout.deviceID;
  $.B_Data_Ingestion.Layout.tripID;
  $.B_Data_Ingestion.Layout.gps_speed;
  $.B_Data_Ingestion.Layout.rpm;
END;

EXPORT raw := DATASET('~levin::SortedDeviceTripFileTable.csv', Layout, THOR);
END;

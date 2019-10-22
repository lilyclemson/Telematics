IMPORT STD;
IMPORT LevinVehicleTelematics as LV;
IMPORT LV.B_Data_Ingestion;
IMPORT Std;

Layout := RECORD
  INTEGER tripID;
  UNSIGNED4 deviceID;
  STRING20 timeStamp;
  STRING164 accData;
  DECIMAL7_4 gps_speed;
  DECIMAL5_3 battery;
  UNSIGNED3 cTemp;
  UNSIGNED4 dtc;
  DECIMAL6_4 eLoad;
  UNSIGNED2 iat;
  UNSIGNED3 imap;
  DECIMAL11_7 kpl;
  DECIMAL5_2 maf;
  DECIMAL7_2 rpm;
  DECIMAL4_1 speed;
  UNSIGNED1 tAdv;
  DECIMAL10_8 tPos;
END;

//For received Kafka message dataset in super file
SuperFileName := '~levin::kafkamessagetablesuper';
file := DATASET(SuperFileName, Layout, THOR);
//For local file on HPCC system
//file := DATASET('~levin::vehicle_telematics::v2_1.csv', Layout, CSV(HEADING(1),separator(',')));
//file := DATASET('~levin::vehicle_telematics::testv2test.csv', Layout, CSV(HEADING(1),separator(',')));
//file := DATASET('~levin::testv2_10000.csv', Layout, CSV(HEADING(1),separator(',')));
//file := DATASET('~levin::vehicle_telematics::testv2_13000.csv', Layout, CSV(HEADING(1),separator(',')));
//file := DATASET('~levin::testv2_100000.csv', Layout, CSV(HEADING(1),separator(',')));
//file := DATASET('~levin::testv2_300000.csv', Layout, CSV(HEADING(1),separator(',')));
//Clean data which tripID and deviceID are zero
fileFilter1 :=file(tripID != 0 );
SortedFile := SORT(fileFilter1,deviceID);

//Get all the names of files in the super file
SubFiles := STD.File.SuperFileContents(SuperFileName);

CountSubFiles := COUNT(SubFiles);
// CountSubFiles;
fileFilterResult := if(CountSubFiles>0,OUTPUT(SortedFile,,'~levin::fileFilter.csv',OVERWRITE));

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
SortedDeviceTripFileTableResult := OUTPUT(SortedDeviceTripFileTable,,'~levin::SortedDeviceTripFileTable.csv',OVERWRITE);

start_build_process := ORDERED(
                                fileFilterResult;
                                SortedDeviceTripFileTableResult
                                  );

start_build_process : WHEN ( CRON ( '0-59/0 * * * *' ) ); //SCHEDULE A JOB every 1 minute




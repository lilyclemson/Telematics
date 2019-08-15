IMPORT STD;

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
file := DATASET('~levin::vehicle_telematics::v2_1.csv', Layout, CSV(HEADING(1),separator(',')));
//file := DATASET('~levin::vehicle_telematics::vehicle_telematics.csv', Layout, CSV(HEADING(1),separator(',')));
//file := DATASET('~levin::vehicle_telematics::testv2.csv', Layout, CSV(HEADING(1),separator(',')));
//Clean data which tripID and deviceID are zero
fileFilter1 :=file(tripID != 0 );

SortedFile := SORT(fileFilter1,deviceID);
OUTPUT(SortedFile,,'~levin::fileFilter.csv',OVERWRITE);




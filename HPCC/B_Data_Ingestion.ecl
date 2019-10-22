EXPORT B_Data_Ingestion := MODULE

EXPORT Layout := RECORD
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

EXPORT raw := DATASET('~levin::fileFilter.csv', Layout, THOR);
//EXPORT rawTable := TABLE('~levin::vehicle_telematics', Layout)
END;

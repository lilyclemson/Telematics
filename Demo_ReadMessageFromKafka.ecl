IMPORT kafka;
IMPORT LevinVehicleTelematics as LV;
IMPORT LV.B_Data_Ingestion;
IMPORT STD;
IMPORT STD.Date;

//Change the maximum output value to 2000 MB
#option('outputLimit',2000);

consumer := kafka.KafkaConsumer('telematics', brokers := '10.128.0.3');
//max 300 message
ds := consumer.GetMessages(300);
OUTPUT(ds);

KafkaMessageFormat  := RECORD
  UNSIGNED4 partitionNum;
  INTEGER8 offset;
  STRING message;
END;

MessageFormat  := RECORD
  STRING message;
END;
//JSON structure
pointLayout := RECORD
  INTEGER tripid{xpath('tripid')};
  UNSIGNED4 deviceID{xpath('deviceid')};
  STRING20 timeStamp{xpath('timestamp')};
  STRING164 accData{xpath('accdata')};
  DECIMAL7_4 gps_speed{xpath('gps_speed')};
  DECIMAL5_3 battery{xpath('battery')};
  UNSIGNED3 cTemp{xpath('cTemp')};
  UNSIGNED4 dtc{xpath('dtc')};
  DECIMAL6_4 eLoad{xpath('eLoad')};
  UNSIGNED2 iat{xpath('iat')};
  UNSIGNED3 imap{xpath('imap')};
  DECIMAL11_7 kpl{xpath('kpl')};
  DECIMAL5_2 maf{xpath('maf')};
  DECIMAL7_2 rpm{xpath('rpm')};
  DECIMAL4_1 speed{xpath('speed')};
  UNSIGNED1 tAdv{xpath('tAdv')};
  DECIMAL10_8 tPos{xpath('tPos')};
END;
//Row data type
rowRec := RECORD
   DATASET(pointLayout) Row {xpath('Row')};
END;
//Kafka Message Type
KafkaMessageFormatAdd  := RECORD
  KafkaMessageFormat;
  rowRec tripJson;
END;
 
KafkaMessageFormatAdd kafkaMessageTableMove(KafkaMessageFormat L) := transform
   rowMessage := L.message[12..LENGTH(L.message)];
   self.tripJson := FROMJSON(rowRec,rowMessage);
   self := L;
end;

//Convert source data to KafkaMessageFormatAdd Structure
kafkaMessageTable := project(ds, kafkaMessageTableMove(left));
kafkaMessageTable;


curTime:=Date.TimestampToString(Date.CurrentTimestamp(),'%Y-%m-%dT%H:%M:%S.%#');

//If Kafka Message is not null, it will save to a dataset 
if(COUNT(kafkaMessageTable)>0,OUTPUT(kafkaMessageTable,,'~levin::kafkaMessageTable'+curTime+'.csv',OVERWRITE));
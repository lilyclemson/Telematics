IMPORT kafka;
IMPORT LevinVehicleTelematics as LV;
IMPORT LV.B_Data_Ingestion;
IMPORT STD;
IMPORT STD.Date;

//Change the maximum output value to 2000 MB
#option('outputLimit',2000);

//consumer := kafka.KafkaConsumer('telematics', brokers := '10.128.0.3');//google cluster
consumer := kafka.KafkaConsumer('telematics', brokers := '172.31.42.181');//AWS cluster
//max 520 message(for 32 GB memory)
ds := consumer.GetMessages(300);

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
   DATASET(pointLayout) Row{xpath('Row')};
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

MyFormat := RECORD
  DATASET(pointLayout) Row{xpath('Row')} := kafkaMessageTable.tripJson.Row;
END;

subKafkaMessageTable :=  (TABLE(kafkaMessageTable,MyFormat)).Row;

curTimestamp :=Date.CurrentTimestamp(): INDEPENDENT;
curTime := Date.TimestampToString(curTimestamp,'%Y-%m-%dT%H:%M:%S.%#'): INDEPENDENT;
//curTime;
currentName := '~levin::kafkaMessageTable'+curTime+'.csv': INDEPENDENT;

//If Kafka Message has save to a dataset, if there is no super file, create a super file. Then move the previous dataset to the super file
superFileName := '~levin::kafkaMessageTableSuper';

superFileNameExists := STD.File.SuperFileExists(superFileName);

//Clean the data
cleanKafkaMessageTable := subKafkaMessageTable(tripid != 0);

//Count the number of trips
countKafkaMessageTable:= COUNT(cleanKafkaMessageTable);

//Create Super file if Not Exists when Kafka message is not null
createSuperFile := if(superFileNameExists = False,STD.File.CreateSuperFile(superFileName));

//Create Super file if Not Exists when Kafka message is not null
//If Kafka Message is not null, it will save to a dataset 
//Move the previous dataset to the super file
moveProcess := ORDERED(
  OUTPUT(ds);
  createSuperFile;
  OUTPUT(cleanKafkaMessageTable,,currentName,OVERWRITE);//Save Kafka Message to a dataset 
  STD.File.StartSuperFileTransaction();
  STD.File.AddSuperFile(superFileName, currentName);
  STD.File.FinishSuperFileTransaction();
);

//If Kafka Message is not null
Result := if(countKafkaMessageTable>0,moveProcess);

start_build_process := ORDERED(
                                Result;
                                  );

start_build_process : WHEN ( CRON ( '0-59/0 * * * *' ) ); //SCHEDULE A JOB every 1 minute
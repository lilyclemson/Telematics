IMPORT STD;
IMPORT LevinVehicleTelematics as LV;
IMPORT LV.B_Data_Ingestion;
IMPORT kafka;

//Change the maximum output value to 1000 MB
#option('outputLimit',1000);

Layout_tripDeviceJson := RECORD
  $.B_Data_Ingestion.Layout.deviceID;
  $.B_Data_Ingestion.Layout.tripID;
  DATASET($.B_Data_Ingestion.Layout) tripJson;
END;

Layout_tripDeviceNeedJson := RECORD
  DATASET($.B_Data_Ingestion.Layout) tripJson;
END;

//Read source data
deviceTripFile :=  PROJECT($.B_Data_Ingestion.raw, TRANSFORM($.B_Data_Ingestion.Layout, SELF := LEFT));
deviceTripFileTable := TABLE(deviceTripFile);
deviceTripFileTable;

Layout_tripDeviceJson deviceTripFileTableMove($.B_Data_Ingestion.Layout L) := transform
   self.tripJson := L;
   self := L;
end;

//Convert source data to Layout_tripDeviceJson Structure
deviceTripProjectTable := project(deviceTripFileTable, deviceTripFileTableMove(left));
deviceTripProjectTable;

Layout_primaryTripDevice := RECORD
  $.B_Data_Ingestion.Layout.deviceID;
  $.B_Data_Ingestion.Layout.tripID;
END;

//Find the unique device ID and trip ID
primaryTripDeviceTable := dedup(table(deviceTripFileTable, {deviceID,tripID}), deviceID,tripID);
primaryTripDeviceTable;

Layout_tripDeviceJson primaryTripDeviceTableMove(Layout_primaryTripDevice L) := transform
   self.tripJson := [];
   self := L;
end;

//Implement unique device ID and trip ID with Null tripJson
tripDeviceJsonOnly := project(primaryTripDeviceTable, primaryTripDeviceTableMove(left));
tripDeviceJsonOnly;
//OUTPUT(tripDeviceJsonOnly,,'~levin::tripDeviceJsonOnly.csv',OVERWRITE);
//COUNT(tripDeviceJsonOnly);

Layout_tripDeviceJson tripDeviceJsonCombine(Layout_tripDeviceJson L, Layout_tripDeviceJson R, INTEGER C):= transform
   self.tripJson := L.tripJson + R.tripJson;
   self := L;
end;

//Combine the DATASET when device ID and trip ID is the same                             
tripJsonTable := DENORMALIZE(tripDeviceJsonOnly, deviceTripProjectTable, LEFT.deviceID = RIGHT.deviceID and LEFT.tripID = RIGHT.tripID , tripDeviceJsonCombine(LEFT, RIGHT, COUNTER));
//tripJsonTable;

Layout_tripDeviceJson_str := RECORD
  $.B_Data_Ingestion.Layout.deviceID;
  $.B_Data_Ingestion.Layout.tripID;
  DATASET($.B_Data_Ingestion.Layout) tripJson;
  UTF8 tripJsonStr;
  STRING  message;
END;

Layout_tripDeviceJson_str tripJsonTableMove(Layout_tripDeviceJson L) := transform
   self.tripJsonStr := '';
   self.message := '';
   self := L;
end;

tripJsonTableStr := project(tripJsonTable, tripJsonTableMove(left));
//tripJsonTableStr;

Layout_tripDeviceJson_str withTripDeviceJsonRecs(Layout_tripDeviceJson_str L, Layout_tripDeviceJson_str R) := TRANSFORM
  SELF.tripJsonStr := TOJSON(ROW(R,Layout_tripDeviceNeedJson));
  SELF.message := (STRING)TOJSON(ROW(R,Layout_tripDeviceNeedJson));
  SELF := R;
END;

//Convert the DATASET to JSON string
tripDeviceJsonTable := ITERATE(tripJsonTableStr,withTripDeviceJsonRecs(LEFT,RIGHT));
//OUTPUT(tripDeviceJsonTable);
OUTPUT(tripDeviceJsonTable,,'~levin::tripDeviceJsonTable.csv',OVERWRITE);

tripDeviceJsonTableDis := DISTRIBUTE(tripDeviceJsonTable);

tripCount := COUNT(tripDeviceJsonTable);
tripCount;
p := kafka.KafkaPublisher('telematics', brokers := '10.128.0.3');

APPLY(tripDeviceJsonTableDis, ORDERED(p.PublishMessage(message)));

// The following configuration parameters are set by the plugin for publishers, overriding their normal default values:

// queue.buffering.max.messages=1000000//1MB
// compression.codec=snappy
// message.send.max.retries=3
// retry.backoff.ms=500
//=>queue.buffering.max.messages=30000000//30MB


// The following configuration parameters are set by the plugin for consumers, overriding their normal default values:

// compression.codec=snappy
// queued.max.messages.kbytes=10000000
// fetch.message.max.bytes=10000000
// auto.offset.reset=smallest
//=>queued.max.messages.kbytes=100000000//100MB
// fetch.message.max.bytes=100000000//100MB



 

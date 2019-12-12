IMPORT $.^.Telematics;

EXPORT Types := MODULE

EXPORT TOPIC := 'Telematics';
EXPORT Broker := '172.31.42.181';

EXPORT KafkaMessageFormat  := RECORD
  UNSIGNED4 partitionNum;
  INTEGER8 offset;
  STRING message;
END;

EXPORT l_waypoints := RECORD
  UNSIGNED4       time_offset{xpath('time_offset')};    
  DECIMAL11_6     x_pos{xpath('x_pos')};          
  DECIMAL11_6     y_pos{xpath('y_pos')};          
  DECIMAL9_6      speed{xpath('speed')};          
END;

EXPORT l_journey := RECORD
  STRING vehicle_id{xpath('vehicle_id')};
  UNSIGNED4 journey_id{xpath('journey_id')};
  REAL time_sent{xpath('time')};
  DATASET(l_waypoints) way_points{xpath('way_points')};
END;

EXPORT l_norm := RECORD
  Telematics.Files.KOLN.ExpandedData.Layout;
  UNSIGNED4 journey_id;
  REAL time_sent;  
END;

EXPORT l_accel := RECORD
  l_norm;
  DECIMAL9_6 accel := 0;       //  brake (-) or acceleration (+) in meter/second
END;

EXPORT l_clean0 := RECORD
  STRING vehicle_id;        // vehicle identifier
  UNSIGNED4 journey_id;     // journey identifier per vehicle
  UNSIGNED4 duration;       // duration (second) per journey
  UNSIGNED4 HB;             // Hard-break events per journey
  UNSIGNED4 HA;             // Hard-acceleration events per journey
  DECIMAL9_6 highestSpeed;  // Highest speed per journey
  DECIMAL9_6 speedingratio; // (speeding time) / (journey duration)
  REAL time_sent;           // The time the message sent by messenger
END;

EXPORT l_clean := RECORD
  STRING vehicle_id;        // vehicle identifier
  UNSIGNED4 journey_id;     // journey identifier per vehicle
  UNSIGNED4 duration;       // duration (second) per journey
  UNSIGNED4 HB;             // Hard-break events per journey
  UNSIGNED4 HA;             // Hard-acceleration events per journey
  DECIMAL9_6 highestSpeed;  // Highest speed per journey
  DECIMAL9_6 speedingratio; // (speeding time) / (journey duration)
  REAL time_sent;           // The time the message sent by messenger
  REAL time_processed;      // The time the message updated to the superFile
  REAL messagelife;         // The duration from the message sent to updated: time_processed - time_sent 
END;

EXPORT l_analysis := RECORD
  STRING topic;
  INTEGER journeys;
  REAL minLife;
  REAL maxLife;
  REAL avgLife;
  REAL varLife;
  REAL SDLife; 
END;

END;
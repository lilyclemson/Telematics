IMPORT HPCCNode.Types AS Types;

jds:= DATASET('~thor::Telematics_test', Types.KafkaMessageFormat, THOR);
OUTPUT(jds);

l_trip := RECORD
  UNSIGNED4       time_offset{xpath('time_offset')};    
  DECIMAL11_6     x_pos{xpath('x_pos')};          
  DECIMAL11_6     y_pos{xpath('y_pos')};          
  DECIMAL9_6      speed{xpath('speed')};          
END;
l_json := RECORD
  STRING vehicle_id{xpath('vehicle_id')};
  UNSIGNED4 journey_id{xpath('journey_id')};
  DATASET(l_trip) way_points{xpath('way_points')};
  // BOOLEAN parsed := TRUE;
END;

// ds := PROJECT(jds, TRANSFORM(l_json, SELF := FROMJSON(l_json, LEFT.message, ONFAIL(TRANSFORM(l_json, SELF.parsed := FALSE, SELF:= [])))));
ds := PROJECT(jds[1..5], TRANSFORM(l_json, SELF := FROMJSON(l_json, LEFT.message, ONFAIL(TRANSFORM(l_json, SELF:= [])))));
OUTPUT(ds);
OUTPUT(ds, , '~telematics::normalized_koln', THOR, OVERWRITE);


l_trip1 := RECORD
  UNSIGNED4       time_offset;    
  DECIMAL11_6     x_pos;          
  DECIMAL11_6     y_pos;          
  DECIMAL9_6      speed;          
END;
l_json1 := RECORD
  STRING vehicle_id;
  UNSIGNED4 journey_id;
  DATASET(l_trip1) way_points;
END;

ds1 := PROJECT(ds, TRANSFORM(l_json1, SELF := LEFT));


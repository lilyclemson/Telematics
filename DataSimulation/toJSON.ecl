IMPORT $.^.Telematics;

// Original dataset
// ds := DATASET(Telematics.Files.KOLN.CleanData.PATH, Telematics.Files.KOLN.CleanData.LAYOUT, THOR);
// Expanded dataset
ds := DATASET(Telematics.Files.KOLN.ExpandedData.PATH, Telematics.Files.KOLN.ExpandedData.Layout , THOR);

// Fileter out journeys with zero or one waypoint
count_waypoints := TABLE(ds, {vehicle_id, cnt := COUNT(GROUP)}, vehicle_id, MERGE);
multiwaypoinsDS := JOIN(ds, count_waypoints(cnt = 1), LEFT.vehicle_id = RIGHT.vehicle_id, LOOKUP, LEFT ONLY);
// OUTPUT(multiwaypoinsDS);

// Step 1: Add Journey_ID and way_points
locDS := GROUP(SORT(DISTRIBUTE(multiwaypoinsDS, HASH32(vehicle_id)), vehicle_id, time_offset, LOCAL), vehicle_id, LOCAL);
// OUTPUT(locDS);

l_Journey := RECORD
  UNSIGNED4 journey_ID := 1; 
  Telematics.Files.KOLN.CleanData.LAYOUT;
  STRING way_points := '';
END;
l_Journey transGap(l_Journey L,l_Journey R) := TRANSFORM
    time_gap := IF(L.vehicle_id <> '', R.time_offset - L.time_offset, 0);
    SELF.journey_ID := IF(L.vehicle_id = '', R.journey_ID,
                          IF(time_gap > 1, L.journey_ID + 1, L.journey_ID));
    SELF.way_points := '{' + (STRING)TOJSON(ROW({R.time_offset, R.x_pos, R.y_pos, R.speed}, Telematics.Files.KOLN.CleanData.LAYOUT-vehicle_id)) + '}';                                                                                         
    SELF := R;
END;

gapDS := ITERATE(PROJECT(locDS, TRANSFORM(l_Journey, SELF := LEFT), LOCAL), transGap(LEFT, RIGHT));
// OUTPUT(gapDS);


// Step 2: ROLLUP to concatenating all the way_points of each journey
gJourney := GROUP(gapDS, vehicle_id, journey_ID, LOCAL);
l_Journey rollupJourney(l_Journey L, l_Journey R) := TRANSFORM
  SELF.way_points := L.way_points + ',' + R.way_points;
  SELF.time_offset := L.time_offset;
  SELF := R;
END;

gjourneyDS := ROLLUP(gJourney, 
                    LEFT.vehicle_id = RIGHT.vehicle_id
                    AND
                    LEFT.journey_ID = RIGHT.journey_ID , 
                    rollupJourney(LEFT, RIGHT));
// OUTPUT(gjourneyDS);

// Step 3: PROJECT to the one line JSON string
l_JSON := RECORD
  STRING vehicle_ID;
  UNSIGNED4 journey_ID;
  STRING way_points;
END;

// Globally sort the time_offset of all the journeys 
sorted_time_offset := PROJECT(SORT(UNGROUP(gjourneyDS), time_offset), l_JSON);

// Format the way_points from objects to array
journeyDS0 := PROJECT(sorted_time_offset, TRANSFORM(l_JSON, SELF.way_points := '"way_points": [' +LEFT.way_points+ ']',
                                                    SELF := LEFT), LOCAL);
// OUTPUT(journeyDS0);

// Format to one-line JSON string
l_line := RECORD
  STRING line
END;
journeyDS := PROJECT(journeyDS0,
                      TRANSFORM(l_line,
                                SELF.line := '{' + (STRING)TOJSON(ROW({LEFT.vehicle_id, LEFT.journey_id},l_JSON-way_points)) + ',' +  LEFT.way_points + '}'
                                ), LOCAL);
OUTPUT(journeyDS,{journeyDS},Telematics.Files.PATH_PREFIX + '::koln_JSON', OVERWRITE, COMPRESSED);

//OUTPUT CSV file
// OUTPUT(journeyDS,,'~thor::telematics_4millions.csv', CSV , OVERWRITE, COMPRESSED);
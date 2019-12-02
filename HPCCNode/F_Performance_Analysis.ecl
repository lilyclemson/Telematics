l_time := RECORD
  STRING vehicle_id;        // vehicle identifier
  UNSIGNED4 journey_id;     // journey identifier per vehicle
  UNSIGNED4 duration;       // duration (second) per journey
  UNSIGNED4 HB;             // Hard-break events per journey
  UNSIGNED4 HA;             // Hard-acceleration events per journey
  DECIMAL9_6 highestSpeed;  // Highest speed per journey
  DECIMAL9_6 speedingratio; // (speeding time)/(journey duration)
  REAL time_sent;
  REAL time_processed;
  REAL messagelife;
END;
ds := DATASET('~telematics::superfile', l_time, THOR);
OUTPUT(ds, NAMED('report'));

minLife := MIN(ds, messagelife);
maxLife := MAX(ds, messagelife);
avgLife := AVE(ds, messagelife);
varLife := VARIANCE(ds, messagelife);
SDLife := SQRT(VARIANCE(ds, messagelife));

OUTPUT(minLife, NAMED('minLife'));
OUTPUT(maxLife, NAMED('maxLife'));
OUTPUT(avgLife, NAMED('avgLife'));
OUTPUT(varLife, NAMED('VARLife'));
OUTPUT(SDLife, NAMED('SDLife'));
IMPORT LevinVehicleTelematics as LV;
IMPORT LV.E_Data_Ingestion;
IMPORT Std;
IMPORT Visualizer;


Layout_tripDevice_acceleration := RECORD
	$.E_Data_Ingestion.raw;
	INTEGER id := 0;
	DECIMAL7_4 accelerationGps := 0;//acceleration rate
	DECIMAL7_2 accelerationRpm := 0;
	DECIMAL7_4 moving_ave_accelerationGps:=0;// Every five acceleration to caculate the average value to make the curve more smoothly 
	INTEGER2 AccelerationGpsType:=0;//Threadhold.If moving_ave_accelerationGps>1, acceleration,AccelerationGpsType=4; 1> moving_ave_accelerationGps >-1  omit,AccelerationGpsType=0;moving_ave_accelerationGps<-1, brake,AccelerationGpsType=4
    INTEGER2 accelerationPoint:=0;//The accleration point in each trip 
    INTEGER2 brakePoint:=0;//The brake point in each trip 
END;
tripDeviceAccelerationTable := TABLE($.E_Data_Ingestion.raw,Layout_tripDevice_acceleration);

Layout_tripDevice_acceleration AccelerationRecs(Layout_tripDevice_acceleration L,Layout_tripDevice_acceleration R) := TRANSFORM
	SELF.id :=if(L.tripID != R.tripID OR L.deviceID != R.deviceID,1,L.id+1);
	SELF.accelerationGps :=if(L.tripID != R.tripID OR L.deviceID != R.deviceID ,L.accelerationGps,R.gps_speed-L.gps_speed);
	SELF.accelerationRpm :=if(L.tripID != R.tripID OR L.deviceID != R.deviceID ,L.accelerationRpm,R.rpm-L.rpm);
	SELF := R;
END;

//Get the GPS acceleration 
tripDeviceAccelerationTableEnhance := ITERATE(tripDeviceAccelerationTable,AccelerationRecs(LEFT,RIGHT),LOCAL);
//tripDeviceAccelerationTableEnhance;

//tripDeviceAccelerationTableEnhanceFilter := tripDeviceAccelerationTableEnhance;
tripDeviceAccelerationTableEnhanceFilter := tripDeviceAccelerationTableEnhance((deviceID=0 OR deviceID=5) AND (tripID=1 OR tripID=2));
tripDeviceAccelerationTableEnhanceFilter;
COUNT(tripDeviceAccelerationTableEnhanceFilter);

//Add moving averages of the GPS acceleration
//Add Acceleration Type(Acceleration, brake, omit) by moving_ave_accelerationGps. Threadhold.If moving_ave_accelerationGps>1, acceleration,AccelerationGpsType=4; 1> moving_ave_accelerationGps >-1  omit,AccelerationGpsType=0;moving_ave_accelerationGps<-1, brake,AccelerationGpsType=4
withMovingAve := DENORMALIZE(
                tripDeviceAccelerationTableEnhance,
                tripDeviceAccelerationTableEnhance,
                RIGHT.id > 0
                    AND RIGHT.id BETWEEN (LEFT.id - 5) AND (LEFT.id - 1)
                    AND LEFT.deviceID = RIGHT.deviceID AND LEFT.tripID = RIGHT.tripID,
                GROUP,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        moving_ave_accelerationGps := IF(COUNT(ROWS(RIGHT)) = 5, AVE(ROWS(RIGHT), accelerationGps), 0);
                        SELF.moving_ave_accelerationGps := moving_ave_accelerationGps;
                        AccelerationGpsType := IF(moving_ave_accelerationGps>1,4,if(moving_ave_accelerationGps<-1,-4,0));
                        SELF.AccelerationGpsType :=AccelerationGpsType;
                        SELF := LEFT
                    ),
                ALL
            );
withMovingAve;

Layout_tripDevice_acceleration withMovingAveTypeAccelerationRecs(Layout_tripDevice_acceleration L, Layout_tripDevice_acceleration R) := TRANSFORM
  SELF.accelerationPoint := if(L.AccelerationGpsType=0,if(R.AccelerationGpsType=4,1,0),0);// accelerate 
  SELF.brakePoint := if(L.AccelerationGpsType=0,if(R.AccelerationGpsType=-4,1,0),0);// brake 
  SELF := R;
END;

//Add acceleration and brake point by AccelerationGpsType
withMovingAveTypeAcceleration := ITERATE(withMovingAve,withMovingAveTypeAccelerationRecs(LEFT,RIGHT));
OUTPUT(withMovingAveTypeAcceleration);

withMovingAveTypeAccelerationResTypeSum:=RECORD
    withMovingAveTypeAcceleration.deviceID;
    withMovingAveTypeAcceleration.tripID;
    accelerationTotal:=SUM(GROUP,withMovingAveTypeAcceleration.accelerationPoint);
    brakeTotal:=SUM(GROUP,withMovingAveTypeAcceleration.brakePoint);
    maxSpeed:=MAX(GROUP,withMovingAveTypeAcceleration.gps_speed);
    aveSpeed:=(DECIMAL7_4)AVE(GROUP,withMovingAveTypeAcceleration.gps_speed);
    maxAcceleration:=MAX(GROUP,withMovingAveTypeAcceleration.accelerationGps);
    maxBrake:=MIN(GROUP,withMovingAveTypeAcceleration.accelerationGps);
    totalTime := COUNT(GROUP);
    seatBelt := if(RANDOM()%2 !=0,True, False);
END;
//Add acceleration Total and brake Total by accelerationPoint and brakePoint
//Add Maximum Speed, Average Speed
//Add Maximum Acceleration,Brake
//Add Total time and seat belt
//For each trip
withMovingAveTypeAccelerationSummary:= TABLE(withMovingAveTypeAcceleration,withMovingAveTypeAccelerationResTypeSum,deviceID,tripID);
withMovingAveTypeAccelerationSummary;
count(withMovingAveTypeAccelerationSummary);

withMovingAveTypeAccelerationResTypeSumDevice:=RECORD
    withMovingAveTypeAccelerationSummary.deviceID;
    accelerationTotal:=SUM(GROUP,withMovingAveTypeAccelerationSummary.accelerationTotal);
    brakeTotal:=SUM(GROUP,withMovingAveTypeAccelerationSummary.brakeTotal);
    maxSpeed:=MAX(GROUP,withMovingAveTypeAccelerationSummary.maxSpeed);
    aveSpeed:=(DECIMAL7_4)AVE(GROUP,withMovingAveTypeAccelerationSummary.aveSpeed);
    maxAcceleration:=MAX(GROUP,withMovingAveTypeAccelerationSummary.maxAcceleration);
    maxBrake:=MIN(GROUP,withMovingAveTypeAccelerationSummary.maxBrake);
    totalTrip := COUNT(GROUP);
    totalTime := SUM(GROUP,withMovingAveTypeAccelerationSummary.totalTime);
    avgAccelerationPerHour:= (DECIMAL7_4) ((DECIMAL7_4)SUM(GROUP,withMovingAveTypeAccelerationSummary.accelerationTotal)/SUM(GROUP,withMovingAveTypeAccelerationSummary.totalTime)) *3600;
    avgBrakePerHour:= (DECIMAL7_4)((DECIMAL7_4)SUM(GROUP,withMovingAveTypeAccelerationSummary.brakeTotal)/SUM(GROUP,withMovingAveTypeAccelerationSummary.totalTime)) *3600;
END;
//Add acceleration Total and brake Total 
//Add Maximum Speed, Average Speed
//Add Maximum Acceleration,Brake
//Add Total time
//For each device
withMovingAveTypeAccelerationSummaryByDevice:= TABLE(withMovingAveTypeAccelerationSummary,withMovingAveTypeAccelerationResTypeSumDevice,deviceID);
//withMovingAveTypeAccelerationSummaryByDevice;

OUTPUT(withMovingAveTypeAccelerationSummaryByDevice, NAMED('withMovingAveTypeAccelerationSummaryByDevice'));
withMovingAveTypeAccelerationSummaryByDeviceMappings := DATASET([{'Device ID','deviceID'},{'accelerationTotal','accelerationTotal'},{'brakeTotal','brakeTotal'},{'totalTime','totalTime'}],Visualizer.KeyValueDef);

Visualizer.MultiD.area('withMovingAveTypeAccelerationSummaryByDeviceChart', /*datasource*/, 'withMovingAveTypeAccelerationSummaryByDevice', withMovingAveTypeAccelerationSummaryByDeviceMappings, /*filteredBy*/, /*dermatologyProperties*/ );

withMovingAveTypeAccelerationSummaryByDeviceMappings2 := DATASET([{'Device ID','deviceID'},{'maxSpeed','maxSpeed'},{'aveSpeed','aveSpeed'},{'maxAcceleration','maxAcceleration'},{'maxBrake','maxBrake'},{'avgAccelerationPerHour','avgAccelerationPerHour'},{'avgBrakePerHour','avgBrakePerHour'}],Visualizer.KeyValueDef);

//Visualizer.MultiD.area('withMovingAveTypeAccelerationSummaryByDeviceChart2', /*datasource*/, 'withMovingAveTypeAccelerationSummaryByDevice', withMovingAveTypeAccelerationSummaryByDeviceMappings2, /*filteredBy*/, /*dermatologyProperties*/ );

Visualizer.MultiD.Column('withMovingAveTypeAccelerationSummaryByDeviceChart2', /*datasource*/, 'withMovingAveTypeAccelerationSummaryByDevice', withMovingAveTypeAccelerationSummaryByDeviceMappings2, /*filteredBy*/, /*dermatologyProperties*/ );

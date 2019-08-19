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
    INTEGER2 accelerationCount:=0;//The total number of accleration in each trip 
    INTEGER2 brakeCount:=0;//The total number of brake in each trip 
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
                        SELF.moving_ave_accelerationGps := IF(COUNT(ROWS(RIGHT)) = 5, AVE(ROWS(RIGHT), accelerationGps), 0),
                        SELF := LEFT
                    ),
                ALL
            );
withMovingAve;

//Add Acceleration Type(Acceleration, brake, omit) by moving_ave_accelerationGps. Threadhold.If moving_ave_accelerationGps>1, acceleration,AccelerationGpsType=4; 1> moving_ave_accelerationGps >-1  omit,AccelerationGpsType=0;moving_ave_accelerationGps<-1, brake,AccelerationGpsType=4
withMovingAveType := DENORMALIZE(
                withMovingAve,
                withMovingAve,
                RIGHT.id > 0,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.AccelerationGpsType := IF(LEFT.moving_ave_accelerationGps>1,4,if(LEFT.moving_ave_accelerationGps<-1,-4,0)),
                        SELF := LEFT
                    ),
                ALL
            );
withMovingAveType;

Layout_tripDevice_acceleration withMovingAveTypeAccelerationRecs(Layout_tripDevice_acceleration L, Layout_tripDevice_acceleration R) := TRANSFORM
  SELF.accelerationCount := if(L.AccelerationGpsType=0,if(R.AccelerationGpsType=4,1,0),0);// accelerate 
  SELF.brakeCount := if(L.AccelerationGpsType=0,if(R.AccelerationGpsType=-4,1,0),0);// brake 
  SELF := R;
END;

//Add acceleration and brake point by AccelerationGpsType
withMovingAveTypeAcceleration := ITERATE(withMovingAveType,withMovingAveTypeAccelerationRecs(LEFT,RIGHT));
OUTPUT(withMovingAveTypeAcceleration);


withMovingAveTypeAccelerationResTypeSum:=RECORD
    withMovingAveTypeAcceleration.deviceID;
    withMovingAveTypeAcceleration.tripID;
    accelerationTotal:=SUM(GROUP,withMovingAveTypeAcceleration.accelerationCount);
    brakeTotal:=SUM(GROUP,withMovingAveTypeAcceleration.brakeCount);
    maxSpeed:=MAX(GROUP,withMovingAveTypeAcceleration.gps_speed);
    aveSpeed:=(DECIMAL7_4)AVE(GROUP,withMovingAveTypeAcceleration.gps_speed);
    maxAcceleration:=MAX(GROUP,withMovingAveTypeAcceleration.accelerationGps);
    maxBrake:=MIN(GROUP,withMovingAveTypeAcceleration.accelerationGps);
    totalTime := COUNT(GROUP);
    seatBelt := if(RANDOM()%2 !=0,True, False);
END;
//Add acceleration Total and brakeCount Total by accelerationCount and brakeCount
//Add Maximum Speed, Average Speed
//Add Maximum Acceleration,Brake
//Add Total time and seat belt
//For each trip
withMovingAveTypeAccelerationSummary:= TABLE(withMovingAveTypeAcceleration,withMovingAveTypeAccelerationResTypeSum,deviceID,tripID);
withMovingAveTypeAccelerationSummary;


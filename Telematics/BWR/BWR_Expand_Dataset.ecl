IMPORT Telematics;
IMPORT Std;
IMPORT * FROM LanguageExtensions;

#WORKUNIT('name', 'Expand Cleaned Dataset');
#OPTION('pickBestEngine', FALSE);

//------------------------------------------------------------------------------

IDMapLayout := RECORD
    STRING          old_vehicle_id;
    STRING          new_vehicle_id;
END;

baseFile := DISTRIBUTE(Telematics.Files.KOLN.CleanData.FILE, HASH32(vehicle_id));

uniqueVehicleIDs := DISTRIBUTE(TABLE(baseFile, {vehicle_id}, vehicle_id, MERGE), HASH32(vehicle_id)) : INDEPENDENT;

DupBaseFile(DATASET(RECORDOF(baseFile)) ds,
            DATASET(RECORDOF(uniqueVehicleIDs)) idList,
            STRING newIDPrefix = 'A') := FUNCTION
    idMap := PROJECT
        (
            idList,
            TRANSFORM
                (
                    {
                        STRING  old_id,
                        STRING  new_id
                    },
                    SELF.old_id := LEFT.vehicle_id,
                    SELF.new_id := newIDPrefix + INTFORMAT(COUNTER, 6, 1)
                )
        );
    
    newDS := JOIN
        (
            ds,
            idMap,
            LEFT.vehicle_id = RIGHT.old_id,
            TRANSFORM
                (
                    RECORDOF(LEFT),
                    SELF.vehicle_id := RIGHT.new_id,
                    SELF := LEFT
                ),
            LOCAL, KEEP(1)
        );
    
    RETURN newDS;
END;

expandedDS := baseFile
                + DupBaseFile(baseFile, uniqueVehicleIDs, 'A')
                + DupBaseFile(baseFile, uniqueVehicleIDs, 'B')
                + DupBaseFile(baseFile, uniqueVehicleIDs, 'C')
                + DupBaseFile(baseFile, uniqueVehicleIDs, 'D')
                + DupBaseFile(baseFile, uniqueVehicleIDs, 'E');

OUTPUT
    (
        expandedDS,
        {expandedDS},
        Telematics.Files.KOLN.ExpandedData.PATH,
        OVERWRITE, COMPRESSED
    );

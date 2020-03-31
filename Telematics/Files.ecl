IMPORT Std;

EXPORT Files := MODULE

    EXPORT PATH_PREFIX := '~telematics';

    EXPORT KOLN := MODULE

        EXPORT KOLN_PREFIX := PATH_PREFIX + '::koln';

        EXPORT RawData := MODULE

            EXPORT LAYOUT := RECORD
                STRING          time_offset;    // seconds
                STRING          vehicle_id;
                STRING          x_pos;          // meters
                STRING          y_pos;          // meters
                STRING          speed;          // meters per second
            END;

            EXPORT PATH := KOLN_PREFIX + '::koln.tr';

            EXPORT FILE := DATASET(PATH, LAYOUT, CSV(SEPARATOR(' ')));
        END;

        EXPORT CleanData := MODULE

            EXPORT LAYOUT := RECORD
                UNSIGNED4       time_offset;    // seconds
                STRING          vehicle_id;
                DECIMAL11_6     x_pos;          // meters
                DECIMAL11_6     y_pos;          // meters
                DECIMAL9_6      speed;          // meters per second
            END;

            EXPORT PATH := KOLN_PREFIX + '::koln';

            EXPORT FILE := DATASET(PATH, LAYOUT, FLAT);
        END;

        EXPORT ExpandedData := MODULE

            EXPORT LAYOUT := RECORD
                UNSIGNED4       time_offset;    // seconds
                STRING          vehicle_id;
                DECIMAL11_6     x_pos;          // meters
                DECIMAL11_6     y_pos;          // meters
                DECIMAL9_6      speed;          // meters per second
            END;

            EXPORT PATH := KOLN_PREFIX + '::koln_expanded';

            EXPORT FILE := DATASET(PATH, LAYOUT, FLAT);
        END;
    END;

END;

IMPORT Telematics;
IMPORT Std;
IMPORT * FROM LanguageExtensions;

#WORKUNIT('name', 'Expanded Data: Profile');
#OPTION('pickBestEngine', FALSE);

//------------------------------------------------------------------------------

IMPORT DataPatterns;

profile := DataPatterns.Profile
    (
        Telematics.Files.KOLN.ExpandedData.FILE,
        fieldListStr := 'vehicle_id',
        features := 'fill_rate,cardinality,patterns'
    );
Dbg(profile, ALL);

IMPORT Telematics;
IMPORT Std;
IMPORT * FROM LanguageExtensions;

#WORKUNIT('name', 'Raw Data: Profile');
#OPTION('pickBestEngine', FALSE);

//------------------------------------------------------------------------------

IMPORT DataPatterns;

profile := DataPatterns.Profile(Telematics.Files.KOLN.RawData.FILE);
Dbg(profile, ALL);

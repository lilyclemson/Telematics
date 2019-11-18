IMPORT Telematics;
IMPORT Std;
IMPORT * FROM LanguageExtensions;

#WORKUNIT('name', 'Raw Data: Clean');
#OPTION('pickBestEngine', FALSE);

//------------------------------------------------------------------------------

IMPORT DataPatterns;

rawDataNewLayout := DATASET
    (
        Telematics.Files.KOLN.RawData.PATH,
        Telematics.Files.KOLN.CleanData.LAYOUT,
        CSV(SEPARATOR(' '))
    );

OUTPUT
    (
        rawDataNewLayout,
        {rawDataNewLayout},
        Telematics.Files.KOLN.CleanData.PATH,
        OVERWRITE, COMPRESSED
    );

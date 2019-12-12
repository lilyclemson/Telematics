IMPORT $.Types AS Types;
IMPORT Std;

superFileName := '~' + Types.Topic + '::superfile';

Std.File.ClearSuperFile(superFileName, del := TRUE);

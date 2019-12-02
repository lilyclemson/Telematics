IMPORT Telematics;
IMPORT kafka;
IMPORT STD;
IMPORT STD.Date;
IMPORT HPCCNode.Types AS Types;
IMPORT HPCCNode.Utils;


raw := Utils.consumer(Types.Topic, Types.broker);
parsed := Utils.parseMsg(raw);
cleaned := Utils.clean(parsed);
cronjob := Utils.maintenance(cleaned, Types.Topic).updateSuperFile;
// Update superFile every second by scheduling CRON job
cronjob : WHEN ( CRON ( '* * * * *' ) );
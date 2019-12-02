IMPORT HPCCNode.Utils;
IMPORT HPCCNode.Types;


/**
  * Generate report for the performance of our pipeline.
  * It includes stats of the superFile of the choosen topic, such as 
  * the MAX, MIN, AVG, VARIANCE, SD of the messageLife.
  */
report := Utils.analysis(Types.TOPIC);
OUTPUT(report);
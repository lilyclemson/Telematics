
EXPORT Types := MODULE

EXPORT KafkaMessageFormat  := RECORD
  UNSIGNED4 partitionNum;
  INTEGER8 offset;
  STRING message;
END;

END;
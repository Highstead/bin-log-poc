#bin-log-poc
This is a bin log proof of concept i'm working on to nicely get bin-logs into kafka.  This is the first step to a deeper problem where objects are moved between a databases in a sharded ecosystem.

##Todo
- Productionize siddontang/go-mysql to allow for a different logger
- Verify I can nicely restart the binlogger 
- Put data into Kafka
  -  It is expected that because kafka is gauranteed delivery but not gaurnteed once that some de-duping will need to occur



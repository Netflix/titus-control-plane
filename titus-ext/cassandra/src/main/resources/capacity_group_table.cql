CREATE TABLE "TitusApplicationSLA" (
  key text,
  column1 text,
  value text,
  PRIMARY KEY ((key), column1)
) WITH
  comment='Titus application SLA' AND
  compression={} AND
  compact storage;

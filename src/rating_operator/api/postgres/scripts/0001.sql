
CREATE TABLE namespaces (
  namespace   VARCHAR(253) NOT NULL,
  tenant_id   VARCHAR(253) NOT NULL,
  PRIMARY KEY (namespace)
);

CREATE TABLE namespace_status (
  namespace   VARCHAR(253) NOT NULL,
  last_update TIMESTAMP NOT NULL,
  PRIMARY KEY (namespace)
);

CREATE TABLE frames (
  frame_begin  TIMESTAMP NOT NULL,
  frame_end    TIMESTAMP NOT NULL,
  namespace    VARCHAR(253) NOT NULL,
  node         VARCHAR(253) NOT NULL,
  metric       VARCHAR(253) NOT NULL,
  pod          VARCHAR(253) NOT NULL,
  quantity     DOUBLE PRECISION NOT NULL,
  frame_price  DOUBLE PRECISION,
  matched_rule VARCHAR,
  PRIMARY KEY  (frame_begin, namespace, node, pod, metric)
);

CREATE TABLE frames_copy (
  frame_begin  TIMESTAMP,
  frame_end    TIMESTAMP,
  namespace    VARCHAR(253),
  node         VARCHAR(253),
  metric       VARCHAR(253),
  pod          VARCHAR(253),
  quantity     DOUBLE PRECISION,
  frame_price  DOUBLE PRECISION,
  matched_rule VARCHAR
);

CREATE TABLE frame_status (
  report_name   VARCHAR(253) NOT NULL,
  metric        VARCHAR(253) NOT NULL,
  last_insert   TIMESTAMP,
  PRIMARY KEY (report_name, metric)
);

CREATE TABLE users (
  tenant_id   VARCHAR(253) NOT NULL,
  password    VARCHAR(253) NOT NULL,
  PRIMARY KEY (tenant_id)
);

CREATE INDEX ON frames (pod);

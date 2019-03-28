CREATE TABLE IF NOT EXISTS default.graphite_reverse  ( 
  Path String,  
  Value Float64,  
  Time UInt32,  
  Date Date,  
  Timestamp UInt32
) ENGINE = GraphiteMergeTree(Date, (Path, Time), 8192, 'graphite_rollup');

CREATE TABLE IF NOT EXISTS default.graphite_tree (
  Date Date,
  Level UInt32,
  Path String,
  Deleted UInt8,
  Version UInt32
) ENGINE = ReplacingMergeTree(Date, (Level, Path), 1024, Version);

CREATE TABLE IF NOT EXISTS default.graphite_series_reverse (
  Date Date,
  Level UInt32,
  Path String,
  Deleted UInt8,
  Version UInt32
) ENGINE = ReplacingMergeTree(Date, (Level, Path, Date), 1024, Version);

CREATE TABLE IF NOT EXISTS default.graphite_tagged (
  Date Date,
  Tag1 String,
  Path String,
  Tags Array(String),
  Version UInt32,
  Deleted UInt8
) ENGINE = ReplacingMergeTree(Date, (Tag1, Path, Date), 1024, Version);
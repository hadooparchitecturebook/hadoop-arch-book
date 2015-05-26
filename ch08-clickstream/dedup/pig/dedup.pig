rawlogs = LOAD '$raw_log_dir';
dedupedlogs = DISTINCT rawlogs;
STORE dedupedlogs INTO '$deduped_log_dir' USING PigStorage();
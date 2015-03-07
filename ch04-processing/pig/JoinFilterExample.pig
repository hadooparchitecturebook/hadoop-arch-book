fooOriginal = LOAD 'foo/foo.txt' USING PigStorage('|') AS (fooId:long, fooVal:int, barId:long);

fooFiltered = FILTER fooOriginal BY (fooVal <= 500);

barOriginal = LOAD 'bar/bar.txt' USING PigStorage('|') AS (barId:long, barVal:int);

joinedValues = JOIN fooFiltered BY barId, barOriginal BY barId;

joinedFiltered = FILTER joinedValues BY (fooVal + barVal <= 500);

STORE joinedFiltered INTO 'pig/out' USING PigStorage ('|');

DESCRIBE joinedFiltered;
EXPLAIN joinedFiltered;


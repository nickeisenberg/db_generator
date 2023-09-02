## Tables
The following tables will be generated after following the instructions below.


### ohlcv 
`show columns from ohlcv;` 

| Field     | Type       | Null | Key | Default | Extra |
|-----------|------------|------|-----|---------|-------|
| datetime  | datetime   | NO   | PRI | NULL    |       |
| ticker    | varchar(5) | NO   | PRI | NULL    |       |
| open      | float      | YES  |     | NULL    |       |
| high      | float      | YES  |     | NULL    |       |
| low       | float      | YES  |     | NULL    |       |
| close     | float      | YES  |     | NULL    |       |
| volume    | float      | YES  |     | NULL    |       |
| timestamp | int        | YES  |     | NULL    |       |



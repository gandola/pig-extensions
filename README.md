# pig-extensions
Utilities for Apache Pig.

- **MD5**: UDF that generates a MD5 and returns the hexadecimal value including leading zeros.
- **CSVExcelStorageWithPath**: [CSVExcelStorage](http://pig.apache.org/docs/r0.12.0/api/org/apache/pig/piggybank/storage/CSVExcelStorage.html) extension that prepends the full file path to each tuple.

##Examples

```java
REGISTER pig-extensions-1.0.jar;

DEFINE CSVLoader pg.hadoop.pig.piggybank.CSVExcelStorageWithPath();
B = LOAD '$INPUT' USING CSVLoader AS (file_path:chararray, ...);
```

```java
REGISTER pig-extensions-1.0.jar;

DEFINE MD5 pg.hadoop.pig.MD5();
...
B = FOREACH A GENERATE MD5(my_string) as md5_str;
...
```

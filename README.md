# pig-extensions
Utilities for Apache Pig.

- **MD5**: UDF that generates a MD5 and returns the hexadecimal value including leading zeros.
- **CSVExcelStorageWithPath**: [CSVExcelStorage](http://pig.apache.org/docs/r0.12.0/api/org/apache/pig/piggybank/storage/CSVExcelStorage.html) extension that prepends the full file path to each tuple.
- **ExtendedMultiStorage**: [MultiStorage](http://pig.apache.org/docs/r0.12.0/api/org/apache/pig/piggybank/storage/MultiStorage.html) allows the removal of the key field from the output files.

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

```java
REGISTER pig-extensions-1.0.jar;
...
B = FOREACH A GENERATE uid, createdAt, url;
STORE B INTO '$OUTPUT' USING pg.hadoop.pig.piggybank.ExtendedMultiStorage('$OUTPUT', '0', 'none', ',', 'false');

/**
Output includes only createdAt and url, example:
2016-01-10,http://mywebsite.com
2016-01-11,http://mywebsite1.com
...
*/
```

## Purpose

Create simple tests using Apache Avro.

## How to compile `Pair.avsc`

Suppose that `avro-tools-1.6.2.jar` is in `$AVRO_HOME`.

On the project home, run the following command:

```java -jar $AVRO_HOME/avro-tools-1.6.2.jar compile schema src/main/resources/Pair.avsc src/main/java/```

## Tests

Run unit tests in `src/test/java/org/sooo/AvroTest`.

## References
[Example source code accompanying O'Reilly's "Hadoop: The Definitive Guide" by Tom White](https://github.com/tomwhite/hadoop-book/tree/master/avro)
[Apache Avro](http://avro.apache.org/)
* BIT/ENUM transformation from TiDB to Postgres
** BIT
   BIT -> bytea(java) -> binary string -> Postgres BIT
** ENUM

* Compilation
#+BEGIN_SRC
OhMyTiUP$ mvn compile
OhMyTiUP$ mvn deploy
OhMyTiUP$ ls target/TiDBTransform-1.0-SNAPSHOT.jar
target/TiDBTransform-1.0-SNAPSHOT.jar
#+END_SRC
Copy the jar file to kafka plugin directory and restart the connect to load the jar file.

* BIT Example 
** TiDB Table Definition
#+BEGIN_SRC sql
CREATE TABLE `test01` (
  `pk_col` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,
  `t_bit` bit(1) DEFAULT NULL,
  `tidb_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,
  `t_bit02` bit(10) DEFAULT NULL,
PRIMARY KEY (`pk_col`) /*T![clustered_index] CLUSTERED */
#+END_SRC
** Postgres Table Definition
#+BEGIN_SRC sql
  CREATE TABLE `test01` (
    `pk_col` bigint(20) NOT NULL ,
    `t_bit` bit(1) DEFAULT NULL,
    `tidb_timestamp` timestamp,
    `pg_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,
    `t_bit02` bit(10) DEFAULT NULL,
  PRIMARY KEY (`pk_col`);
#+END_SRC
** JDBC Sink connect configuration
#+BEGIN_SRC
{
    "name": "JDBCTEST",
    "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "connection.url": "jdbc:postgresql://PostgresServerIP:5432/test?stringtype=unspecified",
        "connection.user": "TestUser",
        "connection.password": "TestPassword",
        "topics": "test_test01",
        "insert.mode": "upsert",
        "dialect.name":"PostgreSqlDatabaseDialect",
        "table.name.format":"test01",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://172.83.1.52:8081",
        "value.converter.schema.registry.url": "http://172.83.1.52:8081",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
        "pk.mode": "record_key",
        "auto.create":"false",
        "auto.evolve":"false",
        "transforms": "timestamp, Cast, TiDBCast",
        "transforms.timestamp.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
        "transforms.timestamp.target.type": "Timestamp",
        "transforms.timestamp.field":"timestamp,t_datetime",
        "transforms.timestamp.format": "yyyy-MM-dd HH:mm:ss",
        "transforms.Cast.type": "org.apache.kafka.connect.transforms.Cast$Value",
        "transforms.Cast.spec": "t_bool:boolean",
        "transforms.TiDBCast.type": "com.github.luyomo.kafka.connect.transform.TiDBTransform$Value",
        "transforms.TiDBCast.field.name": "t_bit,t_bit02",
        "transforms.TiDBCast.field.type": "BIT,BIT",
        "transforms.TiDBCast.field.length": "1,10"
    }
}
#+END_SRC

* SET
#+BEGIN_SRC
  CREATE TYPE mood AS ENUM ('a', 'b', 'c');
alter table test01 add column t_set mood default null;
#+END_SRC

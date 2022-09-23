/*
 * Copyright ©20229Jay(syunka.tyo@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.luyomo.kafka.connect.transform;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class TiDBTransform<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger logger = LoggerFactory.getLogger(TiDBTransform.class);

  public static final String OVERVIEW_DOC =
    "Cast the data type from TiDB to downstream";

  private interface ConfigName {
    String FIELD_NAME = "field.name";
    String CAST_TYPE = "cast.type";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field name for conversion");
//    .define(ConfigName.CAST_TYPE, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Conversion type");

  private static final String PURPOSE = "Convert TiDB data type to downstream";

  private String fieldName;
//  private String convType;

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    logger.info("********** 02. into the configure function");
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    fieldName = config.getString(ConfigName.FIELD_NAME);
    // convType = config.getString(ConfigName.CAST_TYPE);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }


  @Override
  public R apply(R record) {
//    logger.info("********** 02. This is the test message for logging <{}>", record.toString());
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

    final Map<String, Object> updatedValue = new HashMap<>(value);

    updatedValue.put(fieldName, getRandomUuid());

    return newRecord(record, null, updatedValue);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);
    logger.info("********** 02. The schema from applyWithSchema <{}>", value.schema().toString());
    logger.info("***** 001. The value is {}", value);

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    logger.info("****** 002.  The schema 0001 <{}>", updatedSchema);
    if(updatedSchema == null) {
      logger.info("002.01 The schema here is {}", value.schema()); 
      updatedSchema = makeUpdatedSchema(value.schema());
      logger.info("***** 003. The schema before add: <{}>", updatedSchema.toString());
      schemaUpdateCache.put(value.schema(), updatedSchema);
      logger.info("***** 004. The schema into cache add: <{}>", schemaUpdateCache.toString());
    }

    logger.info("********** 005. The data before add value <{}>", updatedSchema.toString());
    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
      logger.info("***** 006. The field name is <{}>", field.name());
      if(field.name().equals("t_bit"))  {
          logger.info("Setting the data as bit");
          logger.info("The data from the original field {} and {} ", field.name(), value.get(field));
          updatedValue.put(field.name(), "1");
          logger.info("Reached the data set");
      }else{
          updatedValue.put(field.name(), value.get(field));
      }

    }

    logger.info("********** 008. The data before add value <{}>", updatedValue.toString());
    updatedValue.put(fieldName, getRandomUuid());
    logger.info("********** 02. This is the test message after data update <{}>", updatedValue.toString());

    return newRecord(record, updatedSchema, updatedValue);
  }

  @Override
  public ConfigDef config() { 
    logger.info("********** 02. into the config function");
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private String getRandomUuid() {
    return UUID.randomUUID().toString();
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
      logger.info("The field name is: <{}>, schema is: <{}>", field.name(), field.schema());
      if (field.name().equals("t_bit") ) {
          builder.field(field.name(), Schema.STRING_SCHEMA);
      } else {
          builder.field(field.name(), field.schema());
      }
    }

    builder.field(fieldName, Schema.STRING_SCHEMA);

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends TiDBTransform<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends TiDBTransform<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}



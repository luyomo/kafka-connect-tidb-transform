/*
 * Copyright ©20229Jay (syunka.tyo@gmail.com)
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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.nio.ByteBuffer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TiDBTransformTest {

  private TiDBTransform<SourceRecord> xform = new TiDBTransform.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  @Test(expected = DataException.class)
  public void topLevelStructRequired() {
    xform.configure(Collections.singletonMap("field.name", "t_bit"));
    xform.configure(Collections.singletonMap("field.length", "10"));
    xform.configure(Collections.singletonMap("cast.type", "BIT"));
    xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
  }

  @Test
  public void copySchemaAndTiDBField() {
    final Map<String, Object> props = new HashMap<>();
    System.out.println("\n\n\n\nHello world in the copySchemaAndTiDBField \n\n\n");

    // java.nio.HeapByteBuffer[pos=0 lim=1 cap=1]
    ByteBuffer buffer = ByteBuffer.allocate(1);
    System.out.println("---->position:\t" + buffer.position() + "\n---->limit:\t" + buffer.limit() + "\n---->capacity:\t" + buffer.capacity());
    buffer.put("h".getBytes());
    System.out.println("---->position:\t" + buffer.position() + "\n---->limit:\t" + buffer.limit() + "\n---->capacity:\t" + buffer.capacity());


    props.put("field.name", "t_bit");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
    System.out.printf("\n Schema name %s \n ", simpleStructSchema.name());
    System.out.printf(" Doc:        %s \n ", simpleStructSchema.doc());
    System.out.printf(" Version:    %s \n ", simpleStructSchema.version());
    System.out.printf(" Fields:     %s \n ", simpleStructSchema.fields());
    System.out.printf(" One Field:  %s \n ", simpleStructSchema.field("magic").schema());
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);
    System.out.printf(" The data is : %s \n", simpleStruct.toString());

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
    assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
    assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("t_bit").schema());
    assertNotNull(((Struct) transformedRecord.value()).getString("t_bit"));

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }

  @Test
  public void schemalessTiDBField() {
    final Map<String, Object> props = new HashMap<>();

    props.put("field.name", "t_bit");

    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
      null, Collections.singletonMap("magic", 42L));

    final SourceRecord transformedRecord = xform.apply(record);
    assertEquals(42L, ((Map) transformedRecord.value()).get("magic"));
    assertNotNull(((Map) transformedRecord.value()).get("t_bit"));

  }
}

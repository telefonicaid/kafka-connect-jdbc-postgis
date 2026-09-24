/**
 * Copyright 2016 Confluent Inc.
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
 **/

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimestampIncrementingTableQuerierTest {

  private static final Timestamp INITIAL_TS = new Timestamp(71);
  private static final long INITIAL_INC = 4761;
  private static final List<String> TIMESTAMP_COLUMNS = Arrays.asList("ts1", "ts2");
  private static final String INCREMENTING_COLUMN = "inc";

  @Mock
  private PreparedStatement stmt;
  @Mock
  private ResultSet resultSet;
  @Mock
  private Connection db;
  @Mock
  private ExpressionBuilder expressionBuilder;
  @Mock
  private TimestampIncrementingCriteria criteria;
  @Mock
  private SchemaMapping schemaMapping;
  private DatabaseDialect dialect;

  private MockedStatic<SchemaMapping> schemaMappingStatic;
  private OngoingStubbing<Boolean> resultSetNextStubbing;
  private OngoingStubbing<TimestampIncrementingOffset> extractValuesStubbing;

  @Before
  public void setUp() {
    dialect = mock(DatabaseDialect.class);
    schemaMappingStatic = mockStatic(SchemaMapping.class);
  }

  @After
  public void tearDown() {
    schemaMappingStatic.close();
  }

  private TimestampIncrementingTableQuerier querier(
      TimestampIncrementingOffset initialOffset,
      boolean timestampMode
  ) {
    final String tableName = "table";
    when(dialect.parseTableIdentifier(tableName)).thenReturn(new TableId("", "", tableName));

    return new TimestampIncrementingTableQuerier(
        dialect,
        TableQuerier.QueryMode.TABLE,
        tableName,
        "",
        timestampMode ? TIMESTAMP_COLUMNS : null,
        INCREMENTING_COLUMN,
        initialOffset.toMap(),
        10211197100L, // Timestamp delay
        ZoneId.of("UTC"),
        "",
        JdbcSourceConnectorConfig.TimestampGranularity.CONNECT_LOGICAL,
        false
    );
  }

  private Schema schema() {
    SchemaBuilder result =SchemaBuilder.struct();
    result.field(INCREMENTING_COLUMN, Schema.INT64_SCHEMA);
    TIMESTAMP_COLUMNS.forEach(
        col -> result.field(col, org.apache.kafka.connect.data.Timestamp.builder().build())
    );
    return result.build();
  }

  private void expectNewQuery() throws Exception {
    when(dialect.createPreparedStatement(eq(db), any())).thenReturn(stmt);
    when(dialect.expressionBuilder()).thenReturn(expressionBuilder);
    when(dialect.criteriaFor(any(), any())).thenReturn(criteria);
    when(stmt.executeQuery()).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(null);
    when(schemaMapping.schema()).thenReturn(schema());
    when(schemaMapping.fieldSetters()).thenReturn(Collections.emptyList());
    schemaMappingStatic.when(() -> SchemaMapping.create(any(), any(), any())).thenReturn(schemaMapping);
  }

  private void stubResultSetNext(boolean hasNext) throws SQLException {
    if (resultSetNextStubbing == null) {
      resultSetNextStubbing = when(resultSet.next()).thenReturn(hasNext);
    } else {
      resultSetNextStubbing = resultSetNextStubbing.thenReturn(hasNext);
    }
  }

  private void stubExtractValues(TimestampIncrementingOffset offset) throws Exception {
    if (extractValuesStubbing == null) {
      extractValuesStubbing = when(criteria.extractValues(any(), any(), any(), any())).thenReturn(offset);
    } else {
      extractValuesStubbing = extractValuesStubbing.thenReturn(offset);
    }
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(offset(INITIAL_TS, INITIAL_INC), false);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    assertFalse(querier.next());
  }

  @Test
  public void testTimestampAndIncrementingMode() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    TimestampIncrementingOffset firstNewOffset = offset(firstNewTimestamp, INITIAL_INC + 1);
    TimestampIncrementingOffset secondNewOffset = offset(new Timestamp(INITIAL_TS.getTime() + 2), INITIAL_INC + 2);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(offset(INITIAL_TS, INITIAL_INC), false);
    expectRecord(firstNewOffset);
    expectRecord(firstNewOffset);
    expectRecord(secondNewOffset);
    expectRecord(secondNewOffset);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    // We just commit timestamp offsets immediately in this mode since the incrementing column
    // provides an additional layer of granularity; as long as there aren't two updates to the same
    // row that take place with the same timestamp, no data loss should occur
    assertNextRecord(querier, firstNewOffset);
    assertNextRecord(querier, firstNewOffset);
    assertNextRecord(querier, secondNewOffset);
    assertNextRecord(querier, secondNewOffset);

    assertFalse(querier.next());
  }

  @Test
  public void testIncrementingMode() throws Exception {
    TimestampIncrementingOffset firstNewOffset = offset(INITIAL_INC + 1);
    TimestampIncrementingOffset secondNewOffset = offset(INITIAL_INC + 2);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(offset(INITIAL_INC), true);
    expectRecord(firstNewOffset);
    expectRecord(firstNewOffset);
    expectRecord(secondNewOffset);
    expectRecord(secondNewOffset);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    // We commit offsets immediately in this mode
    assertNextRecord(querier, firstNewOffset);
    assertNextRecord(querier, firstNewOffset);
    assertNextRecord(querier, secondNewOffset);
    assertNextRecord(querier, secondNewOffset);

    assertFalse(querier.next());
  }

  @Test
  public void testMultipleSingleRecordResultSets() throws Exception {
    TimestampIncrementingOffset initialOffset = offset(INITIAL_TS, INITIAL_INC);
    expectNewQuery();
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(initialOffset, true);
    expectRecord(initialOffset);
    stubResultSetNext(false);
    expectRecord(initialOffset);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    assertNextRecord(querier, initialOffset);

    assertFalse(querier.next());

    querier.reset(0, true);
    querier.maybeStartQuery(db);

    assertNextRecord(querier, initialOffset);

    assertFalse(querier.next());
  }

  private void assertNextRecord(
      TimestampIncrementingTableQuerier querier, TimestampIncrementingOffset offset
  ) throws Exception {
    assertTrue(querier.next());
    assertEquals(offset.toMap(), querier.extractRecord().sourceOffset());
  }

  private void expectRecord(TimestampIncrementingOffset offset) throws Exception {
    stubResultSetNext(true);
    stubExtractValues(offset);
  }

  private static TimestampIncrementingOffset offset(Long inc) {
    return new TimestampIncrementingOffset(null, inc);
  }

  private static TimestampIncrementingOffset offset(Timestamp ts, Long inc) {
    return new TimestampIncrementingOffset(ts, inc);
  }
}

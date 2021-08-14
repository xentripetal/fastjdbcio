package io.xentripetal.beam.fastjdbcio;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WithDistinctPartitionsTests {
    @Rule
    public TestPipeline p = TestPipeline.create();

    SerializableFunction<Void, DataSource> testDbProvider = new SerializableFunction<Void, DataSource>() {
        @Override
        public DataSource apply(Void input) {
            Path resourceDirectory = Paths.get("src", "test", "resources", "test.db");
            String dbPath = resourceDirectory.toFile().getAbsolutePath();

            SQLiteDataSource dataSource = new SQLiteDataSource();
            dataSource.setUrl("jdbc:sqlite:" + dbPath);
            dataSource.setDatabaseName("main");
            return dataSource;
        }
    };

    Schema partitionSchema = Schema.builder()
            .addInt32Field("id")
            .addStringField("group_a")
            .addInt32Field("group_b")
            .addFloatField("value")
            .build();

    FastJdbcIO.RowMapper<Row> mapper = new FastJdbcIO.RowMapper<Row>() {
        @Override
        public Row mapRow(ResultSet resultSet) throws Exception {
            List<Object> values = new ArrayList<>();
            values.add(resultSet.getInt(1));
            values.add(resultSet.getString(2));
            values.add(resultSet.getInt(3));
            values.add(resultSet.getFloat(4));
            return Row.withSchema(partitionSchema).addValues(values).build();
        }
    };

    @Test
    public void SinglePartition() {
        FastJdbcIO.ReadWithDistinctPartitions<Row> reader = FastJdbcIO.<Row>readWithDistinctPartitions()
                .withPartitionColumns(Collections.singletonList("group_a"))
                .withJdbcOptions(FastJdbcIO.JdbcOptions.<Row>create()
                        .withSqlElements(FastJdbcIO.SqlElements.create("main.partitions"))
                        .withDataSourceProviderFn(testDbProvider)
                        .withRowMapper(mapper)
                        .withCoder(RowCoder.of(partitionSchema)));
        PCollection<Row> results = p.apply(reader);
        PCollection<Long> size = results.apply(Count.globally());
        PAssert.that(size).containsInAnyOrder(20L);
        p.run();
    }

}

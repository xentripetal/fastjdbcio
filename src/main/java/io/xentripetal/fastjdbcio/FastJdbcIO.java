package io.xentripetal.fastjdbcio;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

/**
 * Input source for faster JDBC reading
 * <p>
 * This library is just a wrapper around JdbcIO with SQL generation for parallel querying.
 * It does not support any method for faster writing at this time.
 */
public class FastJdbcIO {
    private Logger LOG = LoggerFactory.getLogger(FastJdbcIO.class);

    /**
     * Like {@link org.apache.beam.sdk.io.jdbc.JdbcIO#read}, but gathers all distinct values of the specified
     * columns and runs a query for each value set.
     *
     * @param <T> Type of the data to be read.
     */
    public static <T> ReadWithDistinctPartitions<T> readWithDistinctPartitions() {
        return null;
    }

    public static <T> JdbcOptions<T> jdbcOptions() {
        return null;
    }

    static final InferableFunction<Row, KV<String, Row>> ToStaticKey = new InferableFunction<Row, KV<String, Row>>() {
        @Override
        public KV<String, Row> apply(Row input) throws Exception {
            return KV.of("a", input);
        }
    };

    /**
     * Allows for overriding the final queries used in FastJdbcIO.
     */
    public static SqlElements sqlElements() {
        return null;
    }

    @AutoValue
    public abstract static class SqlElements {
        abstract @Nullable String getSelectStatement();

        abstract @Nullable String getPredicateExtension();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setSelectStatement(String selectStatement);

            abstract Builder setPredicateExtension(String predicateExtension);

            abstract SqlElements build();
        }


        /**
         * Sets the select statement the final query used in the reader that this is passed to.
         * <p>
         * e.g. if the reader uses <code>"SELECT * FROM schema.table"</code>
         * and you specify "a, b AS fixed_b" as the <code>selectStatement</code>
         * it will produce <code>"SELECT a, b AS fixed_b FROM schema.table"</code>
         *
         * @param selectStatement The select statement to use, defaults to *
         */
        public SqlElements withSelectStatement(String selectStatement) {
            checkNotNull(selectStatement, "Select statement can not be null, either don't set it or use * for default.");
            return toBuilder().setSelectStatement(selectStatement).build();
        }

        /**
         * Appends this predicate to the final query used in the reader this is passed to. It will be added on
         * as an AND to any predicates that the query is using for implementation. You do not need to specify wrapping
         * parenthesis.
         * <p>
         * e.g. if the reader uses <code>"SELECT * FROM schema.table WHERE a = 'value'"</code>
         * and you specify "b > 5" as the <code>predicateExtension</code>
         * it will produce <code>"SELECT * FROM schema.table WHERE (a = 'value') AND (b > 5)"</code>
         *
         * @param predicateExtension predicate sql to extend with. Do not use empty string, instead use null for not using
         *                           any extension.
         */
        public SqlElements withPredicateExtension(String predicateExtension) {
            checkArgument(predicateExtension == null || predicateExtension.length() > 0, "Predicate Extension should not be an empty string. Set to null to not use.");
            return toBuilder().setPredicateExtension(predicateExtension).build();
        }
    }

    @AutoValue
    public abstract static class JdbcOptions<T> {
        abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

        abstract JdbcIO.RowMapper<T> getRowMapper();

        abstract SqlElements getSqlElements();

        abstract @Nullable Coder<T> getCoder();

        abstract boolean getOutputParallelization();

        abstract @Nullable String getTable();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {

            abstract Builder<T> setDataSourceProviderFn(SerializableFunction<Void, DataSource> dataSourceProviderFn);

            abstract Builder<T> setRowMapper(JdbcIO.RowMapper<T> rowMapper);

            abstract Builder<T> setSqlElements(SqlElements sqlElements);

            abstract Builder<T> setCoder(Coder<T> coder);

            abstract Builder<T> setTable(String tableName);

            abstract Builder<T> setOutputParallelization(boolean outputParallelization);

            abstract JdbcOptions<T> build();
        }

        public JdbcIO.ReadRows toReadRow() {
            return JdbcIO.readRows()
                    .withDataSourceProviderFn(getDataSourceProviderFn())
                    .withOutputParallelization(getOutputParallelization());
        }

        public <V> JdbcIOExtensions.ReadAll<V, T>toReadAll() {
            return JdbcIOExtensions.<V, T>readAll()
                    .withDataSourceProviderFn(getDataSourceProviderFn())
                    .withCoder(getCoder())
                    .withRowMapper(getRowMapper())
                    .withOutputParallelization(getOutputParallelization());
        }


        public void verifyProperties() {
            checkNotNull(getSqlElements(), "SqlElements is required.");
            checkNotNull(getRowMapper(), "withRowMapper() is required");
            checkNotNull(getCoder(), "withCoder() is required");
            checkNotNull(getDataSourceProviderFn(), "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
            checkNotNull(getTable(), "withTable() is required");
        }

        public JdbcOptions<T> withSqlElements(final SqlElements sqlElements) {
            return toBuilder().setSqlElements(sqlElements).build();
        }

        public JdbcOptions<T> withDataSourceConfiguration(final JdbcIO.DataSourceConfiguration config) {
            checkNotNull(config, "SqlElements can not be null");
            return toBuilder().setDataSourceProviderFn(JdbcIO.DataSourceProviderFromDataSourceConfiguration.of(config)).build();
        }

        public JdbcOptions<T> withDataSourceProviderFn(
                SerializableFunction<Void, DataSource> dataSourceProviderFn) {
            return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
        }

        public JdbcOptions<T> withRowMapper(JdbcIO.RowMapper<T> rowMapper) {
            checkNotNull(rowMapper, "rowMapper can not be null");
            return toBuilder().setRowMapper(rowMapper).build();
        }

        public JdbcOptions<T> withCoder(Coder<T> coder) {
            checkNotNull(coder, "coder can not be null");
            return toBuilder().setCoder(coder).build();
        }

        @Override
        public String toString() {
            return String.format("JdbcOptions: {Coder: %1$s, Table: %2$s, RowMapper: %3$s, DataSource: %4$s, OutputParallelization: %5$s",
                    getCoder().getClass().getName(),
                    getTable(),
                    getRowMapper().getClass().getName(),
                    getDataSourceProviderFn().getClass().getName(),
                    getOutputParallelization());
        }
    }

    /**
     * Implementation of {@link #readWithDistinctPartitions}.
     */
    @AutoValue
    public abstract static class ReadWithDistinctPartitions<T> extends PTransform<PBegin, PCollection<T>> {

        abstract int getNumPartitions();

        abstract @Nullable List<String> getPartitionColumns();

        abstract int getSetSize();

        abstract JdbcOptions<T> getJdbcOptions();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setPartitionColumns(List<String> partitionColumns);

            abstract Builder<T> setJdbcOptions(JdbcOptions<T> jdbcOptions);

            abstract Builder<T> setSetSize(int setSize);

            abstract ReadWithDistinctPartitions<T> build();
        }

        /**
         * The number of distinct value sets per query. When set to 1, a query will be run for every combination of
         * values for the set partition columns.
         */
        public ReadWithDistinctPartitions<T> withJdbcOptions(JdbcOptions<T> jdbcOptions) {
            checkNotNull(jdbcOptions, "JdbcOptions can not be null");
            jdbcOptions.verifyProperties();
            return toBuilder().setJdbcOptions(jdbcOptions).build();
        }


        /**
         * The number of distinct value sets per query. When set to 1, a query will be run for every combination of
         * values for the set partition columns.
         */
        public ReadWithDistinctPartitions<T> withSetSize(int setSize) {
            checkArgument(setSize > 0, "setSize cannot be less than 1");
            return toBuilder().setSetSize(setSize).build();
        }

        /**
         * List of the name of columns to use for partitioning by value
         */
        public ReadWithDistinctPartitions<T> withPartitionColumns(List<String> partitionColumn) {
            checkNotNull(partitionColumn, "partitionColumn can not be null");
            checkArgument(partitionColumn.size() > 0, "You must specify at least one partition column");
            return toBuilder().setPartitionColumns(partitionColumn).build();
        }


        @Override
        public PCollection<T> expand(PBegin input) {
            checkNotNull(getJdbcOptions(), "withJdbcOptions() is required");
            JdbcOptions<T> opts = getJdbcOptions();
            opts.verifyProperties();
            checkNotNull(getPartitionColumns(), "withPartitionColumns() is required");
            checkArgument(getPartitionColumns().size() == 0, "Partition Columns must have at least one element");
            checkArgument(getSetSize() > 0, "SetSize must be greater than 0");


            PCollection<Row> distinctValues = input.apply("Read distinct values", opts.toReadRow()
                    .withQuery(String.format("SELECT DISTINCT %1$s FROM %2$s GROUP BY %1$s",
                            SqlUtilities.toColumnSelect(getPartitionColumns()), opts.getTable())));

            PCollection<KV<String, Iterable<Row>>> batches = distinctValues.apply("Add Key", MapElements.via(FastJdbcIO.ToStaticKey))
                    .apply(GroupIntoBatches.ofSize(getSetSize()));


            return batches.apply("Read batches", opts.toReadAll()
                            .withQueryGetter(new JdbcIOExtensions.QueryGetter<T>() {
                                @Override
                                public String getQuery(T element) throws Exception {
                                    return null;
                                }
                            })
                            .withQuery(
                                    String.format(
                                            "select * from %1$s where %2$s >= ? and %2$s < ?",
                                            getTable(), getPartitionColumn()))
                            .withParameterSetter(
                                    (PreparedStatementSetter<KV<String, Iterable<Integer>>>)
                                            (element, preparedStatement) -> {
                                                String[] range = element.getKey().split(",", -1);
                                                preparedStatement.setInt(1, Integer.parseInt(range[0]));
                                                preparedStatement.setInt(2, Integer.parseInt(range[1]));
                                            })
                            .withOutputParallelization(false));
        }


        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("jdbcOptions", getJdbcOptions().toString()));
            builder.add(DisplayData.item("partitionColumns", getPartitionColumns().toString()));
            builder.add(DisplayData.item("numPartitions", getNumPartitions()));
        }
    }

    @AutoValue
    public class QueryOverrides {

    }

}

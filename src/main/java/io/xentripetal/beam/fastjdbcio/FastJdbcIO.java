package io.xentripetal.beam.fastjdbcio;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        return new AutoValue_FastJdbcIO_ReadWithDistinctPartitions.Builder<T>()
                .setSetSize(1)
                .setJdbcOptions(JdbcOptions.create())
                .build();
    }

    /**
     * An interface used by {@link FastJdbcIO} for converting each row of the {@link ResultSet} into
     * an element of the resulting {@link PCollection}.
     */
    @FunctionalInterface
    public interface RowMapper<T> extends JdbcIO.RowMapper<T> {
    }


    @AutoValue
    public abstract static class SqlElements {
        /**
         * Properties for which table to query and overriding queries used.
         */
        public static SqlElements create(ValueProvider<String> table) {
            return new AutoValue_FastJdbcIO_SqlElements.Builder()
                    .setSelectStatement(ValueProvider.StaticValueProvider.of("*"))
                    .setTable(table)
                    .build();
        }

        /**
         * Properties for which table to query and overriding queries used.
         */
        public static SqlElements create(String table) {
            return new AutoValue_FastJdbcIO_SqlElements.Builder()
                    .setSelectStatement(ValueProvider.StaticValueProvider.of("*"))
                    .setTable(ValueProvider.StaticValueProvider.of(table))
                    .build();
        }

        abstract @Nullable ValueProvider<String> getSelectStatement();

        abstract @Nullable ValueProvider<String> getTable();

        abstract @Nullable ValueProvider<String> getWhereExtension();

        abstract @Nullable ValueProvider<String> getPrefixSql();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {

            public abstract Builder setSelectStatement(ValueProvider<String> selectStatement);

            public abstract Builder setTable(ValueProvider<String> table);

            public abstract Builder setWhereExtension(ValueProvider<String> whereExtension);

            public abstract Builder setPrefixSql(ValueProvider<String> prefixSql);

            abstract SqlElements build();
        }

        public void verifyProperties() {
            checkNotNull(getSelectStatement(), "Select statement can not be null, either don't set it or use * for default.");
            checkNotNull(getTable(), "Table cannot be null");
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
            return withSelectStatement(ValueProvider.StaticValueProvider.of(selectStatement));
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
        public SqlElements withSelectStatement(ValueProvider<String> selectStatement) {
            return toBuilder().setSelectStatement(selectStatement).build();
        }

        /**
         * Sets the table to query. It can also be a subquery as the table parameter will simply be inserted after the
         * FROM clause
         * <p>
         * e.g. if the reader uses <code>"SELECT * FROM "</code>
         * and you specify "(SELECT A FROM schema.table)" as the <code>table</code>
         * it will produce <code>"SELECT * FROM (SELECT A FROM schema.table)"</code>
         *
         * @param table The sql to place in the FROM clause.
         */
        public SqlElements withTable(String table) {
            checkNotNull(table, "Table cannot be null");
            return withTable(ValueProvider.StaticValueProvider.of(table));
        }


        /**
         * Sets the table to query. It can also be a subquery as the table parameter will simply be inserted after the
         * FROM clause
         * <p>
         * e.g. if the reader uses <code>"SELECT * FROM "</code>
         * and you specify "(SELECT A FROM schema.table)" as the <code>table</code>
         * it will produce <code>"SELECT * FROM (SELECT A FROM schema.table)"</code>
         *
         * @param table The sql to place in the FROM clause.
         */
        public SqlElements withTable(ValueProvider<String> table) {
            checkNotNull(table, "Table cannot be null");
            return toBuilder().setTable(table).build();
        }

        /**
         * Appends this clause to the final query used in the reader this is passed to. It will be added on
         * as an AND to any predicates that the query is using for implementation. You do not need to specify wrapping
         * parenthesis.
         * <p>
         * e.g. if the reader uses <code>"SELECT * FROM schema.table WHERE a = 'value'"</code>
         * and you specify "b > 5" as the <code>predicateExtension</code>
         * it will produce <code>"SELECT * FROM schema.table WHERE (a = 'value') AND (b > 5)"</code>
         *
         * @param whereExtension predicate sql to extend with.
         */
        public SqlElements withWhereExtension(String whereExtension) {
            return withWhereExtension(ValueProvider.StaticValueProvider.of(whereExtension));
        }

        /**
         * Appends this clause to the final query used in the reader this is passed to. It will be added on
         * as an AND to any predicates that the query is using for implementation. You do not need to specify wrapping
         * parenthesis.
         * <p>
         * e.g. if the reader uses <code>"SELECT * FROM schema.table WHERE a = 'value'"</code>
         * and you specify "b > 5" as the <code>predicateExtension</code>
         * it will produce <code>"SELECT * FROM schema.table WHERE (a = 'value') AND (b > 5)"</code>
         *
         * @param whereExtension predicate sql to extend with.
         */
        public SqlElements withWhereExtension(ValueProvider<String> whereExtension) {
            return toBuilder().setWhereExtension(whereExtension).build();
        }

        /**
         * Prefixes the provided prefixSql onto the generated query used by the reader this is passed to.
         * <p>
         * This allows for running CTE blocks with custom logic, simply change the table to be the name of whichever
         * CTE block you are wanting to read from.
         * </p><p>
         * e.g. if the reader uses <code>"SELECT * FROM schema.table</code>
         * and you specify "with example as (SELECT * FROM another.table)" as the <code>prefixSql</code> and "example" as the <code>table</code>
         * it will produce <code>"with example as (SELECT * FROM another.table) SELECT * FROM example</code>
         *
         * @param prefixSql SQL to insert before generated sql
         */
        public SqlElements withPrefixSql(String prefixSql) {
            return withPrefixSql(ValueProvider.StaticValueProvider.of(prefixSql));
        }


        /**
         * Prefixes the provided prefixSql onto the generated query used by the reader this is passed to.
         * <p>
         * This allows for running CTE blocks with custom logic, simply change the table to be the name of whichever
         * CTE block you are wanting to read from.
         * </p><p>
         * e.g. if the reader uses <code>"SELECT * FROM schema.table</code>
         * and you specify "with example as (SELECT * FROM another.table)" as the <code>prefixSql</code> and "example" as the <code>table</code>
         * it will produce <code>"with example as (SELECT * FROM another.table) SELECT * FROM example</code>
         *
         * @param prefixSql SQL to insert before generated sql
         */
        public SqlElements withPrefixSql(ValueProvider<String> prefixSql) {
            return toBuilder().setPrefixSql(prefixSql).build();
        }

        protected ValueProvider<String> generateSql(ValueProvider<String> selectorOverride, ValueProvider<String> whereClause, ValueProvider<String> postfix) {
            StringBuilderValueProvider query = StringBuilderValueProvider.of();
            if (getPrefixSql() != null) {
                query.append(getPrefixSql());
                query.append(" ");
            }
            query.append("SELECT ");
            if (selectorOverride != null) {
                query.append(selectorOverride);
            } else {
                query.append(getSelectStatement());
            }

            query.append(" FROM ");
            query.append(getTable());

            StringBuilderValueProvider clause = StringBuilderValueProvider.of();
            if (whereClause != null) {
                clause.append(" WHERE ");
                clause.append(whereClause);
                if (getWhereExtension() != null) {
                    clause.insert(0, "(");
                    clause.append(") AND (");
                    clause.append(getWhereExtension());
                    clause.append(")");
                }
            } else if (getWhereExtension() != null) {
                clause.append(" WHERE ");
                clause.append(getWhereExtension());
            }

            query.append(clause);

            if (postfix != null) {
                query.append(" ");
                query.append(postfix);
            }

            return query;
        }
    }

    @AutoValue
    public abstract static class JdbcOptions<T> implements Serializable {
        public static <T> JdbcOptions<T> create() {
            return new AutoValue_FastJdbcIO_JdbcOptions.Builder<T>()
                    .setOutputParallelization(true)
                    .build();
        }

        public static <T> JdbcOptions<T> create(String table) {
            return new AutoValue_FastJdbcIO_JdbcOptions.Builder<T>()
                    .setOutputParallelization(true)
                    .setSqlElements(SqlElements.create(table))
                    .build();
        }

        public static <T> JdbcOptions<T> create(ValueProvider<String> table) {
            return new AutoValue_FastJdbcIO_JdbcOptions.Builder<T>()
                    .setOutputParallelization(true)
                    .setSqlElements(SqlElements.create(table))
                    .build();
        }

        abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

        abstract @Nullable RowMapper<T> getRowMapper();

        abstract @Nullable SqlElements getSqlElements();

        abstract @Nullable Coder<T> getCoder();

        abstract boolean getOutputParallelization();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {

            abstract Builder<T> setDataSourceProviderFn(SerializableFunction<Void, DataSource> dataSourceProviderFn);

            abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

            abstract Builder<T> setSqlElements(SqlElements sqlElements);

            abstract Builder<T> setCoder(Coder<T> coder);

            abstract Builder<T> setOutputParallelization(boolean outputParallelization);

            abstract JdbcOptions<T> build();
        }

        public JdbcIO.ReadRows toReadRow() {
            return JdbcIO.readRows()
                    .withDataSourceProviderFn(getDataSourceProviderFn())
                    .withOutputParallelization(getOutputParallelization());
        }

        public <V> JdbcIO.ReadAll<V, T> toReadAll() {
            return JdbcIO.<V, T>readAll()
                    .withDataSourceProviderFn(getDataSourceProviderFn())
                    .withCoder(getCoder())
                    .withRowMapper(getRowMapper())
                    .withOutputParallelization(getOutputParallelization());
        }


        public void verifyProperties() {
            checkNotNull(getSqlElements(), "SqlElements is required.");
            getSqlElements().verifyProperties();
            checkNotNull(getRowMapper(), "withRowMapper() is required");
            checkNotNull(getCoder(), "withCoder() is required");
            checkNotNull(getDataSourceProviderFn(), "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
        }

        public JdbcOptions<T> withSqlElements(final SqlElements sqlElements) {
            checkNotNull(sqlElements, "SqlElements can not be null");
            return toBuilder().setSqlElements(sqlElements).build();
        }

        public JdbcOptions<T> withDataSourceConfiguration(final JdbcIO.DataSourceConfiguration config) {
            checkNotNull(config, "withDataSourceConfiguration can not be null");
            return toBuilder().setDataSourceProviderFn(JdbcIO.DataSourceProviderFromDataSourceConfiguration.of(config)).build();
        }

        public JdbcOptions<T> withDataSourceProviderFn(
                SerializableFunction<Void, DataSource> dataSourceProviderFn) {
            checkNotNull(dataSourceProviderFn, "withDataSourceProviderFn can not be null");
            return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
        }

        public JdbcOptions<T> withRowMapper(RowMapper<T> rowMapper) {
            checkNotNull(rowMapper, "rowMapper can not be null");
            return toBuilder().setRowMapper(rowMapper).build();
        }

        public JdbcOptions<T> withCoder(Coder<T> coder) {
            checkNotNull(coder, "coder can not be null");
            return toBuilder().setCoder(coder).build();
        }

        @Override
        public String toString() {
            return String.format("JdbcOptions: {Coder: %1$s, RowMapper: %2$s, DataSource: %3$s, OutputParallelization: %4$s, SqlElements: %5$s}",
                    getCoder().getClass().getName(),
                    getRowMapper().getClass().getName(),
                    getDataSourceProviderFn().getClass().getName(),
                    getOutputParallelization(),
                    getSqlElements());
        }
    }

    /**
     * Implementation of {@link #readWithDistinctPartitions}.
     */
    @AutoValue
    public abstract static class ReadWithDistinctPartitions<T> extends PTransform<PBegin, PCollection<T>> {

        abstract @Nullable ValueProvider<List<String>> getPartitionColumns();

        abstract long getSetSize();

        abstract JdbcOptions<T> getJdbcOptions();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setPartitionColumns(ValueProvider<List<String>> partitionColumns);

            abstract Builder<T> setJdbcOptions(JdbcOptions<T> jdbcOptions);

            abstract Builder<T> setSetSize(long setSize);

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
        public ReadWithDistinctPartitions<T> withSetSize(long setSize) {
            checkArgument(setSize > 0, "setSize cannot be less than 1");
            return withSetSize(setSize);
        }


        /**
         * List of the name of columns to use for partitioning by value
         */
        public ReadWithDistinctPartitions<T> withPartitionColumns(List<String> partitionColumns) {
            checkNotNull(partitionColumns, "partitionColumns can not be null");
            checkArgument(partitionColumns.size() > 0, "You must specify at least one partition column");
            return withPartitionColumns(ValueProvider.StaticValueProvider.of(partitionColumns));
        }

        /**
         * List of the name of columns to use for partitioning by value
         */
        public ReadWithDistinctPartitions<T> withPartitionColumns(ValueProvider<List<String>> partitionColumns) {
            checkNotNull(partitionColumns, "partitionColumns can not be null");
            return toBuilder().setPartitionColumns(partitionColumns).build();
        }

        /**
         * Builds the sql for querying each set of partitions.
         *
         * @return sql
         */
        private ValueProvider<String> generateSql() {

            SerializableFunction<List<String>, String> clauseBuilder = new SerializableFunction<List<String>, String>() {
                @Override
                public String apply(List<String> partitionColumns) {
                    // Build the base clause of COLUMN_A = ? AND COLUMN_B = ? AND etc...
                    StringBuilder partitionClausesBuilder = new StringBuilder();
                    for (int i = 0; i < partitionColumns.size(); i++) {
                        partitionClausesBuilder.append(partitionColumns.get(i) + " = ?");
                        if (i < partitionColumns.size() - 1) {
                            partitionClausesBuilder.append(" AND ");
                        }
                    }
                    String partitionClauses = partitionClausesBuilder.toString();

                    long setSize = getSetSize();
                    // Duplicate the partition clause for each entry in the set
                    StringBuilder clause = new StringBuilder();
                    if (setSize > 1) {
                        for (int i = 0; i < setSize; i++) {
                            clause.append("(" + partitionClauses + ")");
                            if (i < setSize - 1) {
                                clause.append(" OR ");
                            }
                        }
                    } else {
                        // Don't add extra parenthesis. Its probably fine to just leave them in there but I've dealt with too
                        // many DB2 databases to trust anything.
                        clause.append(partitionClauses);
                    }
                    return clause.toString();
                }
            };

            ValueProvider<String> clause = ValueProvider.NestedValueProvider.of(getPartitionColumns(), clauseBuilder);
            return getJdbcOptions().getSqlElements().generateSql(null, clause, null);
        }

        ValueProvider<String> generatePartitionValueSql() {
            ValueProvider<String> columnNames = ValueProvider.NestedValueProvider.of(getPartitionColumns(), new SerializableFunction<List<String>, String>() {
                @Override
                public String apply(List<String> input) {
                    return SqlUtilities.toColumnSelect(input);
                }
            });

            return getJdbcOptions().getSqlElements().generateSql(
                    StringBuilderValueProvider.of("DISTINCT ").append(columnNames),
                    null,
                    StringBuilderValueProvider.of("GROUP BY ").append(columnNames));
        }

        class DistinctParameterSetter implements PreparedStatementSetter<KV<String, Iterable<Row>>> {
            @Override
            public void setParameters(KV<String, Iterable<Row>> element, PreparedStatement preparedStatement) throws Exception {
                Iterable<Row> values = element.getValue();
                int i = 1;
                for (Row row : values) {
                    for (int j = 0; j < row.getFieldCount(); j++)
                        preparedStatement.setObject(i++, row.getBaseValue(j));
                }
            }
        }

        protected PCollection<Row> distinctValues;
        protected PCollection<KV<String, Iterable<Row>>> batches;

        @Override
        public PCollection<T> expand(PBegin input) {
            checkNotNull(getJdbcOptions(), "withJdbcOptions() is required");
            JdbcOptions<T> opts = getJdbcOptions();
            opts.verifyProperties();
            checkNotNull(getPartitionColumns(), "Partition Columns can not be null");
            checkNotNull(getSetSize(), "setSize can not be null");


            distinctValues = input.apply("Read distinct values", opts.toReadRow()
                    .withQuery(generatePartitionValueSql()));

            batches = distinctValues.apply("Add Key", MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.rows()))
                            .via(r -> KV.of("a", r))).setCoder(KvCoder.of(StringUtf8Coder.of(), distinctValues.getCoder()))
                    .apply(GroupIntoBatches.ofSize(getSetSize()))
                    .setCoder(KvCoder.of(
                            StringUtf8Coder.of(),
                            IterableCoder.of(distinctValues.getCoder())
                    ));

            return batches.apply("Read batches", opts.<KV<String, Iterable<Row>>>toReadAll()
                    .withQuery(generateSql())
                    .withParameterSetter(new DistinctParameterSetter()));
        }


        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("jdbcOptions", getJdbcOptions().toString()));
            builder.add(DisplayData.item("partitionColumns", getPartitionColumns().toString()));
            builder.add(DisplayData.item("setSize", getSetSize()));
        }
    }

}

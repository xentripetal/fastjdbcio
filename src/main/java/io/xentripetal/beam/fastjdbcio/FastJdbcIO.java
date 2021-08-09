package io.xentripetal.beam.fastjdbcio;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
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
import java.sql.PreparedStatement;
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
                .setJdbcOptions(jdbcOptions())
                .build();
    }

    public static <T> JdbcOptions<T> jdbcOptions() {
        return new AutoValue_FastJdbcIO_JdbcOptions.Builder<T>()
                .setOutputParallelization(false)
                .build();
    }

    /**
     * Allows for overriding the final queries used in FastJdbcIO.
     */
    public static SqlElements sqlElements() {
        return new AutoValue_FastJdbcIO_SqlElements.Builder()
                .setSelectStatement("*")
                .build();
    }

    static final InferableFunction<Row, KV<String, Row>> ToStaticKey = new InferableFunction<Row, KV<String, Row>>() {
        @Override
        public KV<String, Row> apply(Row input) throws Exception {
            return KV.of("a", input);
        }
    };

    @AutoValue
    public abstract static class SqlElements {
        abstract @Nullable String getSelectStatement();

        abstract @Nullable String getTable();

        abstract @Nullable String getWhereExtension();

        abstract @Nullable String getPrefixSql();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {

            public abstract Builder setSelectStatement(@Nullable String newSelectStatement);

            public abstract Builder setTable(@Nullable String newTable);

            public abstract Builder setWhereExtension(@Nullable String newWhereExtension);

            public abstract Builder setPrefixSql(@Nullable String newPrefixSql);

            abstract SqlElements build();
        }

        public void verifyProperties() {
            checkNotNull(getSelectStatement(), "Select statement can not be null, either don't set it or use * for default.");
            checkNotNull(getTable(), "withTable() is required");
            checkArgument(!Objects.equals(getWhereExtension(), ""), "Predicate Extension should not be an empty string. Set to null to not use.");
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
            checkNotNull(table, "Select statement can not be null, either don't set it or use * for default.");
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
         * @param whereExtension predicate sql to extend with. Do not use empty string, instead use null for not using
         *                       any extension.
         */
        public SqlElements withWhereExtension(String whereExtension) {
            checkArgument(whereExtension == null || whereExtension.length() > 0, "Predicate Extension should not be an empty string. Set to null to not use.");
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
            return toBuilder().setPrefixSql(prefixSql).build();
        }

        protected String generateSql(String selectorOverride, String whereClause, String postfix) {
            StringBuilder query = new StringBuilder();
            String selector = selectorOverride;
            if (selector == null) {
                selector = getSelectStatement();
            }
            if (getPrefixSql() != null) {
                query.append(getPrefixSql());
                query.append(" ");
            }

            query.append("SELECT ");
            query.append(selector);
            query.append(" FROM ");
            query.append(getTable());


            // This is kind of messy. Maybe make a graph based model for representing SQL if we have to keep
            // doing stuff like this.
            StringBuilder clause = new StringBuilder();
            if (whereClause != null) {
                clause.append(whereClause);
            }
            if (getWhereExtension() != null) {
                if (clause.length() > 0) {
                    clause.insert(0, '(');
                    clause.append(") AND ");
                }
                clause.append(getWhereExtension());
            }
            if (clause.length() > 0) {
                query.append(" WHERE ");
                query.append(clause);
            }

            if (postfix != null) {
                query.append(" ");
                query.append(postfix);
            }

            return query.toString();
        }

        @Override
        public String toString() {
            return generateSql(null, null, null);
        }

    }

    @AutoValue
    public abstract static class JdbcOptions<T> {
        abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

        abstract JdbcIO.RowMapper<T> getRowMapper();

        abstract SqlElements getSqlElements();

        abstract @Nullable Coder<T> getCoder();

        abstract boolean getOutputParallelization();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {

            abstract Builder<T> setDataSourceProviderFn(SerializableFunction<Void, DataSource> dataSourceProviderFn);

            abstract Builder<T> setRowMapper(JdbcIO.RowMapper<T> rowMapper);

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

        /**
         * Builds the sql for querying each set of partitions.
         *
         * @return sql
         */
        private String generateSql() {
            List<String> partitionColumns = getPartitionColumns();

            // Build the base clause of COLUMN_A = ? AND COLUMN_B = ? AND etc...
            StringBuilder partitionClausesBuilder = new StringBuilder();
            for (int i = 0; i < partitionColumns.size(); i++) {
                partitionClausesBuilder.append(partitionColumns.get(i) + " = ?");
                if (i < partitionColumns.size() - 1) {
                    partitionClausesBuilder.append(" AND ");
                }
            }
            String partitionClauses = partitionClausesBuilder.toString();

            // Duplicate the partition clause for each entry in the set
            StringBuilder clause = new StringBuilder();
            if (getSetSize() > 1) {
                for (int i = 0; i < getSetSize(); i++) {
                    clause.append("(" + partitionClauses + ")");
                    if (i < getSetSize() - 1) {
                        clause.append(" OR ");
                    }
                }
            } else {
                // Don't add extra parenthesis. Its probably fine to just leave them in there but I've dealt with too
                // many DB2 databases to trust anything.
                clause.append(partitionClauses);
            }


            return getJdbcOptions().getSqlElements().generateSql(null, clause.toString(), null);
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


        @Override
        public PCollection<T> expand(PBegin input) {
            checkNotNull(getJdbcOptions(), "withJdbcOptions() is required");
            JdbcOptions<T> opts = getJdbcOptions();
            opts.verifyProperties();
            checkNotNull(getPartitionColumns(), "withPartitionColumns() is required");
            checkArgument(getPartitionColumns().size() == 0, "Partition Columns must have at least one element");
            checkArgument(getSetSize() > 0, "SetSize must be greater than 0");


            String columnNames = SqlUtilities.toColumnSelect(getPartitionColumns());
            PCollection<Row> distinctValues = input.apply("Read distinct values", opts.toReadRow()
                    .withQuery(opts.getSqlElements().generateSql("DISTINCT " + columnNames, null, "GROUP BY " + columnNames)));

            PCollection<KV<String, Iterable<Row>>> batches = distinctValues.apply("Add Key", MapElements.via(FastJdbcIO.ToStaticKey))
                    .apply(GroupIntoBatches.ofSize(getSetSize()));


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

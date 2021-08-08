package io.xentripetal.fastjdbcio;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

/**
 * A big copy and paste of JdbcIO with an added queryGetter property.
 * JdbcIO has private constructors so can't just extend it.
 */
public class JdbcIOExtensions {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcIOExtensions.class);

    /**
     * An interface used by the JdbcIOExtension to set the query for a ReadAll statement.
     * AutoValues have private constructors, so can't just extend it. Should probably just make a PR
     * to support it.
     */
    @FunctionalInterface
    public interface QueryGetter<T> extends Serializable {
        String getQuery(T element) throws Exception;
    }


    /**
     * {@link PCollection} as query parameters.
     *
     * @param <ParameterT> Type of the data representing query parameters.
     * @param <OutputT> Type of the data to be read.
     */
    public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
        return null;/**
        return new .Builder<ParameterT, OutputT>()
                .setFetchSize(DEFAULT_FETCH_SIZE)
                .setOutputParallelization(true)
                .build();**/
    }

    @AutoValue
    public abstract static class ReadAll<ParameterT, OutputT>
            extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {

        abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

        abstract @Nullable QueryGetter<ParameterT> getQueryGetter();

        abstract JdbcIO.PreparedStatementSetter<ParameterT> getParameterSetter();

        abstract JdbcIO.RowMapper<OutputT> getRowMapper();

        abstract @Nullable Coder<OutputT> getCoder();

        abstract int getFetchSize();

        abstract boolean getOutputParallelization();

        abstract ReadAll.Builder<ParameterT, OutputT> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<ParameterT, OutputT> {
            abstract ReadAll.Builder<ParameterT, OutputT> setDataSourceProviderFn(
                    SerializableFunction<Void, DataSource> dataSourceProviderFn);

            abstract ReadAll.Builder<ParameterT, OutputT> setQueryGetter(QueryGetter<ParameterT> queryGetter);

            abstract ReadAll.Builder<ParameterT, OutputT> setParameterSetter(
                    JdbcIO.PreparedStatementSetter<ParameterT> parameterSetter);

            abstract ReadAll.Builder<ParameterT, OutputT> setRowMapper(JdbcIO.RowMapper<OutputT> rowMapper);

            abstract ReadAll.Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

            abstract ReadAll.Builder<ParameterT, OutputT> setFetchSize(int fetchSize);

            abstract ReadAll.Builder<ParameterT, OutputT> setOutputParallelization(boolean outputParallelization);

            abstract ReadAll<ParameterT, OutputT> build();
        }

        public ReadAll<ParameterT, OutputT> withDataSourceConfiguration(
                JdbcIO.DataSourceConfiguration config) {
            return withDataSourceProviderFn(JdbcIO.DataSourceProviderFromDataSourceConfiguration.of(config));
        }

        public ReadAll<ParameterT, OutputT> withDataSourceProviderFn(
                SerializableFunction<Void, DataSource> dataSourceProviderFn) {
            return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
        }

        public ReadAll<ParameterT, OutputT> withQuery(String query) {
            checkArgument(query != null, "JdbcIO.readAll().withQuery(query) called with null query");
            return withQuery(ValueProvider.StaticValueProvider.of(query));
        }

        public ReadAll<ParameterT, OutputT> withQuery(ValueProvider<String> query) {
            checkArgument(query != null, "JdbcIO.readAll().withQuery(query) called with null query");
            return withQueryGetter(new QueryGetter<ParameterT>() {
                @Override
                public String getQuery(ParameterT element) throws Exception {
                    return query.get();
                }
            });
        }

        public ReadAll<ParameterT, OutputT> withQueryGetter(QueryGetter<ParameterT> queryGetter) {
            checkNotNull(queryGetter, "withQueryGetter() called with null queryGetter");
            return toBuilder().setQueryGetter(queryGetter).build();
        }

        public ReadAll<ParameterT, OutputT> withParameterSetter(
                JdbcIO.PreparedStatementSetter<ParameterT> parameterSetter) {
            checkArgument(
                    parameterSetter != null,
                    "JdbcIO.readAll().withParameterSetter(parameterSetter) called "
                            + "with null statementPreparator");
            return toBuilder().setParameterSetter(parameterSetter).build();
        }

        public ReadAll<ParameterT, OutputT> withRowMapper(JdbcIO.RowMapper<OutputT> rowMapper) {
            checkArgument(
                    rowMapper != null,
                    "JdbcIO.readAll().withRowMapper(rowMapper) called with null rowMapper");
            return toBuilder().setRowMapper(rowMapper).build();
        }

        public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
            checkArgument(coder != null, "JdbcIO.readAll().withCoder(coder) called with null coder");
            return toBuilder().setCoder(coder).build();
        }

        /**
         * This method is used to set the size of the data that is going to be fetched and loaded in
         * memory per every database call. Please refer to: {@link java.sql.Statement#setFetchSize(int)}
         * It should ONLY be used if the default value throws memory errors.
         */
        public ReadAll<ParameterT, OutputT> withFetchSize(int fetchSize) {
            checkArgument(fetchSize > 0, "fetch size must be >0");
            return toBuilder().setFetchSize(fetchSize).build();
        }

        /**
         * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
         * default is to parallelize and should only be changed if this is known to be unnecessary.
         */
        public ReadAll<ParameterT, OutputT> withOutputParallelization(boolean outputParallelization) {
            return toBuilder().setOutputParallelization(outputParallelization).build();
        }

        @Override
        public PCollection<OutputT> expand(PCollection<ParameterT> input) {
            PCollection<OutputT> output =
                    input
                            .apply(
                                    ParDo.of(
                                            new ReadFn<>(
                                                    getDataSourceProviderFn(),
                                                    getQueryGetter(),
                                                    getParameterSetter(),
                                                    getRowMapper(),
                                                    getFetchSize())))
                            .setCoder(getCoder());

            if (getOutputParallelization()) {
                output = output.apply(new Reparallelize<>());
            }

            try {
                TypeDescriptor<OutputT> typeDesc = getCoder().getEncodedTypeDescriptor();
                SchemaRegistry registry = input.getPipeline().getSchemaRegistry();
                Schema schema = registry.getSchema(typeDesc);
                output.setSchema(
                        schema,
                        typeDesc,
                        registry.getToRowFunction(typeDesc),
                        registry.getFromRowFunction(typeDesc));
            } catch (NoSuchSchemaException e) {
                // ignore
            }

            return output;
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("queryGetter", getQueryGetter().getClass().getName()));
            builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
            builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
            if (getDataSourceProviderFn() instanceof HasDisplayData) {
                ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
            }
        }
    }


    /** A {@link DoFn} executing the SQL query to read from the database. */
    private static class ReadFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
        private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
        private final QueryGetter<ParameterT> queryGetter;
        private final JdbcIO.PreparedStatementSetter<ParameterT> parameterSetter;
        private final JdbcIO.RowMapper<OutputT> rowMapper;
        private final int fetchSize;

        private DataSource dataSource;
        private Connection connection;

        private ReadFn(
                SerializableFunction<Void, DataSource> dataSourceProviderFn,
                QueryGetter<ParameterT> queryGetter,
                JdbcIO.PreparedStatementSetter<ParameterT> parameterSetter,
                JdbcIO.RowMapper<OutputT> rowMapper,
                int fetchSize) {
            this.dataSourceProviderFn = dataSourceProviderFn;
            this.queryGetter = queryGetter;
            this.parameterSetter = parameterSetter;
            this.rowMapper = rowMapper;
            this.fetchSize = fetchSize;
        }

        @Setup
        public void setup() throws Exception {
            dataSource = dataSourceProviderFn.apply(null);
        }

        @ProcessElement
        // Spotbugs seems to not understand the nested try-with-resources
        public void processElement(ProcessContext context) throws Exception {
            // Only acquire the connection if we need to perform a read.
            if (connection == null) {
                connection = dataSource.getConnection();
            }
            // PostgreSQL requires autocommit to be disabled to enable cursor streaming
            // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
            LOG.info("Autocommit has been disabled");
            connection.setAutoCommit(false);
            try (PreparedStatement statement =
                         connection.prepareStatement(
                                 queryGetter.getQuery(context.element()), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                statement.setFetchSize(fetchSize);
                parameterSetter.setParameters(context.element(), statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        context.output(rowMapper.mapRow(resultSet));
                    }
                }
            }
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            cleanUpConnection();
        }

        @Override
        protected void finalize() throws Throwable {
            cleanUpConnection();
        }

        private void cleanUpConnection() throws Exception {
            if (connection != null) {
                try {
                    connection.close();
                } finally {
                    connection = null;
                }
            }
        }
    }
    private static class Reparallelize<T> extends PTransform<PCollection<T>, PCollection<T>> {
        @Override
        public PCollection<T> expand(PCollection<T> input) {
            // See https://issues.apache.org/jira/browse/BEAM-2803
            // We use a combined approach to "break fusion" here:
            // (see https://cloud.google.com/dataflow/service/dataflow-service-desc#preventing-fusion)
            // 1) force the data to be materialized by passing it as a side input to an identity fn,
            // then 2) reshuffle it with a random key. Initial materialization provides some parallelism
            // and ensures that data to be shuffled can be generated in parallel, while reshuffling
            // provides perfect parallelism.
            // In most cases where a "fusion break" is needed, a simple reshuffle would be sufficient.
            // The current approach is necessary only to support the particular case of JdbcIO where
            // a single query may produce many gigabytes of query results.
            PCollectionView<Iterable<T>> empty =
                    input
                            .apply("Consume", Filter.by(SerializableFunctions.constant(false)))
                            .apply(View.asIterable());
            PCollection<T> materialized =
                    input.apply(
                            "Identity",
                            ParDo.of(
                                            new DoFn<T, T>() {
                                                @ProcessElement
                                                public void process(ProcessContext c) {
                                                    c.output(c.element());
                                                }
                                            })
                                    .withSideInputs(empty));
            return materialized.apply(Reshuffle.viaRandomKey());
        }
    }
}

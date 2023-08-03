package com.kedacom.bigdata.flink.table.factories;

import com.kedacom.bigdata.flink.api.common.functions.util.PrintSinkOutputWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.configuration.ConfigOptions.key;

@PublicEvolving
public class StdoutPrintTableSinkFactory implements DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StdoutPrintTableSinkFactory.class);

    public static final String IDENTIFIER = "stdout";

    public static final ConfigOption<String> PRINT_IDENTIFIER =
            key("print-identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Message that identify print and is prefixed to the output of the value.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PRINT_IDENTIFIER);
        options.add(FactoryUtil.SINK_PARALLELISM);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        return new PrintSink(
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType(),
                options.get(PRINT_IDENTIFIER),
                options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    private static class PrintSink implements DynamicTableSink {

        private final DataType type;
        private final String printIdentifier;
        private final @Nullable Integer parallelism;

        private PrintSink(DataType type, String printIdentifier, @Nullable Integer parallelism) {
            this.type = type;
            this.printIdentifier = printIdentifier;
            this.parallelism = parallelism;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(DynamicTableSink.Context context) {
            DataStructureConverter converter = context.createDataStructureConverter(type);
            return SinkFunctionProvider.of(
                    new RowDataPrintFunction(converter, printIdentifier), parallelism);
        }

        @Override
        public DynamicTableSink copy() {
            return new PrintSink(type, printIdentifier, parallelism);
        }

        @Override
        public String asSummaryString() {
            return "Print to " + "";
        }
    }

    private static class StreamHolder {

        private static final LoadingCache<String, PrintStream> INSTANCES =
                Caffeine.newBuilder()
                        .maximumSize(100)
                        .expireAfterAccess(5, TimeUnit.MINUTES)
                        .build(
                                path -> {
                                    final int extension = path.lastIndexOf('.');

                                    if (extension > 0) {
                                        String stdoutPath = path.substring(0, extension) + ".out";
                                        File stdoutFile = new File(stdoutPath);
                                        try {
                                            if (stdoutFile.exists() || stdoutFile.createNewFile()) {
                                                return new PrintStream(
                                                        new FileOutputStream(stdoutFile),
                                                        true,
                                                        StandardCharsets.UTF_8.name());
                                            }
                                        } catch (IOException e) {
                                            LOG.warn(
                                                    "Error when prepare stdout: {}", stdoutPath, e);
                                        }
                                    }
                                    return System.out;
                                });

        private static synchronized PrintStream getStdout(Configuration configuration) {
            String logPath =
                    configuration.getString(
                            ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,
                            System.getProperty("log.file"));
            if (logPath != null) {
                return INSTANCES.get(logPath);
            }
            return System.out;
        }
    }

    /**
     * Implementation of the SinkFunction converting {@link RowData} to string and passing to {@link
     * PrintSinkFunction}.
     */
    private static class RowDataPrintFunction extends RichSinkFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private final DynamicTableSink.DataStructureConverter converter;

        private final PrintSinkOutputWriter<String> writer;

        private RowDataPrintFunction(
                DynamicTableSink.DataStructureConverter converter, String printIdentifier) {
            this.converter = converter;
            this.writer = new PrintSinkOutputWriter<>(printIdentifier);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            writer.open(
                    context.getIndexOfThisSubtask(),
                    context.getNumberOfParallelSubtasks(),
                    StreamHolder.getStdout(parameters));
        }

        @Override
        public void close() {
            writer.close();
        }

        @Override
        public void invoke(RowData value, Context context) {
            Object data = converter.toExternal(value);
            assert data != null;
            writer.write(data.toString());
        }
    }
}

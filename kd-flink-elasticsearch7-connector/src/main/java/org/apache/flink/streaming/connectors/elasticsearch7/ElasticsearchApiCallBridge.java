package org.apache.flink.streaming.connectors.elasticsearch7;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;

/**
 * An {@link ElasticsearchApiCallBridge} is used to bridge incompatible Elasticsearch Java API calls
 * across different versions. This includes calls to create Elasticsearch clients, handle failed
 * item responses, etc. Any incompatible Elasticsearch Java APIs should be bridged using this
 * interface.
 *
 * <p>Implementations are allowed to be stateful. For example, for Elasticsearch 1.x, since
 * connecting via an embedded node
 * is allowed, the call bridge will hold reference to the created embedded node. Each instance of
 * the sink will hold exactly one instance of the call bridge, and state cleanup is performed when
 * the sink is closed.
 *
 * @param <C> The Elasticsearch client, that implements {@link AutoCloseable}.
 */
@Internal
public interface ElasticsearchApiCallBridge<C extends AutoCloseable> extends Serializable {

    /**
     * Creates an Elasticsearch client implementing {@link AutoCloseable}.
     *
     * @param clientConfig The configuration to use when constructing the client.
     * @return The created client.
     */
    C createClient(Map<String, String> clientConfig);

    /**
     * Creates a {@link BulkProcessor.Builder} for creating the bulk processor.
     *
     * @param client   the Elasticsearch client.
     * @param client   the Elasticsearch client.
     * @param listener the bulk processor listender.
     * @return the bulk processor builder.
     */
    BulkProcessor.Builder createBulkProcessorBuilder(C client, BulkProcessor.Listener listener);

    ElasticsearchInputSplit[] createInputSplitsInternal(C client, String index, String type,
        int minNumSplits);

    Tuple2<String, String[]> search(C client, SearchRequest searchRequest) throws IOException;

    Tuple2<String, String[]> scroll(C client, SearchScrollRequest searchScrollRequest)
        throws IOException;

    void close(C client) throws IOException;

    /**
     * Extracts the cause of failure of a bulk item action.
     *
     * @param bulkItemResponse the bulk item response to extract cause of failure
     * @return the extracted {@link Throwable} from the response ({@code null} is the response is
     * successful).
     */
    @Nullable
    Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse);

    /**
     * Set backoff-related configurations on the provided {@link BulkProcessor.Builder}. The builder
     * will be later on used to instantiate the actual {@link BulkProcessor}.
     *
     * @param builder            the {@link BulkProcessor.Builder} to configure.
     * @param builder            the {@link BulkProcessor.Builder} to configure.
     * @param flushBackoffPolicy user-provided backoff retry settings ({@code null} if the user
     *                           disabled backoff retries).
     */
    void configureBulkProcessorBackoff(
        BulkProcessor.Builder builder,
        @Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy);

    /**
     * Verify the client connection by making a test request/ping to the Elasticsearch cluster.
     *
     * <p>Called by {@link ElasticsearchSinkBase#open(org.apache.flink.configuration.Configuration)}
     * after creating the client. This makes sure the underlying
     * client is closed if the connection is not successful and preventing thread leak.
     *
     * @param client the Elasticsearch client.
     */
    void verifyClientConnection(C client) throws IOException;

    /**
     * Creates a {@link RequestIndexer} that is able to work with {@link BulkProcessor} binary
     * compatible.
     */
    default RequestIndexer createBulkProcessorIndexer(
        BulkProcessor bulkProcessor,
        boolean flushOnCheckpoint,
        AtomicLong numPendingRequestsRef) {
        return null;
    }

    /**
     * Perform any necessary state cleanup.
     */
    default void cleanup() {
        // nothing to cleanup by default
    }
}
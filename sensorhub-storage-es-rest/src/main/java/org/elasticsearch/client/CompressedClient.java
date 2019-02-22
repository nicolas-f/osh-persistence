package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static java.util.Collections.emptySet;

public class CompressedClient extends RestHighLevelClient {
    private static final String GZIP_CODEC = "gzip";


    public CompressedClient(RestClientBuilder restClientBuilder) {
        super(restClientBuilder);
    }

    public CompressedClient(RestClientBuilder restClientBuilder, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        super(restClientBuilder, namedXContentEntries);
    }

    public CompressedClient(RestClient restClient, CheckedConsumer<RestClient, IOException> doClose, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        super(restClient, doClose, namedXContentEntries);
    }


    /**
     * Asynchronously executes a bulk request using the Bulk API
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">Bulk API on elastic.co</a>
     */
    public final void bulkCompressedAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(bulkRequest, CompressedClient::bulkCompressed, BulkResponse::fromXContent, listener, emptySet(), headers);
    }


    static Request bulkCompressed(BulkRequest bulkRequest) throws IOException {
        Request.Params parameters = Request.Params.builder();
        parameters.withTimeout(bulkRequest.timeout());
        parameters.withRefreshPolicy(bulkRequest.getRefreshPolicy());

        // Bulk API only supports newline delimited JSON or Smile. Before executing
        // the bulk, we need to check that all requests have the same content-type
        // and this content-type is supported by the Bulk API.
        XContentType bulkContentType = null;
        for (int i = 0; i < bulkRequest.numberOfActions(); i++) {
            DocWriteRequest<?> request = bulkRequest.requests().get(i);

            DocWriteRequest.OpType opType = request.opType();
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                bulkContentType = Request.enforceSameContentType((IndexRequest) request, bulkContentType);

            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                UpdateRequest updateRequest = (UpdateRequest) request;
                if (updateRequest.doc() != null) {
                    bulkContentType = Request.enforceSameContentType(updateRequest.doc(), bulkContentType);
                }
                if (updateRequest.upsertRequest() != null) {
                    bulkContentType = Request.enforceSameContentType(updateRequest.upsertRequest(), bulkContentType);
                }
            }
        }

        if (bulkContentType == null) {
            bulkContentType = XContentType.JSON;
        }

        final byte separator = bulkContentType.xContent().streamSeparator();
        final ContentType requestContentType = Request.createContentType(bulkContentType);

        ByteArrayOutputStream content = new ByteArrayOutputStream();
        for (DocWriteRequest<?> request : bulkRequest.requests()) {
            DocWriteRequest.OpType opType = request.opType();

            try (XContentBuilder metadata = XContentBuilder.builder(bulkContentType.xContent())) {
                metadata.startObject();
                {
                    metadata.startObject(opType.getLowercase());
                    if (Strings.hasLength(request.index())) {
                        metadata.field("_index", request.index());
                    }
                    if (Strings.hasLength(request.type())) {
                        metadata.field("_type", request.type());
                    }
                    if (Strings.hasLength(request.id())) {
                        metadata.field("_id", request.id());
                    }
                    if (Strings.hasLength(request.routing())) {
                        metadata.field("routing", request.routing());
                    }
                    if (Strings.hasLength(request.parent())) {
                        metadata.field("parent", request.parent());
                    }
                    if (request.version() != Versions.MATCH_ANY) {
                        metadata.field("version", request.version());
                    }

                    VersionType versionType = request.versionType();
                    if (versionType != VersionType.INTERNAL) {
                        if (versionType == VersionType.EXTERNAL) {
                            metadata.field("version_type", "external");
                        } else if (versionType == VersionType.EXTERNAL_GTE) {
                            metadata.field("version_type", "external_gte");
                        } else if (versionType == VersionType.FORCE) {
                            metadata.field("version_type", "force");
                        }
                    }

                    if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                        IndexRequest indexRequest = (IndexRequest) request;
                        if (Strings.hasLength(indexRequest.getPipeline())) {
                            metadata.field("pipeline", indexRequest.getPipeline());
                        }
                    } else if (opType == DocWriteRequest.OpType.UPDATE) {
                        UpdateRequest updateRequest = (UpdateRequest) request;
                        if (updateRequest.retryOnConflict() > 0) {
                            metadata.field("retry_on_conflict", updateRequest.retryOnConflict());
                        }
                        if (updateRequest.fetchSource() != null) {
                            metadata.field("_source", updateRequest.fetchSource());
                        }
                    }
                    metadata.endObject();
                }
                metadata.endObject();

                BytesRef metadataSource = BytesReference.bytes(metadata).toBytesRef();
                content.write(metadataSource.bytes, metadataSource.offset, metadataSource.length);
                content.write(separator);
            }

            BytesRef source = null;
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest indexRequest = (IndexRequest) request;
                BytesReference indexSource = indexRequest.source();
                XContentType indexXContentType = indexRequest.getContentType();

                try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE, indexSource, indexXContentType)) {
                    try (XContentBuilder builder = XContentBuilder.builder(bulkContentType.xContent())) {
                        builder.copyCurrentStructure(parser);
                        source = BytesReference.bytes(builder).toBytesRef();
                    }
                }
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                source = XContentHelper.toXContent((UpdateRequest) request, bulkContentType, false).toBytesRef();
            }

            if (source != null) {
                content.write(source.bytes, source.offset, source.length);
                content.write(separator);
            }
        }

        byte[] original = content.toByteArray();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(original.length);
        GZIPOutputStream gzip = new GZIPOutputStream(byteArrayOutputStream);
        gzip.write(original);
        gzip.finish();
        ByteArrayEntity entity = new ByteArrayEntity(byteArrayOutputStream.toByteArray(), requestContentType);
        entity.setContentEncoding(new BasicHeader(HTTP.CONTENT_ENCODING, GZIP_CODEC));
        return new Request(HttpPost.METHOD_NAME, "/_bulk", parameters.getParams(), entity);
    }
}

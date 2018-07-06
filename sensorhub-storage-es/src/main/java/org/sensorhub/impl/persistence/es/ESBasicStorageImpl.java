/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2016 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.persistence.es;

import net.opengis.gml.v32.AbstractTimeGeometricPrimitive;
import net.opengis.gml.v32.TimeInstant;
import net.opengis.gml.v32.TimePeriod;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.persistence.*;
import org.sensorhub.impl.module.AbstractModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.data.*;

import java.io.*;
import java.lang.Boolean;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * <p>
 * ES implementation of {@link IObsStorage} for storing observations.
 * This class is Thread-safe.
 * </p>
 *
 * @author Mathieu Dhainaut <mathieu.dhainaut@gmail.com>
 * @since 2017
 */
public class ESBasicStorageImpl extends AbstractModule<ESBasicStorageConfig> implements IRecordStorageModule<ESBasicStorageConfig> {
	private static final int TIME_RANGE_CLUSTER_SCROLL_FETCH_SIZE = 5000;

	protected static final double MAX_TIME_CLUSTER_DELTA = 60.0;

	// ms .Fetch again record store map if it is done at least this time
	private static final int RECORD_STORE_CACHE_LIFETIME = 5000;

	// From ElasticSearch v6, multiple index type is not supported
    //                    v7 index type dropped
    protected static final String INDEX_METADATA_TYPE = "osh_metadata";

    protected static final String STORAGE_ID_FIELD_NAME = "storageID";

    // The data index, serialization of OpenSensorHub internals metadata
    protected static final String METADATA_TYPE_FIELD_NAME = "metadataType";

	protected static final String DATA_INDEX_FIELD_NAME = "index";

	protected static final String RS_KEY_SEPARATOR = "##";

	protected static final String BLOB_FIELD_NAME = "blob";

	private static final int WAIT_TIME_AFTER_COMMIT = 1000;

	// Last time the store has been changed, may require waiting if data changed since less than a second
	long storeChanged = 0;

	private List<String> addedIndex = new ArrayList<>();

    private Map<String, EsRecordStoreInfo> recordStoreCache = new HashMap<>();

    private long recordStoreCachTime = 0;

	protected static final double[] ALL_TIMES = new double[] {Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY};
	/**
	 * Class logger
	 */
	private static final Logger log = LoggerFactory.getLogger(ESBasicStorageImpl.class);  
	
	/**
	 * The TransportClient connects remotely to an Elasticsearch cluster using the transport module. 
	 * It does not join the cluster, but simply gets one or more initial transport addresses and communicates 
	 * with them in round robin fashion on each action (though most actions will probably be "two hop" operations).
	 */
	RestHighLevelClient client;

	private BulkProcessor bulkProcessor;

	// Kinds of OpenSensorHub serialized objects
	protected static final String METADATA_TYPE_DESCRIPTION = "desc";
	protected static final String METADATA_TYPE_RECORD_STORE = "info";

	String indexNamePrepend;
	String indexNameMetaData;

    BulkListener bulkListener = new BulkListener();

	
	public ESBasicStorageImpl() {
	    // default constructor
    }
	
	public ESBasicStorageImpl(RestHighLevelClient client) {
		this.client = client;
	}
	
	@Override
	public void backup(OutputStream os) throws IOException {
		throw new UnsupportedOperationException("Backup");
	}

    public RestHighLevelClient getClient() {
        return client;
    }

    @Override
	public void restore(InputStream is) throws IOException {
		throw new UnsupportedOperationException("Restore");
	}

	@Override
	public void commit() {
		refreshIndex();
        // https://www.elastic.co/guide/en/elasticsearch/guide/current/near-real-time.html
        // document changes are not visible to search immediately, but will become visible within 1 second.
        long now = System.currentTimeMillis();
        if(now - storeChanged < WAIT_TIME_AFTER_COMMIT) {
            try {
                Thread.sleep(WAIT_TIME_AFTER_COMMIT);
            } catch (InterruptedException ignored) {
            }
        }
	}

	@Override
	public void rollback() {
		throw new UnsupportedOperationException("Rollback");
	}

	@Override
	public void sync(IStorageModule<?> storage) throws StorageException {
		throw new UnsupportedOperationException("Storage Sync");
	}
	
	@Override
	public void init() {
	    this.indexNamePrepend = (config.indexNamePrepend != null) ? config.indexNamePrepend : "";
	    this.indexNameMetaData = (config.indexNameMetaData != null && !config.indexNameMetaData.isEmpty()) ? config.indexNameMetaData : ESBasicStorageConfig.DEFAULT_INDEX_NAME_METADATA;
	}

	@Override
	public synchronized void start() throws SensorHubException {
	    log.info("ESBasicStorageImpl:start");
		if(client == null) {
			// init transport client
			HttpHost[] hosts = new HttpHost[config.nodeUrls.size()];
            int i=0;
            for(String nodeUrl : config.nodeUrls){
                try {
                    URL url = null;
                    // <host>:<port>
                    if(nodeUrl.startsWith("http")){
                        url = new URL(nodeUrl);
                    } else {
                        url = new URL("http://"+nodeUrl);
                    }

                    hosts[i++] = new HttpHost(InetAddress.getByName(url.getHost()), url.getPort(), url.getProtocol());

                } catch (MalformedURLException | UnknownHostException e) {
                    log.error("Cannot initialize transport address:"+e.getMessage());
                    throw new SensorHubException("Cannot initialize transport address",e);
                }
            }

            RestClientBuilder restClientBuilder = RestClient.builder(hosts);

            // Handle authentication
            if(!config.user.isEmpty()) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(config.user, config.password));
                restClientBuilder.setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
			client = new RestHighLevelClient(restClientBuilder);
		}

        bulkProcessor = BulkProcessor.builder(client::bulkAsync, bulkListener).setBulkActions(config.bulkActions)
                .setBulkSize(new ByteSizeValue(config.bulkSize, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(config.bulkFlushInterval))
                .setConcurrentRequests(config.bulkConcurrentRequests)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

		// Check if metadata mapping must be defined
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexNameMetaData);
        try {
            if(!client.indices().exists(getIndexRequest)) {
                createMetaMapping();
            }
        } catch (IOException ex) {
            logger.error("Cannot create metadata mapping", ex);
        }
	}

	void createMetaMappingProperties(XContentBuilder builder) throws IOException {
        builder.startObject(STORAGE_ID_FIELD_NAME);
        {
            builder.field("type", "keyword");
        }
        builder.endObject();
        builder.startObject(METADATA_TYPE_FIELD_NAME);
        {
            builder.field("type", "keyword");
        }
        builder.endObject();
        builder.startObject(DATA_INDEX_FIELD_NAME);
        {
            builder.field("type", "keyword");
        }
        builder.endObject();
        builder.startObject(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME);
        {
            builder.field("type", "date");
            builder.field("format", "epoch_millis");
        }
        builder.endObject();
        builder.startObject(BLOB_FIELD_NAME);
        {
            builder.field("type", "binary");
        }
        builder.endObject();
    }

	void createMetaMapping () throws IOException {
		// create the index
	    CreateIndexRequest indexRequest = new CreateIndexRequest(indexNameMetaData);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject(INDEX_METADATA_TYPE);
            {
                builder.startObject("properties");
                {
                    createMetaMappingProperties(builder);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

	    indexRequest.mapping(INDEX_METADATA_TYPE, builder);

	    client.indices().create(indexRequest);

        addedIndex.add(indexNameMetaData);
	}

    @Override
	public synchronized void stop() throws SensorHubException {
        log.info("ESBasicStorageImpl:stop");
		if(client != null) {
		    try {
                client.close();
            } catch (IOException ex) {
		        throw new SensorHubException(ex.getLocalizedMessage(), ex);
            }
		}
	}

	@Override
	public AbstractProcess getLatestDataSourceDescription() {
        AbstractProcess result = null;
        SearchRequest searchRequest = new SearchRequest(indexNameMetaData);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(STORAGE_ID_FIELD_NAME, config.id))
                .must(new TermQueryBuilder(METADATA_TYPE_FIELD_NAME, METADATA_TYPE_DESCRIPTION));
        searchSourceBuilder.query(query);
        searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.DESC));
        searchSourceBuilder.size(1);
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse response = client.search(searchRequest);
       		if(response.getHits().getTotalHits() > 0) {
                result = this.getObject(response.getHits().getAt(0).getSourceAsMap().get(BLOB_FIELD_NAME));
            }
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("getRecordStores failed", ex);
        }

        return result;
	}

	@Override
	public List<AbstractProcess> getDataSourceDescriptionHistory(double startTime, double endTime) {
        List<AbstractProcess> results = new ArrayList<>();

        SearchRequest searchRequest = new SearchRequest(indexNameMetaData);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(STORAGE_ID_FIELD_NAME, config.id))
                .must(new TermQueryBuilder(METADATA_TYPE_FIELD_NAME, METADATA_TYPE_DESCRIPTION))
                .must(new RangeQueryBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).from(ESDataStoreTemplate.toEpochMillisecond(startTime)).to(ESDataStoreTemplate.toEpochMillisecond(endTime)).format("epoch_millis"));
        searchSourceBuilder.query(query);
        searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.DESC));
        searchSourceBuilder.size(config.scrollFetchSize);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));
        try {
            SearchResponse response = client.search(searchRequest);
            do {
                String scrollId = response.getScrollId();
                for (SearchHit hit : response.getHits()) {
                    results.add(this.getObject(hit.getSourceAsMap().get(BLOB_FIELD_NAME)));
                }
                if (response.getHits().getHits().length > 0) {
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                    scrollRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));
                    response = client.searchScroll(scrollRequest);
                }
            } while (response.getHits().getHits().length > 0);

        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("getRecordStores failed", ex);
        }

        return results;
	}

	@Override
	public AbstractProcess getDataSourceDescriptionAtTime(double time) {


        AbstractProcess result = null;
        SearchRequest searchRequest = new SearchRequest(indexNameMetaData);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(STORAGE_ID_FIELD_NAME, config.id))
                .must(new TermQueryBuilder(METADATA_TYPE_FIELD_NAME, METADATA_TYPE_DESCRIPTION))
                .must(new RangeQueryBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).from(0).to(Double.valueOf(time * 1000).longValue()));
        searchSourceBuilder.query(query);
        searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.DESC));
        searchSourceBuilder.size(1);
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse response = client.search(searchRequest);
            if(response.getHits().getTotalHits() > 0) {
                result = this.getObject(response.getHits().getAt(0).getSourceAsMap().get(BLOB_FIELD_NAME));
            }
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("getRecordStores failed", ex);
        }

        return result;
	}

	protected boolean storeDataSourceDescription(AbstractProcess process, double time, boolean update) {
        // add new record storage
        byte[] bytes = this.getBlob(process);

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            // Convert to elastic search epoch millisecond
            long epoch = ESDataStoreTemplate.toEpochMillisecond(time);
            builder.startObject();
            {
                builder.field(STORAGE_ID_FIELD_NAME, config.id);
                builder.field(METADATA_TYPE_FIELD_NAME, METADATA_TYPE_DESCRIPTION);
                builder.field(DATA_INDEX_FIELD_NAME, "");
                builder.field(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME, epoch);
                builder.field(BLOB_FIELD_NAME, bytes);
            }
            builder.endObject();

            IndexRequest request = new IndexRequest(indexNameMetaData, INDEX_METADATA_TYPE, config.id + "_" + epoch);

            request.source(builder);

            bulkProcessor.add(request);

        } catch (IOException ex) {
            logger.error(String.format("storeDataSourceDescription exception %s in elastic search driver", process.getId()), ex);
        }
        return true;
	}


	protected boolean storeDataSourceDescription(AbstractProcess process, boolean update) {

		boolean ok = false;

		if (process.getNumValidTimes() > 0) {
			// we add the description in index for each validity period/instant
			for (AbstractTimeGeometricPrimitive validTime : process.getValidTimeList()) {
				double time = Double.NaN;

				if (validTime instanceof TimeInstant)
					time = ((TimeInstant) validTime).getTimePosition().getDecimalValue();
				else if (validTime instanceof TimePeriod)
					time = ((TimePeriod) validTime).getBeginPosition().getDecimalValue();

				if (!Double.isNaN(time))
					ok = storeDataSourceDescription(process, time, update);
			}
		} else {
			double time = System.currentTimeMillis() / 1000.;
			ok = storeDataSourceDescription(process, time, update);
		}

		return ok;
	}

	@Override
	public void storeDataSourceDescription(AbstractProcess process) {
		storeDataSourceDescription(process, false);
	}

	@Override
	public void updateDataSourceDescription(AbstractProcess process) {
		storeDataSourceDescription(process, true);
	}

	@Override
	public void removeDataSourceDescription(double time) {
        long epoch = ESDataStoreTemplate.toEpochMillisecond(time);
		DeleteRequest deleteRequest = new DeleteRequest(indexNameMetaData, METADATA_TYPE_DESCRIPTION, config.id + "_" + epoch);
		bulkProcessor.add(deleteRequest);

		storeChanged = System.currentTimeMillis();
	}

	@Override
	public void removeDataSourceDescriptionHistory(double startTime, double endTime) {
        try {
            // Delete by query, currently not supported by High Level Api
            BoolQueryBuilder query = QueryBuilders.boolQuery().must(
                    QueryBuilders.termQuery(METADATA_TYPE_FIELD_NAME, METADATA_TYPE_DESCRIPTION))
                    .must(new RangeQueryBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME)
                            .from(ESDataStoreTemplate.toEpochMillisecond(startTime))
                            .to(ESDataStoreTemplate.toEpochMillisecond(endTime)).format("epoch_millis"));

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            XContentBuilder builder = XContentFactory.jsonBuilder(bos);
            builder.startObject();
            builder.rawField("query", new ByteArrayInputStream(query.toString().getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
            builder.endObject();
            builder.flush();
            String json = bos.toString("UTF-8");
            HttpEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON);
            client.getLowLevelClient().performRequest("POST"
                    , encodeEndPoint(indexNameMetaData, "_delete_by_query")
                    , Collections.EMPTY_MAP, entity);

            storeChanged = System.currentTimeMillis();

        } catch (IOException ex) {
            log.error("Failed to removeRecords", ex);
        }
	}

	@Override
	public  Map<String, EsRecordStoreInfo> getRecordStores() {
	    long now = System.currentTimeMillis();
	    if(now - recordStoreCachTime < RECORD_STORE_CACHE_LIFETIME) {
	        return recordStoreCache;
        }

		Map<String, EsRecordStoreInfo> result = new HashMap<>();

        SearchRequest searchRequest = new SearchRequest(indexNameMetaData);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(STORAGE_ID_FIELD_NAME, config.id))
                .must(new TermQueryBuilder(METADATA_TYPE_FIELD_NAME, METADATA_TYPE_RECORD_STORE));
        searchSourceBuilder.query(query);
        // Default to 10 results
        searchSourceBuilder.size(config.scrollFetchSize);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));
        try {
            SearchResponse response = client.search(searchRequest);
            do {
                String scrollId = response.getScrollId();
                for (SearchHit hit : response.getHits()) {
                    Map<String, Object> dataMap = hit.getSourceAsMap();
                    EsRecordStoreInfo rsInfo = this.getObject(dataMap.get(BLOB_FIELD_NAME)); // DataStreamInfo
                    result.put(rsInfo.getName(), rsInfo);
                }
                if (response.getHits().getHits().length > 0) {
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                    scrollRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));
                    response = client.searchScroll(scrollRequest);
                }
            } while (response.getHits().getHits().length > 0);

        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("getRecordStores failed", ex);
        }

        recordStoreCache = result;

        recordStoreCachTime = now;

        return recordStoreCache;
	}

    /**
     * Index in Elastic Search are restricted
     * @param indexName Index name to remove undesired chars
     * @return valid index for es
     */
	String fixIndexName(String indexName) {
        for(Character chr : Strings.INVALID_FILENAME_CHARS) {
            indexName = indexName.replace(chr.toString(), "");
        }
        indexName = indexName.replace("#", "");
        while(indexName.startsWith("_") || indexName.startsWith("-") || indexName.startsWith("+")) {
            indexName = indexName.substring(1, indexName.length());
        }
        return indexName.toLowerCase(Locale.ROOT);
    }

	@Override
	public void addRecordStore(String name, DataComponent recordStructure, DataEncoding recommendedEncoding) {
        log.info("ESBasicStorageImpl:addRecordStore");
        EsRecordStoreInfo rsInfo = new EsRecordStoreInfo(name,fixIndexName(indexNamePrepend + recordStructure.getName()),
                recordStructure, recommendedEncoding);

        recordStoreCache.put(rsInfo.name, rsInfo);

		// add new record storage
		byte[] bytes = this.getBlob(rsInfo);

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                // Convert to elastic search epoch millisecond
                builder.field(STORAGE_ID_FIELD_NAME, config.id);
                builder.field(METADATA_TYPE_FIELD_NAME, METADATA_TYPE_RECORD_STORE);
                builder.field(DATA_INDEX_FIELD_NAME, rsInfo.getIndexName()); // store recordType
                builder.field(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME, System.currentTimeMillis());
                builder.field(BLOB_FIELD_NAME, bytes);
            }
            builder.endObject();
            IndexRequest request = new IndexRequest(indexNameMetaData, INDEX_METADATA_TYPE);

            request.source(builder);

            client.index(request);

            // Check if metadata mapping must be defined
            GetIndexRequest getIndexRequest = new GetIndexRequest();
            getIndexRequest.indices(rsInfo.indexName);
            try {
                if(!client.indices().exists(getIndexRequest)) {
                    createDataMapping(rsInfo);
                }
            } catch (IOException ex) {
                logger.error("Cannot create metadata mapping", ex);
            }

            storeChanged = System.currentTimeMillis();

            refreshIndex();

        } catch (IOException ex) {
            logger.error(String.format("addRecordStore exception %s:%s in elastic search driver",name, recordStructure.getName()), ex);
        }
	}

	@Override
	public int getNumRecords(String recordType) {
	    commit();

        Map<String, EsRecordStoreInfo>  recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(recordType);
        if(info != null) {
            SearchRequest searchRequest = new SearchRequest(info.indexName);
            searchRequest.source(new SearchSourceBuilder().size(0)
                    .query(new BoolQueryBuilder()
                            .must(new TermQueryBuilder(STORAGE_ID_FIELD_NAME, config.id))));
            try {
                SearchResponse response = client.search(searchRequest);
                try {
                    return Math.toIntExact(response.getHits().getTotalHits());
                } catch (ArithmeticException ex) {
                    logger.error("Too many records");
                    return Integer.MAX_VALUE;
                }
            } catch (IOException | ElasticsearchStatusException ex) {
                log.error("getRecordStores failed", ex);
            }
        }
        return 0;
	}

	@Override  
	public synchronized double[] getRecordsTimeRange(String recordType) {
		double[] result = new double[2];

        Map<String, EsRecordStoreInfo>  recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(recordType);
        if(info != null) {
            SearchRequest searchRequest = new SearchRequest(info.indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.ASC));
            searchSourceBuilder.size(1);
            searchRequest.source(searchSourceBuilder);

            try {

                // build request to get the least recent record
                SearchResponse response = client.search(searchRequest);

                if (response.getHits().getTotalHits() > 0) {
                    result[0] = ESDataStoreTemplate.fromEpochMillisecond((Number) response.getHits().getAt(0).getSourceAsMap().get( ESDataStoreTemplate.TIMESTAMP_FIELD_NAME));
                }

                // build request to get the most recent record
                searchRequest = new SearchRequest(info.indexName);
                searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.matchAllQuery());
                searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.DESC));
                searchSourceBuilder.size(1);
                searchRequest.source(searchSourceBuilder);

                response = client.search(searchRequest);

                if (response.getHits().getTotalHits() > 0) {
                    result[1] = ESDataStoreTemplate.fromEpochMillisecond((Number) response.getHits().getAt(0).getSourceAsMap().get( ESDataStoreTemplate.TIMESTAMP_FIELD_NAME));
                }
            } catch (IOException ex) {
                log.error("getRecordsTimeRange failed", ex);
            }
        }
		return result;
	}

	@Override
	public Iterator<double[]> getRecordsTimeClusters(String recordType) {
        Map<String, EsRecordStoreInfo> recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(recordType);
        if (info != null) {
            SearchRequest searchRequest = new SearchRequest(info.indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.boolQuery().must(
                    QueryBuilders.termQuery(STORAGE_ID_FIELD_NAME, config.id))
                    .must(QueryBuilders.rangeQuery(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME)
                            .from(ESDataStoreTemplate.toEpochMillisecond(System.currentTimeMillis() - TIME_RANGE_CLUSTER_SCROLL_FETCH_SIZE)).format("epoch_millis")));
            searchSourceBuilder.size(config.scrollFetchSize);
            searchSourceBuilder.fetchSource(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME, null);
            searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.ASC));
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));

            final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, searchRequest, TimeValue.timeValueMillis(config.scrollMaxDuration)); //max of scrollFetchSize hits will be returned for each scroll

            // build a IDataRecord iterator based on the searchHits iterator

            return new Iterator<double[]>() {
                Double lastTime = Double.NaN;

                @Override
                public boolean hasNext() {
                    return searchHitsIterator.hasNext();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public double[] next() {
                    double[] clusterTimeRange = new double[2];
                    clusterTimeRange[0] = lastTime;

                    SearchHit nextSearchHit = null;
                    double recTime;
                    double dt;

                    while (searchHitsIterator.hasNext()) {
                        nextSearchHit = searchHitsIterator.next();
                        recTime = ESDataStoreTemplate.fromEpochMillisecond((Number) nextSearchHit.getSourceAsMap().get(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME));

                        synchronized (this) {
                            if (Double.isNaN(lastTime)) {
                                clusterTimeRange[0] = recTime;
                                lastTime = recTime;
                            } else {
                                dt = recTime - lastTime;
                                lastTime = recTime;
                                if (dt > MAX_TIME_CLUSTER_DELTA)
                                    break;
                            }
                        }
                        clusterTimeRange[1] = recTime;
                    }
                    return clusterTimeRange;
                }
            };
        } else {
            return Collections.emptyIterator();
        }
	}

	void dataSimpleComponent(SimpleComponent dataComponent, Map data,int i, DataBlock dataBlock) {
        switch (dataComponent.getDataType()) {
            case FLOAT:
                dataBlock.setFloatValue(i, ((Number)data.get(dataComponent.getName())).floatValue());
                break;
            case DOUBLE:
                dataBlock.setDoubleValue(i, ((Number)data.get(dataComponent.getName())).doubleValue());
                break;
            case SHORT:
            case USHORT:
            case UINT:
            case INT:
                dataBlock.setIntValue(i, ((Number)data.get(dataComponent.getName())).intValue());
                break;
            case ASCII_STRING:
            case UTF_STRING:
                dataBlock.setStringValue(i, (String)data.get(dataComponent.getName()));
                break;
            case BOOLEAN:
                dataBlock.setBooleanValue(i, (Boolean) data.get(dataComponent.getName()));
                break;
            case ULONG:
            case LONG:
                dataBlock.setLongValue(i,((Number)data.get(dataComponent.getName())).longValue());
                break;
            case UBYTE:
            case BYTE:
                dataBlock.setByteValue(i, ((Number)data.get(dataComponent.getName())).byteValue());
                break;
            default:
                logger.error("Unsupported type " + ((SimpleComponent) dataComponent).getDataType());
        }
    }

	DataBlock dataBlockFromES(DataComponent component, Map data, DataBlock dataBlock, int j) {
	    if(dataBlock == null) {
            dataBlock = component.createDataBlock();
        }
        if(component instanceof SimpleComponent) {
            dataSimpleComponent((SimpleComponent) component, data, 0, dataBlock);
        } else {
	        final int arraySize = component.getComponentCount();
            for (int i = 0; i < arraySize; i++) {
                if (component.getComponent(i) instanceof SimpleComponent) {
                    dataSimpleComponent((SimpleComponent) component.getComponent(i), data,arraySize * j + i, dataBlock);
                } else {
                    dataBlockFromES(component.getComponent(i), (Map)((List)data.get(component.getComponent(i).getName())).get(i), dataBlock, i);
                }
            }
        }
        return dataBlock;
    }

	@Override
	public DataBlock getDataBlock(DataKey key) {
        DataBlock result = null;
        Map<String, EsRecordStoreInfo>  recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(key.recordType);
        if(info != null) {
            // build the key as recordTYpe_timestamp_producerID
            String esKey = getRsKey(key);

            // build the request
            GetRequest getRequest = new GetRequest(info.indexName, info.name, esKey);
            try {
                // build  and execute the response
                GetResponse response = client.get(getRequest);

                // deserialize the blob field from the response if any
                if (response.isExists()) {
                    result = dataBlockFromES(info.recordDescription, response.getSourceAsMap(), null, 0);
                }
            } catch (IOException ex) {
                log.error(ex.getLocalizedMessage(), ex);
            }
        }
		return result;
	}

	@Override
	public Iterator<DataBlock> getDataBlockIterator(IDataFilter filter) {

        Map<String, EsRecordStoreInfo> recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(filter.getRecordType());
        if (info != null) {
            SearchRequest searchRequest = new SearchRequest(info.indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(queryByFilter(filter));
            searchSourceBuilder.size(config.scrollFetchSize);
            searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.ASC));
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));

            final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, searchRequest, TimeValue.timeValueMillis(config.scrollMaxDuration)); //max of scrollFetchSize hits will be returned for each scroll

            // build a DataBlock iterator based on the searchHits iterator

            return new Iterator<DataBlock>() {

                @Override
                public boolean hasNext() {
                    return searchHitsIterator.hasNext();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public DataBlock next() {
                    SearchHit nextSearchHit = searchHitsIterator.next();

                    return dataBlockFromES(info.recordDescription, nextSearchHit.getSourceAsMap(), null, 0);
                }
            };
        } else {
            return Collections.emptyIterator();
        }
	}

    Iterator<? extends IDataRecord> recordIteratorFromESQueryFilter(IDataFilter filter, BoolQueryBuilder esFilter) {

        Map<String, EsRecordStoreInfo> recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(filter.getRecordType());
        if (info != null) {
            SearchRequest searchRequest = new SearchRequest(info.indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(esFilter);
            searchSourceBuilder.size(config.scrollFetchSize);
            searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.ASC));
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));

            final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, searchRequest, TimeValue.timeValueMillis(config.scrollMaxDuration)); //max of scrollFetchSize hits will be returned for each scroll

            // build a IDataRecord iterator based on the searchHits iterator

            return new Iterator<IDataRecord>() {

                @Override
                public boolean hasNext() {
                    return searchHitsIterator.hasNext();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public IDataRecord next() {
                    SearchHit nextSearchHit = searchHitsIterator.next();

                    Map<String, Object> queryResult = nextSearchHit.getSourceAsMap();
                    // build key
                    final DataKey key = getDataKey(nextSearchHit.getId(), queryResult);

                    final DataBlock datablock = dataBlockFromES(info.recordDescription, queryResult, null, 0);

                    return new IDataRecord() {

                        @Override
                        public DataKey getKey() {
                            return key;
                        }

                        @Override
                        public DataBlock getData() {
                            return datablock;
                        }

                    };
                }
            };
        } else {
            return Collections.emptyIterator();
        }

    }

    @Override
	public Iterator<? extends IDataRecord> getRecordIterator(IDataFilter filter) {
        return recordIteratorFromESQueryFilter(filter, queryByFilter(filter));
	}
	
	@Override
	public int getNumMatchingRecords(IDataFilter filter, long maxCount) {
        int result = 0;
        Map<String, EsRecordStoreInfo> recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(filter.getRecordType());
        if (info != null) {
            SearchRequest searchRequest = new SearchRequest(info.indexName);
            searchRequest.source(new SearchSourceBuilder().size(0)
                    .query(queryByFilter(filter)));
            try {
                SearchResponse response = client.search(searchRequest);
                try {
                    return Math.toIntExact(Math.min(response.getHits().getTotalHits(), maxCount));
                } catch (ArithmeticException ex) {
                    logger.error("Too many records");
                    return Integer.MAX_VALUE;
                }
            } catch (IOException | ElasticsearchStatusException ex) {
                log.error("getRecordStores failed", ex);
            }
        }
        return result;
	}



	private void parseDataMapping(XContentBuilder builder, DataComponent dataComponent) throws IOException {
        if (dataComponent instanceof SimpleComponent) {
            switch (((SimpleComponent) dataComponent).getDataType()) {
                case FLOAT:
                    builder.startObject(dataComponent.getName());
                    // TODO When Quantity will contains a precision information
                    // builder.field("type", "scaled_float");
                    // builder.field("index", false);
                    // builder.field("scaling_factor", 100);
                    builder.field("type", "float");
                    builder.endObject();
                    break;
                case DOUBLE:
                    builder.startObject(dataComponent.getName());
                    builder.field("type", "double");
                    builder.endObject();
                    break;
                case SHORT:
                    builder.startObject(dataComponent.getName());
                    builder.field("type", "short");
                    builder.endObject();
                    break;
                case USHORT:
                case UINT:
                case INT:
                    builder.startObject(dataComponent.getName());
                    builder.field("type", "integer");
                    builder.endObject();
                    break;
                case ASCII_STRING:
                    builder.startObject(dataComponent.getName());
                    builder.field("type", "keyword");
                    builder.endObject();
                    break;
                case UTF_STRING:
                    builder.startObject(dataComponent.getName());
                    builder.field("type", "text");
                    builder.endObject();
                    break;
                case BOOLEAN:
                    builder.startObject(dataComponent.getName());
                    builder.field("type", "boolean");
                    builder.endObject();
                    break;
                case ULONG:
                case LONG:
                    builder.startObject(dataComponent.getName());
                    builder.field("type", "long");
                    builder.endObject();
                    break;
                case UBYTE:
                case BYTE:
                    builder.startObject(dataComponent.getName());
                    builder.field("type", "byte");
                    builder.endObject();
                    break;
                default:
                    logger.error("Unsupported type " + ((SimpleComponent) dataComponent).getDataType());
            }
        } else if(dataComponent instanceof DataRecord) {
            for(int i = 0; i < dataComponent.getComponentCount(); i++) {
                DataComponent component = dataComponent.getComponent(i);
                parseDataMapping(builder, component);
            }
        } else if(dataComponent instanceof DataArray){
            builder.startObject(((DataArray) dataComponent).getElementType().getName());
            {
                builder.field("type", "nested");
                builder.field("dynamic", false);
                builder.startObject("properties");
                {
                    parseDataMapping(builder, ((DataArray) dataComponent).getElementType());
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }


    /**
     * Override this method to add special fields in es data mapping
     * @param builder
     * @throws IOException
     */
    void createDataMappingFields(XContentBuilder builder) throws IOException {
        builder.startObject(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME);
        {
            builder.field("type", "keyword");
        }
        builder.endObject();
        builder.startObject(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME);
        {
            // Issue with date https://discuss.elastic.co/t/weird-issue-with-date-sort/137646
            builder.field("type", "date");
            builder.field("format", "epoch_millis");
        }
        builder.endObject();
        builder.startObject(STORAGE_ID_FIELD_NAME);
        {
            builder.field("type", "keyword");
        }
        builder.endObject();
    }

    /**
     * @param rsInfo record store metadata
     * @throws IOException
     */
    void createDataMapping(EsRecordStoreInfo rsInfo) throws IOException {

        // create the index
        CreateIndexRequest indexRequest = new CreateIndexRequest(rsInfo.indexName);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject(rsInfo.name);
            {
                builder.field("dynamic", false);
                builder.startObject("properties");
                {
                    createDataMappingFields(builder);
                    DataComponent dataComponent = rsInfo.getRecordDescription();
                    parseDataMapping(builder, dataComponent);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        indexRequest.mapping(rsInfo.name, builder);

        client.indices().create(indexRequest);

        addedIndex.add(rsInfo.indexName);
    }

    public List<String> getAddedIndex() {
        return Collections.unmodifiableList(addedIndex);
    }

    void dataComponentSimpleToJson(SimpleComponent component, DataBlock data, int i, XContentBuilder builder) throws IOException {
        switch (data.getDataType(i)) {
            case FLOAT:
                builder.field(component.getName(), data.getFloatValue(i));
                break;
            case DOUBLE:
                builder.field(component.getName(), data.getDoubleValue(i));
                break;
            case SHORT:
            case USHORT:
            case UINT:
            case INT:
                builder.field(component.getName(), data.getIntValue(i));
                break;
            case ASCII_STRING:
            case UTF_STRING:
                builder.field(component.getName(), data.getStringValue(i));
                break;
            case BOOLEAN:
                builder.field(component.getName(), data.getBooleanValue(i));
                break;
            case ULONG:
            case LONG:
                builder.field(component.getName(), data.getLongValue(i));
                break;
            case UBYTE:
            case BYTE:
                builder.field(component.getName(), data.getByteValue(i));
                break;
            case OTHER:
                builder.field(component.getName(), data.getUnderlyingObject());
                break;
            default:
                logger.error("Unsupported type " + data.getDataType(i).name());
        }
    }

    void dataComponentToJson(DataComponent dataComponent, DataBlock data, XContentBuilder builder, int nj, int j) throws IOException {
        if(dataComponent instanceof SimpleComponent) {
            dataComponentSimpleToJson((SimpleComponent) dataComponent, data, j, builder);
        } else {
            int compSize = dataComponent.getComponentCount();
            if(dataComponent instanceof DataArray) {
                builder.startArray(((DataArray) dataComponent).getElementType().getName());
            }
            for (int i = 0; i < compSize; i++) {
                if (dataComponent.getComponent(i) instanceof SimpleComponent) {
                    dataComponentSimpleToJson((SimpleComponent) dataComponent.getComponent(i), data, j*compSize + i, builder);
                } else {
                    builder.startObject();
                    if(data instanceof DataBlockParallel) {
                        dataComponentToJson(dataComponent.getComponent(i), data, builder, compSize, i);
                    }
                    builder.endObject();
                }
            }
            if(dataComponent instanceof DataArray) {
                builder.endArray();
            }
        }
    }

    void storeRecordIndexRequestFields(XContentBuilder builder, DataKey key) throws IOException {
        builder.field(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME, key.producerID);
        builder.field(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME, ESDataStoreTemplate.toEpochMillisecond(key.timeStamp));
        builder.field(STORAGE_ID_FIELD_NAME, config.id);
    }

    IndexRequest storeRecordIndexRequest(DataKey key, DataBlock data) throws IOException {
        IndexRequest request = null;

        Map<String, EsRecordStoreInfo>  recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(key.recordType);
        if(info != null) {
            XContentBuilder builder = XContentFactory.jsonBuilder();

            builder.startObject();
            {
                storeRecordIndexRequestFields(builder, key);
                DataComponent dataComponent = info.getRecordDescription();
                dataComponentToJson(dataComponent, data, builder, 1, 0);
            }
            builder.endObject();

            request = new IndexRequest(info.getIndexName(), info.name, getRsKey(key));

            request.source(builder);
        }
	    return request;
    }

    @Override
	public void storeRecord(DataKey key, DataBlock data) {
        try {
            IndexRequest request = storeRecordIndexRequest(key, data);
            if(request != null) {
                bulkProcessor.add(request);
            } else {
                log.error("Missing record store " + key.recordType);
            }
        } catch (IOException ex) {
            log.error("Cannot create json data storeRecord", ex);
        }

        storeChanged = System.currentTimeMillis();
    }

	@Override
	public void updateRecord(DataKey key, DataBlock data) {
        // Key handle duplicates
        storeRecord(key, data);
	}

	@Override
	public void removeRecord(DataKey key) {
        Map<String, EsRecordStoreInfo>  recordStoreInfoMap = getRecordStores();
        EsRecordStoreInfo info = recordStoreInfoMap.get(key.recordType);
        if(info != null) {
            // build the key as recordTYpe_timestamp_producerID
            String esKey = getRsKey(key);

            // prepare delete request
            DeleteRequest deleteRequest = new DeleteRequest(info.indexName, info.name, esKey);
            bulkProcessor.add(deleteRequest);
        }
	}

	private static String encodeEndPoint(String... params) throws IOException {
	    StringBuilder s = new StringBuilder();
	    for(String param : params) {
	        if(s.length() > 0) {
	            s.append("/");
            }
            s.append(URLEncoder.encode(param, "UTF-8"));
        }
	    return s.toString();
    }

    /**
     * Convert OSH filter to ElasticSearch filter
     * @param filter OSH filter
     * @return ElasticSearch query object
     */
    BoolQueryBuilder queryByFilter(IDataFilter filter) {
        double[] timeRange = getTimeRange(filter);

        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(STORAGE_ID_FIELD_NAME, config.id))
                .must(new RangeQueryBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME)
                        .from(ESDataStoreTemplate.toEpochMillisecond(timeRange[0]))
                        .to(ESDataStoreTemplate.toEpochMillisecond(timeRange[1])).format("epoch_millis"));

        // check if any producerIDs
        if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
            query.must(QueryBuilders.termsQuery(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
        }

        return query;
    }

	@Override
	public int removeRecords(IDataFilter filter) {
		try {
            Map<String, EsRecordStoreInfo>  recordStoreInfoMap = getRecordStores();
            EsRecordStoreInfo info = recordStoreInfoMap.get(filter.getRecordType());
            if(info != null) {
                // Delete by query, currently not supported by High Level Api

                BoolQueryBuilder query = queryByFilter(filter);

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                XContentBuilder builder = XContentFactory.jsonBuilder(bos);
                builder.startObject();
                builder.rawField("query", new ByteArrayInputStream(query.toString().getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
                builder.endObject();
                builder.flush();
                String json = bos.toString("UTF-8");
                HttpEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON);
                Response response = client.getLowLevelClient().performRequest("POST", encodeEndPoint(info.indexName, "_delete_by_query"),Collections.EMPTY_MAP, entity);
                String source = EntityUtils.toString(response.getEntity());
                Map<String, Object> content = XContentHelper.convertToMap(XContentFactory.xContent(XContentType.JSON), source, true);

                storeChanged = System.currentTimeMillis();

                return ((Number)content.get("total")).intValue();
            }
        } catch (IOException ex) {
		  log.error("Failed to removeRecords", ex);
        }
        return 0;
	}

	/**
	 * Get a serialized object from an object.
	 * The object is serialized using Kryo.
	 * @param object The raw object
	 * @return the serialized object
	 */
	protected static <T> byte[] getBlob(T object){
		return KryoSerializer.serialize(object);
	}

	/**
	 * Get an object from a base64 encoding String.
	 * The object is deserialized using Kryo.
	 * @param blob The base64 encoding String
	 * @return The deserialized object
	 */
	protected static <T> T getObject(Object blob) {
		// Base 64 decoding
		byte [] base64decodedData = Base64.decodeBase64(blob.toString().getBytes());
		// Kryo deserialize
		return KryoSerializer.<T>deserialize(base64decodedData);
	}


	/**
	 * Transform a DataKey into an ES key as: <recordtype><SEPARATOR><timestamp><SEPARATOR><producerID>.
	 * @param key the ES key.
	 * @return the ES key. 
	 */
	protected String getRsKey(DataKey key) {
		return key.recordType+RS_KEY_SEPARATOR+Double.doubleToLongBits(key.timeStamp)+RS_KEY_SEPARATOR+key.producerID;
	}
	
	/**
	 * Transform the recordStorage data key into a DataKey by splitting <recordtype><SEPARATOR><timestamp><SEPARATOR><producerID>.
	 * @param rsKey the corresponding dataKey
	 * @return the dataKey. NULL if the length != 3 after splitting
	 */
	protected DataKey getDataKey(String rsKey, Map<String, Object> content) {
		DataKey dataKey = null;
		
		// split the rsKey using separator
		String [] split = rsKey.split(RS_KEY_SEPARATOR);
		
		// must find <recordtype><SEPARATOR><timestamp><SEPARATOR><producerID>
    	if(split.length == 3) {
    		dataKey = new DataKey(split[0], split[2], Double.longBitsToDouble(Long.parseLong(split[1])));
    	}
		return dataKey;
	}

	protected double[] getTimeRange(IDataFilter filter) {
		double[] timeRange = filter.getTimeStampRange();
		if (timeRange != null)
			return timeRange;
		else
			return ALL_TIMES;
	}
	
	/**
	 * Refreshes the index.
	 */
	protected void refreshIndex() {
        log.info("ESBasicStorageImpl:refreshIndex");
        bulkProcessor.flush();
        RefreshRequest refreshRequest = new RefreshRequest();
        try {
            client.indices().refresh(refreshRequest);
        } catch (IOException ex) {
            logger.error("Error while refreshIndex", ex);
        }
	}


    @Override
    public boolean isReadSupported() {
        return true;
    }

    @Override
    public boolean isWriteSupported() {
        return true;
    }

    private static final class BulkListener implements BulkProcessor.Listener {
	    Logger logger = LoggerFactory.getLogger(BulkListener.class);

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("Exception while pushing data to ElasticSearch", failure);
        }
    }
}
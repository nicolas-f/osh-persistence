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
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.lucene.queryparser.surround.query.AndQuery;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.persistence.DataKey;
import org.sensorhub.api.persistence.IDataFilter;
import org.sensorhub.api.persistence.IDataRecord;
import org.sensorhub.api.persistence.IObsStorage;
import org.sensorhub.api.persistence.IRecordStorageModule;
import org.sensorhub.api.persistence.IRecordStoreInfo;
import org.sensorhub.api.persistence.IStorageModule;
import org.sensorhub.api.persistence.StorageException;
import org.sensorhub.impl.module.AbstractModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
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
	protected RestHighLevelClient client;

	private BulkProcessor bulkProcessor;
	
	/**
	 * The data index. The data are indexed by their timestamps
	 * UUID_SOURCE/RECORD_STORE_ID/{ timestamp: <timestamp>, data: <anyData> }
	 */
	protected static final String DESC_HISTORY_IDX_NAME = "desc";
	protected static final String RS_INFO_IDX_NAME = "info";

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

	@Override
	public void restore(InputStream is) throws IOException {
		throw new UnsupportedOperationException("Restore");
	}

	@Override
	public void commit() {
		refreshIndex();
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


	protected void createMetaMapping () throws IOException {
		// create the index
	    CreateIndexRequest indexRequest = new CreateIndexRequest(indexNameMetaData);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject(INDEX_METADATA_TYPE);
            {
                builder.field("dynamic", false);
                builder.startObject("properties");
                {
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
                    }
                    builder.endObject();
                    builder.startObject(BLOB_FIELD_NAME);
                    {
                        builder.field("type", "binary");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

	    indexRequest.mapping(INDEX_METADATA_TYPE, builder);

	    client.indices().create(indexRequest);
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
                .must(new TermQueryBuilder(METADATA_TYPE_FIELD_NAME, DESC_HISTORY_IDX_NAME));
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
                .must(new TermQueryBuilder(METADATA_TYPE_FIELD_NAME, DESC_HISTORY_IDX_NAME))
                .must(new RangeQueryBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).from(Double.valueOf(startTime * 1000).longValue()).to(Double.valueOf(endTime * 1000).longValue()));
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
                .must(new TermQueryBuilder(METADATA_TYPE_FIELD_NAME, DESC_HISTORY_IDX_NAME))
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
            long epoch = Double.valueOf(time * 1000).longValue();
            builder.startObject();
            {
                builder.field(STORAGE_ID_FIELD_NAME, config.id);
                builder.field(METADATA_TYPE_FIELD_NAME, DESC_HISTORY_IDX_NAME);
                builder.field(DATA_INDEX_FIELD_NAME, "");
                builder.timeField(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME, epoch);
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
        log.info("ESBasicStorageImpl:removeDataSourceDescription");
//		DeleteRequest deleteRequest = new DeleteRequest(indexNamePrepend, DESC_HISTORY_IDX_NAME, Double.toString(time));
//		bulkProcessor.add(deleteRequest);
	}

	@Override
	public void removeDataSourceDescriptionHistory(double startTime, double endTime) {
        log.info("ESBasicStorageImpl:removeDataSourceDescriptionHistory");
//		// query ES to get the corresponding timestamp
//		// the response is applied a post filter allowing to specify a range request on the timestamp
//		// the hits should be directly filtered
//		SearchResponse response = client.prepareSearch(indexNamePrepend).setTypes(DESC_HISTORY_IDX_NAME)
//				.setPostFilter(QueryBuilders.rangeQuery(TIMESTAMP_FIELD_NAME)
//					.from(startTime).to(endTime)) // Query
//				.setFetchSource(new String[]{}, new String[]{"*"}) // does not fetch source
//		        .get();
//
//		// the corresponding filtering hits
//		for(SearchHit hit : response.getHits()) {
//			DeleteRequest deleteRequest = new DeleteRequest(indexNamePrepend, DESC_HISTORY_IDX_NAME,hit.getId());
//			bulkProcessor.add(deleteRequest);
//		}
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
                .must(new TermQueryBuilder(METADATA_TYPE_FIELD_NAME, RS_INFO_IDX_NAME));
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


//		SearchResponse response = client.prepareSearch(indexNamePrepend).setTypes(RS_INFO_IDX_NAME).get();
//
//		String name = null;
//		DataStreamInfo rsInfo = null;
//		for(SearchHit hit : response.getHits()) {
//			name = hit.getId(); // name
//			rsInfo = this.<DataStreamInfo>getObject(hit.getSourceAsMap().get(BLOB_FIELD_NAME)); // DataStreamInfo
//			result.put(name,rsInfo);
//		}
//		return result;
	}

	@Override
	public void addRecordStore(String name, DataComponent recordStructure, DataEncoding recommendedEncoding) {
        log.info("ESBasicStorageImpl:addRecordStore");
        EsRecordStoreInfo rsInfo = new EsRecordStoreInfo(name,indexNamePrepend + recordStructure.getName(),
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
                builder.field(METADATA_TYPE_FIELD_NAME, RS_INFO_IDX_NAME);
                builder.field(DATA_INDEX_FIELD_NAME, rsInfo.getIndexName()); // store recordType
                builder.timeField(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME, System.currentTimeMillis());
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

            refreshIndex();

        } catch (IOException ex) {
            logger.error(String.format("addRecordStore exception %s:%s in elastic search driver",name, recordStructure.getName()), ex);
        }

//
//		Map<String, Object> json = new HashMap<>();
//		json.put(BLOB_FIELD_NAME,blob);
//
//		// set id and blob before executing the request
//		//String id = client.prepareIndex(indexNamePrepend,RS_INFO_IDX_NAME).setId(name).setSource(json).get().getId();
//		bulkProcessor.add(client.prepareIndex(indexNamePrepend,RS_INFO_IDX_NAME).setId(name).setSource(json).request());
//		//TODO: make the link to the recordStore storage
//		// either we can use an intermediate mapping table or use directly the recordStoreInfo index
//		// to fetch the corresponding description
//
//		// To test: try to use the recordStoreInfo index directly
//		// do nothing
	}

	@Override
	public int getNumRecords(String recordType) {
        log.info("ESBasicStorageImpl:getNumRecords");
	    return 0;
//		SearchResponse response = client.prepareSearch(indexNamePrepend).setTypes(RS_DATA_IDX_NAME)
//				.setPostFilter(QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, recordType))
//				.setFetchSource(new String[]{}, new String[]{"*"}) // does not fetch source
//		        .get();
//		return (int) response.getHits().getTotalHits();
	}

	@Override  
	public synchronized double[] getRecordsTimeRange(String recordType) {
        log.info("ESBasicStorageImpl:getRecordsTimeRange");
	    return null;
//
//		double[] result = new double[2];
//
//		// build request to get the least recent record
//		SearchResponse response = client.prepareSearch(indexNamePrepend).setTypes(RS_DATA_IDX_NAME)
//				.setQuery(QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, recordType))
//				.addSort(TIMESTAMP_FIELD_NAME, SortOrder.ASC) // sort results by DESC timestamp
//				.setFetchSource(new String[]{TIMESTAMP_FIELD_NAME}, new String[]{}) // get only the timestamp
//				.setSize(1) // fetch only 1 result
//		        .get();
//
//		if(response.getHits().getTotalHits()> 0) {
//			result[0] = (double) response.getHits().getAt(0).getSourceAsMap().get(TIMESTAMP_FIELD_NAME);
//		}
//
//		// build request to get the most recent record
//		 response = client.prepareSearch(indexNamePrepend).setTypes(RS_DATA_IDX_NAME)
//				.setQuery(QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, recordType))
//				.addSort(TIMESTAMP_FIELD_NAME, SortOrder.DESC) // sort results by DESC timestamp
//				.setFetchSource(new String[]{TIMESTAMP_FIELD_NAME}, new String[]{}) // get only the timestamp
//				//.setSize(1) // fetch only 1 result
//		        .get();
//
//		if(response.getHits().getTotalHits()> 0) {
//			result[1] = (double) response.getHits().getAt(0).getSourceAsMap().get(TIMESTAMP_FIELD_NAME);
//		}
//
//		return result;
	}

	@Override
	public Iterator<double[]> getRecordsTimeClusters(String recordType) {
        log.info("ESBasicStorageImpl:getRecordsTimeClusters");
	    return null;
//		// build response
//		final SearchRequestBuilder scrollReq = client.prepareSearch(indexNamePrepend).setTypes(RS_DATA_IDX_NAME)
//				//TOCHECK
//				//.addSort(SortOrder.ASC)
//				.addSort(TIMESTAMP_FIELD_NAME, SortOrder.ASC)
//		        .setScroll(new TimeValue(config.pingTimeout))
//		        .setRequestCache(true)
//		        .setQuery(QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, recordType))
//		        .setFetchSource(new String[]{TIMESTAMP_FIELD_NAME}, new String[]{}); // get only the timestamp
//
//        // wrap the request into custom ES Scroll iterator
//		final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, scrollReq,
//				TIME_RANGE_CLUSTER_SCROLL_FETCH_SIZE); //max of scrollFetchSize hits will be returned for each scroll
//
//		return new Iterator<double[]>() {
//			Double lastTime = Double.NaN;
//
//			@Override
//			public boolean hasNext() {
//				return searchHitsIterator.hasNext();
//			}
//
//			@Override
//			public void remove() {
//
//			}
//
//			@Override
//			public double[] next() {
//				double[] clusterTimeRange = new double[2];
//                clusterTimeRange[0] = lastTime;
//
//                SearchHit nextSearchHit = null;
//                double recTime;
//                double dt;
//
//				while (searchHitsIterator.hasNext()) {
//					nextSearchHit = searchHitsIterator.next();
//					recTime = (double) nextSearchHit.getSourceAsMap().get(TIMESTAMP_FIELD_NAME);
//
//					synchronized (this) {
//						if (Double.isNaN(lastTime)) {
//							clusterTimeRange[0] = recTime;
//							lastTime = recTime;
//						} else {
//							 dt = recTime - lastTime;
//							lastTime = recTime;
//							if (dt > MAX_TIME_CLUSTER_DELTA)
//								break;
//						}
//					}
//					clusterTimeRange[1] = recTime;
//				}
//				return clusterTimeRange;
//			}
//		};
	}

	@Override
	public DataBlock getDataBlock(DataKey key) {
        log.info("ESBasicStorageImpl:getDataBlock");
	    return null;
//		DataBlock result = null;
//		// build the key as recordTYpe_timestamp_producerID
//		String esKey = getRsKey(key);
//
//		// build the request
//		GetRequest getRequest = new GetRequest(indexNamePrepend,RS_DATA_IDX_NAME,esKey);
//
//		// build  and execute the response
//		GetResponse response = client.get(getRequest).actionGet();
//
//		// deserialize the blob field from the response if any
//		if(response.isExists()) {
//			result = this.<DataBlock>getObject(response.getSource().get(BLOB_FIELD_NAME)); // DataBlock
//		}
//		return result;
	}

	@Override
	public Iterator<DataBlock> getDataBlockIterator(IDataFilter filter) {
        log.info("ESBasicStorageImpl:getDataBlockIterator");
	    return null;
//		double[] timeRange = getTimeRange(filter);
//
//		// prepare filter
//		QueryBuilder timeStampRangeQuery = QueryBuilders.rangeQuery(TIMESTAMP_FIELD_NAME).from(timeRange[0]).to(timeRange[1]);
//		QueryBuilder recordTypeQuery = QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, filter.getRecordType());
//
//		// aggregate queries
//		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery()
//				.must(timeStampRangeQuery);
//
//		// check if any producerIDs
//		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
//			filterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
//		}
//
//		// build response
//		final SearchRequestBuilder scrollReq = client.prepareSearch(indexNamePrepend).setTypes(RS_DATA_IDX_NAME)
//				//TOCHECK
//				.addSort(TIMESTAMP_FIELD_NAME, SortOrder.ASC)
//				.setFetchSource(new String[]{BLOB_FIELD_NAME}, new String[]{}) // get only the BLOB
//		        .setScroll(new TimeValue(config.pingTimeout))
//		        .setQuery(recordTypeQuery)
//		        .setRequestCache(true)
//		        .setPostFilter(filterQueryBuilder);
//
//		// wrap the request into custom ES Scroll iterator
//		final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, scrollReq,
//				config.scrollFetchSize); //max of scrollFetchSize hits will be returned for each scroll
//
//		// build a datablock iterator based on the searchHits iterator
//		return new Iterator<DataBlock>(){
//
//			@Override
//			public boolean hasNext() {
//				return searchHitsIterator.hasNext();
//			}
//
//			@Override
//			public void remove() {
//
//			}
//
//			@Override
//			public DataBlock next() {
//				SearchHit nextSearchHit = searchHitsIterator.next();
//				// get DataBlock from blob
//				Object blob = nextSearchHit.getSourceAsMap().get(BLOB_FIELD_NAME);
//				return ESBasicStorageImpl.this.<DataBlock>getObject(blob); // DataBlock
//			}
//		};
	}
	
	@Override
	public Iterator<? extends IDataRecord> getRecordIterator(IDataFilter filter) {
        log.info("ESBasicStorageImpl:getRecordIterator");
	    return null;
//		double[] timeRange = getTimeRange(filter);
//
//		// prepare filter
//		QueryBuilder timeStampRangeQuery = QueryBuilders.rangeQuery(TIMESTAMP_FIELD_NAME).from(timeRange[0]).to(timeRange[1]);
//		QueryBuilder recordTypeQuery = QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, filter.getRecordType());
//
//		// aggregate queries
//		BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
//				.must(timeStampRangeQuery)
//				.must(recordTypeQuery);
//
//		// check if any producerIDs
//		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
//			queryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
//		}
//
//		// build response
//		final SearchRequestBuilder scrollReq = client.prepareSearch(indexNamePrepend).setTypes(RS_DATA_IDX_NAME)
//				.addSort(TIMESTAMP_FIELD_NAME, SortOrder.ASC)
//		        .setScroll(new TimeValue(config.pingTimeout))
//		        .setQuery(recordTypeQuery)
//		        //.setRequestCache(true)
//		        .setPostFilter(queryBuilder);
//
//        // wrap the request into custom ES Scroll iterator
//		final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, scrollReq,
//				config.scrollFetchSize); //max of scrollFetchSize hits will be returned for each scroll
//
//		// build a datablock iterator based on the searchHits iterator
//
//		return new Iterator<IDataRecord>(){
//
//			@Override
//			public boolean hasNext() {
//				return searchHitsIterator.hasNext();
//			}
//
//			@Override
//			public void remove() {
//
//			}
//
//			@Override
//			public IDataRecord next() {
//				SearchHit nextSearchHit = searchHitsIterator.next();
//
//				// build key
//				final DataKey key = getDataKey(nextSearchHit.getId());
//				key.timeStamp = (double) nextSearchHit.getSourceAsMap().get(TIMESTAMP_FIELD_NAME);
//				// get DataBlock from blob
//				final DataBlock datablock=ESBasicStorageImpl.this.<DataBlock>getObject(nextSearchHit.getSourceAsMap().get(BLOB_FIELD_NAME)); // DataBlock
//				return new IDataRecord(){
//
//					@Override
//					public DataKey getKey() {
//						return key;
//					}
//
//					@Override
//					public DataBlock getData() {
//						return datablock;
//					}
//
//				};
//			}
//		};
	}
	
	@Override
	public int getNumMatchingRecords(IDataFilter filter, long maxCount) {
        log.info("ESBasicStorageImpl:getNumMatchingRecords");
	    return 0;
//		double[] timeRange = getTimeRange(filter);
//
//		// aggregate queries
//		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery();
//
//		// prepare filter
//		QueryBuilder timeStampRangeQuery = QueryBuilders.rangeQuery(TIMESTAMP_FIELD_NAME).from(timeRange[0]).to(timeRange[1]);
//		QueryBuilder recordTypeQuery = QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, filter.getRecordType());
//
//		filterQueryBuilder.must(timeStampRangeQuery);
//
//		// check if any producerIDs
//		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
//			filterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
//		}
//
//		// build response
//		final SearchResponse scrollResp = client.prepareSearch(indexNamePrepend).setTypes(RS_DATA_IDX_NAME)
//		        .setScroll(new TimeValue(config.pingTimeout))
//		        .setQuery(recordTypeQuery)
//		        .setRequestCache(true)
//		        .setPostFilter(filterQueryBuilder)
//		        .setFetchSource(new String[]{}, new String[]{"*"}) // does not fetch source
//		        .setSize(config.scrollFetchSize).get(); //max of scrollFetchSize hits will be returned for each scroll
//		return (int) scrollResp.getHits().getTotalHits();
	}




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
                    builder.startObject(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME);
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME);
                    {
                        builder.field("type", "date");
                    }
                    builder.endObject();


                    DataComponent dataComponent = rsInfo.getRecordDescription();
                    for(int i = 0; i < dataComponent.getComponentCount(); i++) {
                        DataComponent component = dataComponent.getComponent(i);
                        if (component instanceof SimpleComponent) {
                            switch (((SimpleComponent) component).getDataType()) {
                                case FLOAT:
                                    builder.startObject(component.getName());
                                    // While Quantity does not contain a precision information
                                    if(component instanceof HasUom && ((HasUom) component).getUom().getCode().toLowerCase().startsWith("db"))  {
                                        builder.field("type", "scaled_float");
                                        builder.field("index", false);
                                        builder.field("scaling_factor", 100);
                                    } else {
                                        builder.field("type", "float");
                                    }
                                    builder.endObject();
                                    break;
                                case DOUBLE:
                                    builder.startObject(component.getName());
                                    builder.field("type", "double");
                                    builder.endObject();
                                    break;
                                case SHORT:
                                    builder.startObject(component.getName());
                                    builder.field("type", "short");
                                    builder.endObject();
                                    break;
                                case USHORT:
                                case UINT:
                                case INT:
                                    builder.startObject(component.getName());
                                    builder.field("type", "integer");
                                    builder.endObject();
                                    break;
                                case ASCII_STRING:
                                    builder.startObject(component.getName());
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                    break;
                                case UTF_STRING:
                                    builder.startObject(component.getName());
                                    builder.field("type", "text");
                                    builder.endObject();
                                    break;
                                case BOOLEAN:
                                    builder.startObject(component.getName());
                                    builder.field("type", "boolean");
                                    builder.endObject();
                                    break;
                                case ULONG:
                                case LONG:
                                    builder.startObject(component.getName());
                                    builder.field("type", "long");
                                    builder.endObject();
                                    break;
                                case UBYTE:
                                case BYTE:
                                    builder.startObject(component.getName());
                                    builder.field("type", "byte");
                                    builder.endObject();
                                    break;
                                default:
                                    logger.error("Unsupported type " + ((SimpleComponent) component).getDataType());
                            }
                        } else {
                            logger.error("Unsupported type " + ((SimpleComponent) component).getDataType());
                        }
                    }
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        indexRequest.mapping(rsInfo.name, builder);

        client.indices().create(indexRequest);
    }


	@Override
	public void storeRecord(DataKey key, DataBlock data) {
        log.info("ESBasicStorageImpl:storeRecord");

        try {
            Map<String, EsRecordStoreInfo>  recordStoreInfoMap = getRecordStores();
            EsRecordStoreInfo info = recordStoreInfoMap.get(key.recordType);
            if(info != null) {
                XContentBuilder builder = XContentFactory.jsonBuilder();

                builder.startObject();
                {
                    builder.field(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME, key.producerID);
                    builder.field(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME, ESDataStoreTemplate.toEpochMillisecond(key.timeStamp));
                    DataComponent dataComponent = info.getRecordDescription();
                    for(int i = 0; i < dataComponent.getComponentCount(); i++) {
                        DataComponent component = dataComponent.getComponent(i);
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
                }
                builder.endObject();

                IndexRequest request = new IndexRequest(info.getIndexName(), info.getIndexName(), getRsKey(key));

                request.source(builder);

                client.index(request);
            } else {
                log.error("Missing record store " + key.recordType);

            }
        } catch (IOException ex) {
            log.error("Cannot create json data storeRecord", ex);
        }



//        // build the key as recordTYpe_timestamp_producerID
//        String esKey = getRsKey(key);
//
//        // get blob from dataBlock object using serializer
//        Object blob = this.getBlob(data);
//
//        Map<String, Object> json = new HashMap<>();
//        json.put(TIMESTAMP_FIELD_NAME,key.timeStamp); // store timestamp
//        json.put(PRODUCER_ID_FIELD_NAME,key.producerID); // store producerID
//        json.put(RECORD_TYPE_FIELD_NAME,key.recordType); // store recordType
//        json.put(BLOB_FIELD_NAME,blob); // store DataBlock
//
//        // set id and blob before executing the request
//		/*String id = client.prepareIndex(indexName,RS_DATA_IDX_NAME)
//				.setId(esKey)
//				.setSource(json)
//				.get()
//				.getId();*/
//        bulkProcessor.add(client.prepareIndex(indexName,RS_DATA_IDX_NAME)
//                .setId(esKey)
//                .setSource(json).request());
	}

	@Override
	public void updateRecord(DataKey key, DataBlock data) {
        log.info("ESBasicStorageImpl:data");
//		// build the key as recordTYpe_timestamp_producerID
//		String esKey = getRsKey(key);
//
//		// get blob from dataBlock object using serializer
//		Object blob = this.getBlob(data);
//
//		Map<String, Object> json = new HashMap<>();
//		json.put(TIMESTAMP_FIELD_NAME,key.timeStamp); // store timestamp
//		json.put(PRODUCER_ID_FIELD_NAME,key.producerID); // store producerID
//		json.put(RECORD_TYPE_FIELD_NAME,key.recordType); // store recordType
//		json.put(BLOB_FIELD_NAME,blob); // store DataBlock
//
//		// prepare update
//		UpdateRequest updateRequest = new UpdateRequest(indexNamePrepend, RS_DATA_IDX_NAME, esKey);
//		updateRequest.doc(json);
//
//		bulkProcessor.add(updateRequest);
	}

	@Override
	public void removeRecord(DataKey key) {
        log.info("ESBasicStorageImpl:key");
//		// build the key as recordTYpe_timestamp_producerID
//		String esKey = getRsKey(key);
//
//		// prepare delete request
//		DeleteRequest deleteRequest = new DeleteRequest(indexNamePrepend, RS_DATA_IDX_NAME, esKey);
//		bulkProcessor.add(deleteRequest);
	}

	@Override
	public int removeRecords(IDataFilter filter) {
        log.info("ESBasicStorageImpl:filter");
	    return 0;
//		double[] timeRange = getTimeRange(filter);
//
//		// MultiSearch API does not support scroll?!
//		// prepare filter
//		QueryBuilder timeStampRangeQuery = QueryBuilders.rangeQuery(TIMESTAMP_FIELD_NAME).from(timeRange[0]).to(timeRange[1]);
//		QueryBuilder recordTypeQuery = QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, filter.getRecordType());
//
//		// aggregate queries
//		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery()
//				.must(timeStampRangeQuery);
//
//		// check if any producerIDs
//		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
//			filterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
//		}
//
//		// build response
//		SearchResponse scrollResp = client.prepareSearch(indexNamePrepend).setTypes(RS_DATA_IDX_NAME)
//		        .setScroll(new TimeValue(config.pingTimeout))
//		        .setQuery(recordTypeQuery)
//		        .setFetchSource(new String[]{}, new String[]{"*"}) // does not fetch source
//		        .setPostFilter(filterQueryBuilder)
//		        .setSize(config.scrollFetchSize).get(); //max of scrollFetchSize hits will be returned for each scroll
//
//		//Scroll until no hit are returned
//		int nb = 0;
//		do {
//		    for (SearchHit hit : scrollResp.getHits().getHits()) {
//		    	// prepare delete request
//				DeleteRequest deleteRequest = new DeleteRequest(indexNamePrepend,RS_DATA_IDX_NAME, hit.getId());
//				bulkProcessor.add(deleteRequest);
//		    	nb++;
//		    }
//
//		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(config.scrollMaxDuration)).execute().actionGet();
//		} while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
//
//		log.info("[ES] Delete "+nb+" records from "+new Date((long)timeRange[0]*1000)+" to "+new Date((long)timeRange[1]*1000));
//		return 0;
	}

	/**
	 * Get a serialized object from an object.
	 * The object is serialized using Kryo.
	 * @param object The raw object
	 * @return the serialized object
	 */
	protected <T> byte[] getBlob(T object){
		return KryoSerializer.serialize(object);
	}

	/**
	 * Get an object from a base64 encoding String.
	 * The object is deserialized using Kryo.
	 * @param blob The base64 encoding String
	 * @return The deserialized object
	 */
	protected <T> T getObject(Object blob) {
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
		return key.recordType+RS_KEY_SEPARATOR+key.timeStamp+RS_KEY_SEPARATOR+key.producerID;
	}
	
	/**
	 * Transform the recordStorage data key into a DataKey by splitting <recordtype><SEPARATOR><timestamp><SEPARATOR><producerID>.
	 * @param rsKey the corresponding dataKey
	 * @return the dataKey. NULL if the length != 3 after splitting
	 */
	protected DataKey getDataKey(String rsKey) {
		DataKey dataKey = null;
		
		// split the rsKey using separator
		String [] split = rsKey.split(RS_KEY_SEPARATOR);
		
		// must find <recordtype><SEPARATOR><timestamp><SEPARATOR><producerID>
    	if(split.length == 3) {
    		dataKey = new DataKey(split[0], split[2], Double.parseDouble(split[1]));
    	}
		return dataKey;
	}
	
	/**
	 * Check if a type exist into the ES index.
	 * @param indexName 
	 * @param typeName
	 * @return true if the type exists, false otherwise
	 */
	protected boolean isTypeExist(String indexName, String typeName) {
	    return false;
//		TypesExistsRequest typeExistRequest = new TypesExistsRequest(new String[]{indexName},typeName);
//		return client.admin().indices().typesExists(typeExistRequest).actionGet().isExists();
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
        RefreshRequest refreshRequest = new RefreshRequest(indexNameMetaData);
        try {
            client.indices().refresh(refreshRequest);
        } catch (IOException ex) {
            logger.error("Error while refreshIndex", ex);
        }
	}
	
	/**
	 * Build and return the data mapping.
	 * @return The object used to map the type
	 * @throws IOException
	 */
	protected synchronized XContentBuilder getRsDataMapping() throws IOException {
	    return null;
//	    XContentBuilder builder = XContentFactory.jsonBuilder();
//	    try
//        {
//            builder.startObject()
//            	.startObject(RS_DATA_IDX_NAME)
//            		.startObject("properties")
//            			// map the timestamp as double
//            			.startObject(TIMESTAMP_FIELD_NAME).field("type", "double").endObject()
//            			// map the record type as keyword (to exact match)
//            			.startObject(RECORD_TYPE_FIELD_NAME).field("type", "keyword").endObject()
//            			// map the producer id as keyword (to exact match)
//            			.startObject(PRODUCER_ID_FIELD_NAME).field("type", "keyword").endObject()
//            			// map the blob as binary data
//            			.startObject(BLOB_FIELD_NAME).field("type", "binary").endObject()
//            		.endObject()
//            	.endObject()
//            .endObject();
//            return builder;
//        }
//        catch (IOException e)
//        {
//            builder.close();
//            throw e;
//        }
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
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

        }
    }
}
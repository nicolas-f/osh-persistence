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

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTWriter;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.GeoWKTParser;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.persistence.DataKey;
import org.sensorhub.api.persistence.IDataFilter;
import org.sensorhub.api.persistence.IDataRecord;
import org.sensorhub.api.persistence.IFoiFilter;
import org.sensorhub.api.persistence.IObsFilter;
import org.sensorhub.api.persistence.IObsStorage;
import org.sensorhub.api.persistence.IObsStorageModule;
import org.sensorhub.api.persistence.ObsFilter;
import org.sensorhub.api.persistence.ObsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.ogc.gml.GMLUtils;
import org.vast.util.Bbox;

import net.opengis.gml.v32.AbstractFeature;
import net.opengis.gml.v32.AbstractGeometry;
import net.opengis.gml.v32.impl.EnvelopeJTS;
import net.opengis.gml.v32.impl.PointJTS;
import net.opengis.gml.v32.impl.PolygonJTS;
import net.opengis.swe.v20.DataBlock;

/**
 * <p>
 * ES implementation of {@link IObsStorage} for storing observations.
 * </p>
 *
 * @author Mathieu Dhainaut <mathieu.dhainaut@gmail.com>
 * @since 2017
 */
public class ESObsStorageImpl extends ESBasicStorageImpl implements IObsStorageModule<ESBasicStorageConfig> {

	private static final String POLYGON_QUERY_ERROR_MSG = "Cannot build polygon geo query";
    private static final String NO_PARENT_VALUE = "-1";
	private static final String RESULT_TIME_FIELD_NAME = "resultTime";
	private static final String SAMPLING_GEOMETRY_FIELD_NAME = "geom";
	protected static final String FOI_IDX_NAME = "foi";
	protected static final String GEOBOUNDS_IDX_NAME = "geobounds";
	protected static final String FOI_UNIQUE_ID_FIELD = "foiID";
	protected static final String SHAPE_FIELD_NAME = "geometry";
	protected Bbox foiExtent = new Bbox();
	protected static final String METADATA_TYPE_FOI = "foi";
	
	/**
	 * Class logger
	 */
	private static final Logger log = LoggerFactory.getLogger(ESObsStorageImpl.class);

	public ESObsStorageImpl() {
        // default constructor
    }
    
    public ESObsStorageImpl(RestHighLevelClient client) {
		super(client);
	}
	
	@Override
	public void start() throws SensorHubException {
	    super.start();
	    
	    // preload bbox
	    if (client != null) {
	    	try {
				GetResponse response = client.get(new GetRequest(indexNameMetaData, INDEX_METADATA_TYPE, GEOBOUNDS_IDX_NAME));
				if(response != null && response.getSource() != null && response.getSource().containsKey(BLOB_FIELD_NAME)) {
					foiExtent = getObject(response.getSource().get(BLOB_FIELD_NAME));
				}
			} catch (IOException ex) {
	    		log.error(ex.getLocalizedMessage(), ex);
			}
	    }
	}
	
//	@Override
//	protected void createIndices (){
//		super.createIndices();
//
//		// create FOI index
//		try {
//			client.admin().indices()
//			    .preparePutMapping(indexNamePrepend)
//			    .setType("_doc")
//			    .setSource(getFoiMapping())
//			    .execute().actionGet();
//		} catch (IOException e) {
//			log.error("Cannot create the " + FOI_IDX_NAME + " mapping",e);
//		}
//
//		// create FOI bounds index
//		// this is used to store computed geographic bounds since
//		// the geobounds aggregation doesn't work with geo_shape yet
//        try {
//            client.admin().indices()
//                .preparePutMapping(indexNamePrepend)
//                .setSource(getFoiBoundsMapping())
//                .execute().actionGet();
//
//            // index an empty bbox
//            foiExtent = new Bbox();
//            client.prepareIndex(indexNamePrepend, "_doc")
//                .setId(GEOBOUNDS_IDX_NAME)
//                .setSource(BLOB_FIELD_NAME, this.getBlob(foiExtent))
//                .get();
//        } catch (IOException e) {
//            log.error("Cannot create the " + GEOBOUNDS_IDX_NAME + " mapping",e);
//        }
//
//
//		//TODO: update mapping for RS_DATA and add support for samplingTime => geo_shape
//	}
//

	BoolQueryBuilder queryByFilter(IObsFilter obsFilter) {
		BoolQueryBuilder filter = queryByFilter((IDataFilter)obsFilter);

		// filter on Foi
		if(obsFilter.getFoiIDs() != null && !obsFilter.getFoiIDs().isEmpty()) {
			filter.must(QueryBuilders.termsQuery(FOI_UNIQUE_ID_FIELD, obsFilter.getFoiIDs()));
		}

		// build parent query if ROI filter is used
		// filter on ROI
		if (obsFilter.getRoi() != null) {
			try {
				filter.must(getPolygonGeoQuery(obsFilter.getRoi()));
			} catch (IOException e) {
				log.error(POLYGON_QUERY_ERROR_MSG, e);
			}
		}

		return filter;
	}

	@Override
	public synchronized Iterator<? extends IDataRecord> getRecordIterator(IDataFilter filter) {

		// if not Obs, use the super one
		if(!(filter instanceof ObsFilter)) {
			return super.getRecordIterator(filter);
		}

		// otherwise case as obsFilter
		IObsFilter obsFilter = (IObsFilter) filter;

		BoolQueryBuilder esFilter = queryByFilter(obsFilter);

		return recordIteratorFromESQueryFilter(filter, esFilter);
//
//		BoolQueryBuilder foiFilterQueryBuilder = QueryBuilders.boolQuery(); //parent query
//		BoolQueryBuilder dataFilterQueryBuilder = QueryBuilders.boolQuery(); //children query
//
//		// FOI STORE has:
//		// Foi: uniqueId
//		// ProducerId: producer id
//		// Shape : geo_shape = roi
//
//		/*// filter on Producer Ids
//		if(obsFilter.getProducerIDs() != null && !obsFilter.getProducerIDs().isEmpty()) {
//			foiFilterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, obsFilter.getProducerIDs()));
//		}
//
//		// filter on Foi
//		if(obsFilter.getFoiIDs() != null && !obsFilter.getFoiIDs().isEmpty()) {
//			foiFilterQueryBuilder.must(QueryBuilders.termsQuery(FOI_UNIQUE_ID_FIELD, obsFilter.getFoiIDs()));
//		}*/
//
//		// build parent query if ROI filter is used
//		// filter on ROI
//		boolean useFoiFilter = false;
//		if (obsFilter.getRoi() != null) {
//			try {
//				useFoiFilter = true;
//				// get the geo query
//				foiFilterQueryBuilder.must(getPolygonGeoQuery(obsFilter.getRoi()));
//			} catch (IOException e) {
//				log.error(POLYGON_QUERY_ERROR_MSG, e);
//			}
//		}
//
//		// build children query (data records)
//		// Data Store has:
//		// Timestamp: double
//		// Producer Id: producer Id
//		// Record type: record type
//		// Foi id: foi unique id
//		// Blob: datablock
//
//		// build the RS data request using the filtering list if not null
//		double[] timeRange = getTimeRange(obsFilter);
//		if(timeRange[0] != Double.NEGATIVE_INFINITY && timeRange[1] != Double.NEGATIVE_INFINITY){
//			dataFilterQueryBuilder.must(QueryBuilders.rangeQuery(TIMESTAMP_FIELD_NAME).from(timeRange[0]).to(timeRange[1]));
//		}
//
//		// prepare filters
//		// filter on record type?
//		if(filter.getRecordType() != null) {
//			dataFilterQueryBuilder.must(QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, filter.getRecordType()));
//		}
//
//		// filter on producer id?
//		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
//			dataFilterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, obsFilter.getProducerIDs()));
//		}
//
//		// filter on Foi?
//		if(obsFilter.getFoiIDs() != null && !obsFilter.getFoiIDs().isEmpty()) {
//			dataFilterQueryBuilder.must(QueryBuilders.termsQuery(FOI_UNIQUE_ID_FIELD, obsFilter.getFoiIDs()));
//		}
//
//		// queries concatener
//		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery();
//
//		if(useFoiFilter) {
//			// combine queries
//			filterQueryBuilder.must(JoinQueryBuilders.hasParentQuery("_doc", foiFilterQueryBuilder, false))
//					.must(dataFilterQueryBuilder);
//		}	else {
//			filterQueryBuilder = dataFilterQueryBuilder;
//		}
//
//		// build response
//		SearchRequestBuilder scrollReq = client.prepareSearch(indexNamePrepend)
//				.addSort(TIMESTAMP_FIELD_NAME, SortOrder.ASC)
//		        .setScroll(new TimeValue(config.scrollMaxDuration))
//		        .setQuery(filterQueryBuilder);
//
//		// wrap the request into custom ES Scroll iterator
//		final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, scrollReq,
//				config.scrollFetchSize); //max of scrollFetchSize hits will be returned for each scroll
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
//				String recordType = nextSearchHit.getSourceAsMap().get(RECORD_TYPE_FIELD_NAME).toString();
//				String foiID = nextSearchHit.getSourceAsMap().get(FOI_UNIQUE_ID_FIELD).toString();
//				double timeStamp = (double) nextSearchHit.getSourceAsMap().get(TIMESTAMP_FIELD_NAME);
//
//				final ObsKey key = new ObsKey(recordType, foiID, timeStamp);
//
//				// get DataBlock from blob
//				final DataBlock datablock=ESObsStorageImpl.this.getObject(nextSearchHit.getSourceAsMap().get(BLOB_FIELD_NAME)); // DataBlock
//
//				return new IDataRecord(){
//
//					@Override
//					public ObsKey getKey() {
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
	public synchronized Iterator<DataBlock> getDataBlockIterator(IDataFilter filter) {

		// if not Obs, use the super one
		if(!(filter instanceof ObsFilter)) {
			return super.getDataBlockIterator(filter);
		}

		return null;
//
//		// otherwise case as obsFilter
//		IObsFilter obsFilter = (IObsFilter) filter;
//
//		BoolQueryBuilder foiFilterQueryBuilder = QueryBuilders.boolQuery(); //children query
//		BoolQueryBuilder dataFilterQueryBuilder = QueryBuilders.boolQuery(); //parent query
//
//		// FOI STORE has:
//		// Foi: uniqueId
//		// ProducerId: producer id
//		// Shape : geo_shape = roi
//
//		boolean useFoiFilter = false;
//
//		// filter on Producer Ids
//		if(obsFilter.getProducerIDs() != null && !obsFilter.getProducerIDs().isEmpty()) {
//			foiFilterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, obsFilter.getProducerIDs()));
//		}
//
//		// filter on Foi
//		if(obsFilter.getFoiIDs() != null && !obsFilter.getFoiIDs().isEmpty()) {
//			foiFilterQueryBuilder.must(QueryBuilders.termsQuery(FOI_UNIQUE_ID_FIELD, obsFilter.getFoiIDs()));
//		}
//
//		// filter on ROI
//		if (obsFilter.getRoi() != null) {
//			try {
//				useFoiFilter = true;
//				foiFilterQueryBuilder.must(getPolygonGeoQuery(obsFilter.getRoi()));
//			} catch (IOException e) {
//				log.error(POLYGON_QUERY_ERROR_MSG, e);
//			}
//		}
//
//		// Build parent query = data
//		// Data Store has:
//		// Timestamp: double
//		// Producer Id: producer Id
//		// Record type: record type
//		// Foi id: foi unique id
//		// Blob: datablock
//
//		// build the RS data request using the filtering list if not null
//		double[] timeRange = getTimeRange(obsFilter);
//		if(timeRange[0] != Double.NEGATIVE_INFINITY && timeRange[1] != Double.NEGATIVE_INFINITY){
//			dataFilterQueryBuilder.must(QueryBuilders.rangeQuery(TIMESTAMP_FIELD_NAME).from(timeRange[0]).to(timeRange[1]));
//		}
//
//		// prepare filter
//		if(filter.getRecordType() != null) {
//			dataFilterQueryBuilder.must(QueryBuilders.termQuery(RECORD_TYPE_FIELD_NAME, filter.getRecordType()));
//		}
//
//		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
//			dataFilterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, obsFilter.getProducerIDs()));
//		}
//
//		// filter on Foi
//		if(obsFilter.getFoiIDs() != null && !obsFilter.getFoiIDs().isEmpty()) {
//			dataFilterQueryBuilder.must(QueryBuilders.termsQuery(FOI_UNIQUE_ID_FIELD, obsFilter.getFoiIDs()));
//		}
//
//		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery();
//
//		if(useFoiFilter) {
//			// combine queries
//			filterQueryBuilder.must(JoinQueryBuilders.hasParentQuery("_doc", foiFilterQueryBuilder, false))
//					.must(dataFilterQueryBuilder);
//		}	else {
//			filterQueryBuilder = dataFilterQueryBuilder;
//		}
//
//		// build response
//		SearchRequestBuilder scrollReq = client.prepareSearch(indexNamePrepend)
//				.setTypes(RS_DATA_IDX_NAME)
//				.addSort(TIMESTAMP_FIELD_NAME, SortOrder.ASC)
//				.setFetchSource(new String[]{BLOB_FIELD_NAME}, new String[]{}) // get only the BLOB
//		        .setScroll(new TimeValue(config.scrollMaxDuration))
//		        .setQuery(filterQueryBuilder);
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
//				return ESObsStorageImpl.this.<DataBlock>getObject(blob); // DataBlock
//			}
//		};
	}
	
	@Override
	public synchronized int getNumFois(IFoiFilter filter) {
		return 0;
//		// build query
//		// aggregate queries
//
//		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery();
//		// filter on feature ids?
//		if(filter.getFeatureIDs() != null && !filter.getFeatureIDs().isEmpty()) {
//			filterQueryBuilder.must(QueryBuilders.termsQuery(FOI_UNIQUE_ID_FIELD, filter.getFeatureIDs()));
//		}
//
//		// filter on producer ids
//		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
//			filterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
//		}
//
//		// filter on ROI
//		if (filter.getRoi() != null) {
//			try {
//				// build geo query from filter ROI
//				filterQueryBuilder.must(getPolygonGeoQuery(filter.getRoi()));
//			} catch (IOException e) {
//				log.error(POLYGON_QUERY_ERROR_MSG, e);
//			}
//		}
//
//		// build the request
//		final SearchRequestBuilder scrollReq = client.prepareSearch(indexNamePrepend)
//				.setTypes("_doc")
//				.setQuery(filterQueryBuilder)
//				.setFetchSource(new String[] {}, new String[] {"*"});
//
//		// return the number of total hits
//		return (int) scrollReq.get().getHits().getTotalHits();
	}

	@Override
	public Bbox getFoisSpatialExtent() {
		
		// NOT WORKING because of https://github.com/elastic/elasticsearch/issues/7574
	    /*final SearchRequestBuilder sReq = client.prepareSearch(indexNamePrepend)
            .setTypes(RS_FOI_IDX_NAME)
            .addAggregation(
                AggregationBuilders.geoBounds("agg")
                                   .field(SHAPE_FIELD_NAME)
                                   .wrapLongitude(true)
            );
		
		GeoBounds agg = sReq.get().getAggregations().get("agg");
		GeoPoint tl = agg.topLeft();
		GeoPoint br = agg.bottomRight();
		return new Bbox(tl.lon(), br.lat(), br.lon(), tl.lat());*/
	    return foiExtent.copy();
	}

	@Override
	void createMetaMappingProperties(XContentBuilder builder) throws IOException {
		builder.startObject(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME);
		{
			builder.field("type", "keyword");
		}
		builder.endObject();
		builder.startObject(SHAPE_FIELD_NAME);
		{
			builder.field("type", "geo_shape");
		}
		builder.endObject();
		super.createMetaMappingProperties(builder);
	}

	@Override
	public synchronized Iterator<String> getFoiIDs(IFoiFilter filter) {

		commit();

		// build query
		// aggregate queries
		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery();

		filterQueryBuilder.must(QueryBuilders.termQuery(STORAGE_ID_FIELD_NAME, config.id));

		// filter on producer ids?
		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
			filterQueryBuilder.must(QueryBuilders.termsQuery(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
		}

		if(filter.getFeatureIDs() != null && !filter.getFeatureIDs().isEmpty()) {
			filterQueryBuilder.must(QueryBuilders.termsQuery("_id" ,filter.getFeatureIDs().toArray(new String[filter.getFeatureIDs().size()])));
		}

		// filter on roi?
		if (filter.getRoi() != null) {
			try {
				// build geo query
				filterQueryBuilder.must(getPolygonGeoQuery(filter.getRoi()));
			} catch (IOException e) {
				log.error(POLYGON_QUERY_ERROR_MSG, e);
			}
		}

		SearchRequest searchRequest = new SearchRequest(indexNameMetaData);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(filterQueryBuilder);
		if(filter.getRoi() != null) {
		    // ElasticSearch return false positive (only approximate bound box test)
            // We have to check before returning values
            searchSourceBuilder.fetchSource(SHAPE_FIELD_NAME, null);
        } else {
            searchSourceBuilder.fetchSource(false);
        }
		searchSourceBuilder.size(config.scrollFetchSize);
		searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.ASC));
		searchRequest.source(searchSourceBuilder);
		searchRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));

		final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, searchRequest, TimeValue.timeValueMillis(config.scrollMaxDuration)); //max of scrollFetchSize hits will be returned for each scroll

		// build a IDataRecord iterator based on the searchHits iterator

		final Shape geomTest = filter.getRoi() == null ? null : getPolygonBuilder(filter.getRoi()).build();

		return new Iterator<String>() {


			@Override
			public boolean hasNext() {
				return searchHitsIterator.hasNext();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public String next() {
				SearchHit nextSearchHit = searchHitsIterator.next();

				if (geomTest != null) {
					try {
						do {
							// Extract shape from the geometry field using ES shape parser
								XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
										LoggingDeprecationHandler.INSTANCE, new BytesArray(nextSearchHit.getSourceAsString()), XContentType.JSON);
								parser.nextToken();
								// Continue while we not found the geometry field
								while (parser.currentToken() != XContentParser.Token.FIELD_NAME ||
										!parser.currentName().equals(SHAPE_FIELD_NAME)) {
									parser.nextToken();
								}
								parser.nextToken(); // Go into field content
								ShapeBuilder shapeBuilder = ShapeParser.parse(parser);
								Shape geom = shapeBuilder.build();
								if (geom.relate(geomTest).intersects()) {
									break;
								}
								nextSearchHit = searchHitsIterator.next();
						} while (searchHitsIterator.hasNext());
					} catch (Exception ex) {
						log.error(ex.getLocalizedMessage(), ex);
					}
				}

				return nextSearchHit.getId();
			}
		};
//
//		// build query
//		// aggregate queries
//		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery();
//		// filter on feature ids?
//		if(filter.getFeatureIDs() != null && !filter.getFeatureIDs().isEmpty()) {
//			filterQueryBuilder.must(QueryBuilders.termsQuery(FOI_UNIQUE_ID_FIELD, filter.getFeatureIDs()));
//		}
//
//		// filter on producer ids?
//		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
//			filterQueryBuilder.must(QueryBuilders.termsQuery(PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
//		}
//
//		// filter on ROI?
//		if (filter.getRoi() != null) {
//			try {
//				// build geo query
//				filterQueryBuilder.must(getPolygonGeoQuery(filter.getRoi()));
//			} catch (IOException e) {
//				log.error(POLYGON_QUERY_ERROR_MSG, e);
//			}
//		}
//
//		final SearchRequestBuilder scrollReq = client.prepareSearch(indexNamePrepend)
//				.setTypes("_doc")
//				.setQuery(filterQueryBuilder)
//				 // get only the id
//				.setFetchSource(new String[] { FOI_UNIQUE_ID_FIELD }, new String[] {})
//				.setScroll(new TimeValue(config.scrollMaxDuration));
//		// wrap the request into custom ES Scroll iterator
//		final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, scrollReq, config.scrollFetchSize);
//
//		return new Iterator<String>() {
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
//			public String next() {
//				SearchHit nextSearchHit = searchHitsIterator.next();
//				// get Feature id
//				return nextSearchHit.getSourceAsMap().get(FOI_UNIQUE_ID_FIELD).toString();
//			}
//		};
	}

	@Override
	public synchronized Iterator<AbstractFeature> getFois(IFoiFilter filter) {

		commit();

		// build query
		// aggregate queries
		BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery();

		filterQueryBuilder.must(QueryBuilders.termQuery(STORAGE_ID_FIELD_NAME, config.id));

		// filter on producer ids?
		if(filter.getProducerIDs() != null && !filter.getProducerIDs().isEmpty()) {
			filterQueryBuilder.must(QueryBuilders.termsQuery(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME, filter.getProducerIDs()));
		}

		if(filter.getFeatureIDs() != null && !filter.getFeatureIDs().isEmpty()) {
			filterQueryBuilder.must(QueryBuilders.termsQuery("_id" ,filter.getFeatureIDs().toArray(new String[filter.getFeatureIDs().size()])));
		}

		// filter on roi?
		if (filter.getRoi() != null) {
			try {
				// build geo query
				filterQueryBuilder.must(getPolygonGeoQuery(filter.getRoi()));
			} catch (IOException e) {
				log.error(POLYGON_QUERY_ERROR_MSG, e);
			}
		}

		SearchRequest searchRequest = new SearchRequest(indexNameMetaData);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(filterQueryBuilder);
		searchSourceBuilder.size(config.scrollFetchSize);
		searchSourceBuilder.sort(new FieldSortBuilder(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME).order(SortOrder.ASC));
		searchRequest.source(searchSourceBuilder);
		searchRequest.scroll(TimeValue.timeValueMillis(config.scrollMaxDuration));

		final Iterator<SearchHit> searchHitsIterator = new ESIterator(client, searchRequest, TimeValue.timeValueMillis(config.scrollMaxDuration)); //max of scrollFetchSize hits will be returned for each scroll

		// build a IDataRecord iterator based on the searchHits iterator

		return new Iterator<AbstractFeature>() {

			@Override
			public boolean hasNext() {
				return searchHitsIterator.hasNext();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public AbstractFeature next() {
				SearchHit nextSearchHit = searchHitsIterator.next();

				return getObject(nextSearchHit.getSourceAsMap().get(BLOB_FIELD_NAME));
			}
		};
	}

	@Override
	public synchronized DataBlock getDataBlock(DataKey key) {
		return null;
//
//		// because we need to specify parent in any case
//		String parent = "";
//
//		// test if obsKey
//		// if not, set parent to -1. If no parent is specified, ES will throw an exception.
//		// we must specify a parent
//		if(!(key instanceof ObsKey)) {
//			parent = NO_PARENT_VALUE;
//		} else {
//			parent = ((ObsKey)key).foiID;
//		}
//
//		DataBlock result = null;
//		// build the key as recordTYpe_timestamp_producerID
//		String esKey = getRsKey(key);
//
//		// build the request
//		GetResponse  response = client.prepareGet(indexNamePrepend,"_doc",esKey)
//				.setParent(parent)
//				.get();
//
//		// deserialize the blob field from the response if any
//		if(response.isExists()) {
//			result = this.<DataBlock>getObject(response.getSource().get(BLOB_FIELD_NAME)); // DataBlock
//		}
//		return result;
	}
	
	@Override
	public synchronized void removeRecord(DataKey key) {


//
//		// because we need to specify parent in any case
//		String parent = "";
//
//		// test if obsKey
//		// if not, set parent to -1. If no parent is specified, ES will throw an exception.
//		// we must specify a parent
//		if(!(key instanceof ObsKey)) {
//			parent = NO_PARENT_VALUE;
//		} else {
//			parent = ((ObsKey)key).foiID;
//		}
//
//		// build the key as recordTYpe_timestamp_producerID
//		String esKey = getRsKey(key);
//
//		// prepare delete request
//		client.prepareDelete(indexNamePrepend, RS_DATA_IDX_NAME, esKey)
//			.setParent(parent).get();
	}
	
	@Override
	public synchronized void updateRecord(DataKey key, DataBlock data) {
//		String esKey = getRsKey(key);
//
//		if(!(key instanceof ObsKey)) {
//			// build the key as recordTYpe_timestamp_producerID
//
//			// get blob from dataBlock object using serializer
//			Object blob = this.getBlob(data);
//
//			//TOCHECK: do we need to store the whole key?
//			Map<String, Object> json = new HashMap<>();
//			json.put(TIMESTAMP_FIELD_NAME,key.timeStamp); // store timestamp
//			json.put(PRODUCER_ID_FIELD_NAME,key.producerID); // store producerID
//			json.put(RECORD_TYPE_FIELD_NAME,key.recordType); // store recordType
//			json.put(BLOB_FIELD_NAME,blob); // store DataBlock
//
//			// create update request
//			client.prepareUpdate(indexNamePrepend, RS_DATA_IDX_NAME, esKey)
//				.setDoc(json)
//				.setParent(NO_PARENT_VALUE).get();
//		} else {
//			ObsKey obsKey = (ObsKey) key;
//
//			// get blob from dataBlock object using serializer
//			Object blob = this.getBlob(data);
//
//			Map<String, Object> json = new HashMap<>();
//			json.put(TIMESTAMP_FIELD_NAME,key.timeStamp); // store timestamp
//
//			if(key.producerID != null) {
//				json.put(PRODUCER_ID_FIELD_NAME,key.producerID); // store producerID
//			}
//
//			if(key.recordType != null) {
//				json.put(RECORD_TYPE_FIELD_NAME,key.recordType); // store recordType
//			}
//
//			json.put(BLOB_FIELD_NAME,blob); // store DataBlock
//
//			// obs part
//			json.put(RESULT_TIME_FIELD_NAME,obsKey.resultTime);
//
//			if(obsKey.foiID != null) {
//				json.put(FOI_UNIQUE_ID_FIELD,obsKey.foiID);
//			}
//
//			if(obsKey.samplingGeometry != null) {
//				json.put(SAMPLING_GEOMETRY_FIELD_NAME,getPolygonBuilder(obsKey.samplingGeometry));
//			}
//
//			// create update request
//			client.prepareUpdate(indexNamePrepend, RS_DATA_IDX_NAME, esKey)
//				.setDoc(json)
//				.setParent(obsKey.foiID).get();
//		}
//
		
	}
	
	@Override
	public synchronized void storeRecord(DataKey key, DataBlock data) {
//	    // build the key as recordTYpe_timestamp_producerID
//        String esKey = getRsKey(key);
//
//	    if(!(key instanceof ObsKey)) {
//			// get blob from dataBlock object using serializer
//			Object blob = this.getBlob(data);
//
//			Map<String, Object> json = new HashMap<>();
//			json.put(TIMESTAMP_FIELD_NAME,key.timeStamp); // store timestamp
//			json.put(PRODUCER_ID_FIELD_NAME,key.producerID); // store producerID
//			json.put(RECORD_TYPE_FIELD_NAME,key.recordType); // store recordType
//			json.put(BLOB_FIELD_NAME,blob); // store DataBlock
//
//			// set id and blob before executing the request
//			String id = client.prepareIndex(indexNamePrepend,"_doc")
//					.setId(esKey)
//                    .setParent(NO_PARENT_VALUE)
//					.setSource(json)
//					.get().getId();
//		} else {
//			ObsKey obsKey = (ObsKey) key;
//
//			// get blob from dataBlock object using serializer
//			Object blob = this.getBlob(data);
//
//			Map<String, Object> json = new HashMap<>();
//			json.put(TIMESTAMP_FIELD_NAME,key.timeStamp); // store timestamp
//
//			if(key.producerID != null) {
//				json.put(PRODUCER_ID_FIELD_NAME,key.producerID); // store producerID
//			}
//
//			if(key.recordType != null) {
//				json.put(RECORD_TYPE_FIELD_NAME,key.recordType); // store recordType
//			}
//
//			json.put(BLOB_FIELD_NAME,blob); // store DataBlock
//
//			// obs part
//			if(!Double.isNaN(obsKey.resultTime)) {
//				json.put(RESULT_TIME_FIELD_NAME,obsKey.resultTime);
//			}
//
//			String uniqueParentID = NO_PARENT_VALUE;
//
//			if(obsKey.foiID != null) {
//				json.put(FOI_UNIQUE_ID_FIELD,obsKey.foiID);
//				uniqueParentID = obsKey.foiID;
//			}
//
//			if(obsKey.samplingGeometry != null) {
//				json.put(SAMPLING_GEOMETRY_FIELD_NAME,getPolygonBuilder(obsKey.samplingGeometry));
//			}
//
//
//			// set id and blob before executing the request
//			String id = client.prepareIndex(indexNamePrepend,"_doc")
//			        .setId(esKey)
//					.setParent(uniqueParentID)
//					.setSource(json).get().getId();
//		}
	}
	
	@Override
	public synchronized void storeFoi(String producerID, AbstractFeature foi) {
		log.info("ESObsStorageImpl:storeFoi");

		// add new record storage
		byte[] bytes = this.getBlob(foi);

		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();
			{
				// Convert to elastic search epoch millisecond
				builder.field(STORAGE_ID_FIELD_NAME, config.id);
				builder.field(METADATA_TYPE_FIELD_NAME, METADATA_TYPE_FOI);
				builder.field(SHAPE_FIELD_NAME, getShapeBuilder(foi.getLocation()));
				builder.field(ESDataStoreTemplate.PRODUCER_ID_FIELD_NAME, producerID);
				builder.timeField(ESDataStoreTemplate.TIMESTAMP_FIELD_NAME, System.currentTimeMillis());
				builder.field(BLOB_FIELD_NAME, bytes);
			}
			builder.endObject();
			IndexRequest request = new IndexRequest(indexNameMetaData, INDEX_METADATA_TYPE, foi.getUniqueIdentifier());

			request.source(builder);

			client.index(request);

			storeChanged = System.currentTimeMillis();

		} catch (IOException | SensorHubException ex) {
			logger.error(String.format("storeFoi exception %s:%s in elastic search driver",producerID, foi.getName()), ex);
		}
	}

	/**
	 * Build and return the FOI mapping.
	 * @return The object used to map the type
	 * @throws IOException
	 */
	protected synchronized XContentBuilder getFoiMapping() throws IOException {
		return null;

//		XContentBuilder builder = XContentFactory.jsonBuilder();
//	    try
//        {
//            builder.startObject()
//            		.startObject(FOI_IDX_NAME)
//            			.startObject("properties")
//            			    .startObject(FOI_UNIQUE_ID_FIELD).field("type", "keyword").endObject()
//                            .startObject(SHAPE_FIELD_NAME).field("type", "geo_shape").endObject()
//            				.startObject(PRODUCER_ID_FIELD_NAME).field("type", "keyword").endObject()
//            				.startObject(BLOB_FIELD_NAME).field("type", "binary").endObject()
//            			.endObject()
//            		.endObject()
//            	.endObject();
//            return builder;
//        }
//        catch (IOException e)
//        {
//            builder.close();
//            throw e;
//        }
	}
	
	/**
     * Build and return the FOI bounds mapping.
     * @return The object used to map the type
     * @throws IOException
     */
    protected synchronized XContentBuilder getFoiBoundsMapping() throws IOException {
    	return null;
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        try
//        {
//            builder.startObject()
//                .startObject(GEOBOUNDS_IDX_NAME)
//                    .startObject("properties")
//                        .startObject(BLOB_FIELD_NAME).field("type", "binary").endObject()
//                    .endObject()
//                .endObject()
//            .endObject();
//            return builder;
//        }
//        catch (IOException e)
//        {
//            builder.close();
//            throw e;
//        }
    }


	/**
     * Gets the envelope builder from a bbox object.
     * @param bbox the bbox
     * @returnthe Envelope builder
     */
    protected synchronized EnvelopeBuilder getEnvelopeBuilder(Bbox bbox) {
		org.locationtech.jts.geom.Coordinate topLeft = new org.locationtech.jts.geom.Coordinate(bbox.getMinX(), bbox.getMaxY());
		org.locationtech.jts.geom.Coordinate btmRight = new org.locationtech.jts.geom.Coordinate(bbox.getMaxX(), bbox.getMinY());
        return new EnvelopeBuilder(topLeft, btmRight);
    }

	/**
	 * Gets the envelope builder from envelope geometry.
	 * @param env the envelope geometry
	 * @returnthe Envelope builder
	 */
	protected synchronized EnvelopeBuilder getEnvelopeBuilder(Envelope env) {
		org.locationtech.jts.geom.Coordinate topLeft = new org.locationtech.jts.geom.Coordinate(env.getMinX(), env.getMaxY());
		org.locationtech.jts.geom.Coordinate btmRight = new org.locationtech.jts.geom.Coordinate(env.getMaxX(), env.getMinY());
        return new EnvelopeBuilder(topLeft, btmRight);
	}

	/**
	 * Build a polygon builder from a polygon geometry.
	 * @param polygon the Polygon geometry
	 * @return the builder
	 */
	protected synchronized PolygonBuilder getPolygonBuilder(Polygon polygon) {
		// get coordinates list from polygon
		CoordinatesBuilder coordinates = new CoordinatesBuilder();
		for(Coordinate coordinate : polygon.getExteriorRing().getCoordinates()) {
            double x = coordinate.x;
            double y = coordinate.y;
            // Handle out of bounds points
            if(x < -180 || x > 180) {
                x = -180 + x % 180;
                log.warn("Point %f,%f out of bounds",x,y);
            }
            if(y < -90 || y > 90) {
                y = -90 + y % 90;
                log.warn("Point %f,%f out of bounds",x,y);
            }
			coordinates.coordinate(x, y);
		}
		// build shape builder from coordinates
		return new PolygonBuilder(coordinates);
		//.relation(ShapeRelation.WITHIN); strategy, Default is INTERSECT
	}

	/**
	 * Build a point builder from a point geometry.
	 * @param point the Point geometry
	 * @return the builder
	 */
	protected synchronized PointBuilder getPointBuilder(Point point) {
		// build shape builder from coordinates
        double x = point.getX();
        double y = point.getY();
        // Handle out of bounds points
        if(x < -180 || x > 180) {
            x = -180 + x % 180;
            log.warn("Point %f,%f out of bounds",x,y);
        }
        if(y < -90 || y > 90) {
            y = -90 + y % 90;
            log.warn("Point %f,%f out of bounds",x,y);
        }
		return new PointBuilder(x, y);
	}

	/**
	 * Build the corresponding builder given a generic geometry.
	 * @param geometry The abstract geometry
	 * @return the corresponding builder. The current supported builder are: PolygonJTS, Point, EnvelopeJTS
	 * @throws SensorHubException if the geometry is not supported
	 */
	protected synchronized ShapeBuilder getShapeBuilder(AbstractGeometry geometry) throws SensorHubException {
		if(geometry instanceof PolygonJTS) {
			return getPolygonBuilder((PolygonJTS)geometry);
		} else if(geometry instanceof PointJTS) {
			return getPointBuilder((PointJTS)geometry);
		} else if(geometry instanceof EnvelopeJTS) {
			return getEnvelopeBuilder((Envelope)geometry);
		} else {
			throw new SensorHubException("Unsupported Geometry exception: "+geometry.getClass());
		}
	}

	/**
	 * Build the geo shape query from a Polygon. The query will use a geo intersection query
	 * @param polygon The geometry to build the query
	 * @return The corresponding builder
	 * @throws IOException
	 */
	protected synchronized QueryBuilder getPolygonGeoQuery(Polygon polygon) throws IOException {
		return QueryBuilders.geoIntersectionQuery(SHAPE_FIELD_NAME,
				getPolygonBuilder(polygon));
	}
}

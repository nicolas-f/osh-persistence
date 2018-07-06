/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.persistence.es;

import java.util.*;

import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.config.DisplayInfo.Required;

/**
 * <p>
 * Configuration class for ES basic storage
 * </p>
 *
 * @author Mathieu Dhainaut <mathieu.dhainaut@gmail.com>
 * @author Nicolas Fortin <nicolas.fortin at - ifsttar.fr>
 * @since 2017
 */
public class ESBasicStorageConfig extends org.sensorhub.api.persistence.ObsStorageConfig {

	public static final String DEFAULT_INDEX_NAME_METADATA = "osh_meta_record_store";

    @Required
    @DisplayInfo(desc="ES cluster name")
    public String clusterName = "elasticsearch";

    @DisplayInfo(desc = "ElasticSearch user for authentication (leave blank if not required)")
    public String user = "";

	@DisplayInfo(desc = "ElasticSearch password for authentication")
    @DisplayInfo.FieldType(DisplayInfo.FieldType.Type.PASSWORD)
    public String password = "";
        
    @Required
    @DisplayInfo(desc="List of nodes")
    public List<String> nodeUrls = Arrays.asList("localhost:9200","localhost:9201");

    @DisplayInfo(desc="String to add in index name before the data name")
    public String indexNamePrepend = "";

	@DisplayInfo(desc="Index name of the OpenSensorHub metadata")
	public String indexNameMetaData = DEFAULT_INDEX_NAME_METADATA;
            
    @DisplayInfo(desc="When scrolling, the maximum duration ScrollableResults will be usable if no other results are fetched from, in ms")
    public int scrollMaxDuration = 6000;
	
	@DisplayInfo(desc="MWhen scrolling, the number of results fetched by each Elasticsearch call")
    public int scrollFetchSize = 10;
	
	@DisplayInfo(desc="When scrolling, the minimum number of previous results kept in memory at any time")
	public int scrollBacktrackingWindowSize = 10000;
		
	@DisplayInfo(desc="Set to true to ignore cluster name validation of connected nodes")
	public boolean ignoreClusterName = false;
	
	@DisplayInfo(desc="The time to wait for a ping response from a node")
	public int pingTimeout = 5;
	
	@DisplayInfo(desc="How often to sample / ping the nodes listed and connected")
	public int nodeSamplerInterval = 5;
	
	@DisplayInfo(desc="Enable sniffing")
	public boolean transportSniff = false;
	
	@DisplayInfo(desc="Set the number of concurrent requests")
	public int bulkConcurrentRequests = 10;
	
	@DisplayInfo(desc="We want to execute the bulk every n requests")
	public int bulkActions = 10000;
	
	@DisplayInfo(desc="We want to flush the bulk every n mb")
	public int bulkSize = 10;
	
	@DisplayInfo(desc="We want to flush the bulk every n seconds whatever the number of requests")
	public int bulkFlushInterval = 10;

    @Override
    public void setStorageIdentifier(String name)
    {
        indexNamePrepend = name;
    }

}

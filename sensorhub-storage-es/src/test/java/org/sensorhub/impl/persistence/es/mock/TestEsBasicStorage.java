/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.persistence.es.mock;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.sensorhub.impl.persistence.es.ESBasicStorageImpl;
import org.sensorhub.impl.persistence.es.ESBasicStorageConfig;
import org.sensorhub.test.persistence.AbstractTestBasicStorage;
import org.sensorhub.utils.FileUtils;


public class TestEsBasicStorage extends AbstractTestBasicStorage<ESBasicStorageImpl> {

    protected static final String CLUSTER_NAME = "elasticsearch";
	private static RestHighLevelClient client;
	private static File tmpDir;

	@BeforeClass
	public static void initClass() {
		tmpDir = new File(System.getProperty("java.io.tmpdir")+"/es/"+UUID.randomUUID().toString());
		tmpDir.mkdirs();
		client = new RestHighLevelClient(
				RestClient.builder(
						new HttpHost("localhost", 9200, "http"),
						new HttpHost("localhost", 9201, "http")));
	}
	
	@Before
	public void init() throws Exception {
		
		
		ESBasicStorageConfig config = new ESBasicStorageConfig();
		config.autoStart = true;
		config.clusterName = CLUSTER_NAME;
		List<String> nodes = new ArrayList<String>();
		nodes.add("localhost:9300");

		config.nodeUrls = nodes;
		config.scrollFetchSize = 2000;
		config.bulkConcurrentRequests = 0;
		config.id = "junit_testesbasicstorage_" + UUID.randomUUID().toString();
		
		storage = new ESBasicStorageImpl(client);
		storage.init(config);
		storage.start();
	}

	@Override
	protected void forceReadBackFromStorage() throws Exception {
		// Let the time to ES to write the data
    	// if some tests are not passed,  try to increase this value first!!
		storage.commit();
		
	}

	@AfterClass
    public static void cleanup() throws Exception {
		if(client != null) {
			client.close();
			client = null;
		}

		if(tmpDir.exists()) {
			FileUtils.deleteRecursively(tmpDir);
		}
	}
}

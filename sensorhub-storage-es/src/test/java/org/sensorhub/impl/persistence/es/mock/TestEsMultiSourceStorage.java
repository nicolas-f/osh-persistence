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

public class TestEsMultiSourceStorage {

// extends AbstractTestBasicStorage<ESMultiSourceStorageImpl> {
//
//    protected static final String CLUSTER_NAME = "elasticsearch";
//    private static TransportClient client;
//	private static File tmpDir;
//
//	static {
//		tmpDir = new File(System.getProperty("java.io.tmpdir")+"/es/"+UUID.randomUUID().toString());
//		tmpDir.mkdirs();
//		try {
//			client = getTransportClient();
//		} catch (NodeValidationException e) {
//			e.printStackTrace();
//		}
//	}
//
//	@Before
//	public void init() throws Exception {
//
//
//		ESBasicStorageConfig config = new ESBasicStorageConfig();
//		config.autoStart = true;
//		config.clusterName = CLUSTER_NAME;
//		config.scrollFetchSize = 2000;
//		config.bulkConcurrentRequests = 0;
//		config.id = "junit_" + UUID.randomUUID().toString();
//
//		storage = new ESMultiSourceStorageImpl(client);
//		storage.init(config);
//		storage.start();
//	}
//
//	@Override
//	protected void forceReadBackFromStorage() throws Exception {
//		// Let the time to ES to write the data
//    	// if some tests are not passed,  try to increase this value first!!
//		storage.commit();
//
//	}
//
//	public static TransportClient getTransportClient() throws NodeValidationException {
//		try {
//			return new PreBuiltTransportClient(Settings.EMPTY)
//					.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
//		} catch (UnknownHostException ex) {
//			throw new NodeValidationException(ex.getLocalizedMessage());
//		}
//	}
//
//	@AfterClass
//    public static void cleanup() throws Exception {
//		if(client != null) {
//			client.close();
//			client = null;
//		}
//
//		if(tmpDir.exists()) {
//			FileUtils.deleteRecursively(tmpDir);
//		}
//	}
}
	

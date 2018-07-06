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

import net.opengis.gml.v32.Point;
import net.opengis.gml.v32.impl.PointImpl;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;
import net.opengis.swe.v20.Vector;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.persistence.DataFilter;
import org.sensorhub.api.persistence.DataKey;
import org.sensorhub.api.persistence.IDataRecord;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.persistence.es.ESBasicStorageConfig;
import org.sensorhub.impl.persistence.es.ESBasicStorageImpl;
import org.sensorhub.impl.sensor.AbstractSensorModule;
import org.sensorhub.impl.sensor.DefaultLocationOutputLLA;
import org.sensorhub.impl.sensor.SensorSystem;
import org.sensorhub.test.TestUtils;
import org.sensorhub.test.persistence.AbstractTestBasicStorage;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEConstants;
import org.vast.swe.helper.GeoPosHelper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


public class TestEsBasicStorage extends AbstractTestBasicStorage<ESBasicStorageImpl> {

    protected static final String CLUSTER_NAME = "elasticsearch";

    private static final boolean clean_index = true;

    @Before
    public void init() throws Exception {


        ESBasicStorageConfig config = new ESBasicStorageConfig();
        config.autoStart = true;
        config.clusterName = CLUSTER_NAME;
        List<String> nodes = new ArrayList<String>();
        nodes.add("localhost:9200");
        nodes.add("localhost:9201");

        config.nodeUrls = nodes;
        config.bulkConcurrentRequests = 0;
        config.id = "junit_testesbasicstorage_" + System.currentTimeMillis();
        config.indexNamePrepend = "data_" + config.id + "_";
        config.indexNameMetaData = "meta_" + config.id + "_";

        storage = new ESBasicStorageImpl();
        storage.init(config);
        storage.start();
    }

    @After
    public void after() throws SensorHubException {
        // Delete added index
        storage.commit();
        if (clean_index) {
            DeleteIndexRequest request = new DeleteIndexRequest(storage.getAddedIndex().toArray(new String[storage.getAddedIndex().size()]));
            try {
                storage.getClient().indices().delete(request);
            } catch (IOException ex) {
                throw new SensorHubException(ex.getLocalizedMessage(), ex);
            }
        }
        storage.stop();
    }

    @Override
    protected void forceReadBackFromStorage() throws Exception {
        // Let the time to ES to write the data
        // if some tests are not passed,  try to increase this value first!!
        storage.commit();
    }

    @Test
    public void testLocationOutput() throws Exception {
        GeoPosHelper fac = new GeoPosHelper();
        Vector locVector = fac.newLocationVectorLLA(SWEConstants.DEF_SENSOR_LOC);
        locVector.setName("location");
        locVector.setLocalFrame('#' + "locationjunit");
        DataComponent outputStruct = fac.wrapWithTimeStampUTC(locVector);
        outputStruct.setName("sensorLocation");
        outputStruct.setId("SENSOR_LOCATION");
        DataEncoding outputEncoding = new TextEncodingImpl();

        storage.addRecordStore(outputStruct.getName(), outputStruct, outputEncoding);

        double timeStamp = new Date().getTime() / 1000.;
        // build new datablock
        DataBlock dataBlock = outputStruct.createDataBlock();
        Coordinate location = new Coordinate(-1.55336, 47.21725, 15);
        dataBlock.setDoubleValue(0, timeStamp);
        dataBlock.setDoubleValue(1, location.y); //y
        dataBlock.setDoubleValue(2, location.x); //x
        dataBlock.setDoubleValue(3, location.z); //z

        storage.storeRecord(new DataKey(outputStruct.getName(), "e44cb499-3b6c-4305-b479-ebacc965579f", timeStamp), dataBlock);

        forceReadBackFromStorage();

        // Read back
        Iterator<? extends IDataRecord> it = storage.getRecordIterator(new DataFilter(outputStruct.getName()) {
            @Override
            public double[] getTimeStampRange() {
                return new double[]{timeStamp - 5, Double.MAX_VALUE};
            }
        });
        int i = 0;
        while (it.hasNext()) {
            TestUtils.assertEquals(dataBlock, it.next().getData());
            i++;
        }
        assertEquals(1, i);
    }

}

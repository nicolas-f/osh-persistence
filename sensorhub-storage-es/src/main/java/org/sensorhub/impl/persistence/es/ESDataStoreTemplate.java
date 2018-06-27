package org.sensorhub.impl.persistence.es;

import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.sensorhub.api.persistence.DataKey;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ESDataStoreTemplate {

    public static final String PRODUCER_ID_FIELD_NAME = "producerID";
    public static final String TIMESTAMP_FIELD_NAME = "timestamp";

    String name;
    List<String> jsonParts = new ArrayList<>();
    final int recordStructureStart;
    final int capacityHint;


    public ESDataStoreTemplate(DataComponent recordStructure) {
        this.name = recordStructure.getName();
        jsonParts.add("{\""+PRODUCER_ID_FIELD_NAME+"\":\"");
        jsonParts.add("\"" + TIMESTAMP_FIELD_NAME + "\":");
        recordStructureStart = jsonParts.size();
        jsonParts.add("\"");
        for(int i = 0; i < recordStructure.getComponentCount(); i++) {
            DataComponent comp = recordStructure.getComponent(i);
        }

        int totalLength = 0;
        for(String part : jsonParts) {
            totalLength += part.length();
        }

        capacityHint = totalLength * 2;
    }

    public static long toEpochMillisecond(double timesteampsecond) {
        return Double.valueOf(timesteampsecond * 1000).longValue();
    }

    public String build(DataKey key, DataBlock data) throws IOException {
        StringBuilder s = new StringBuilder(capacityHint);
        s.append(jsonParts.get(0));
        s.append(key.producerID);
        for(int i = recordStructureStart; i < data.getAtomCount(); i++) {
            s.append(jsonParts.get(i));
            s.append(data.getUnderlyingObject());
        }
        return s.toString();
    }
}

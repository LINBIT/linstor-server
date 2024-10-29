package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.pojo.backups.RscMetaPojo;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RscPojoMigrationTest
{
    private static final String DUMMY_VALUE = "dummyValue";

    @Test
    public void migrationTest() throws Exception
    {
        Map<String, Object> oldJsonMap = new HashMap<>();
        {
            Map<String, Object> oldPropsMap = new HashMap<>();
            oldPropsMap.put("Backup/SourceSnapDfnUUID", DUMMY_VALUE);
            oldPropsMap.put("BackupShipping/dummyKey", DUMMY_VALUE);
            oldPropsMap.put("Shipping/dummyKey", DUMMY_VALUE);
            oldPropsMap.put("Satellite/EBS/EbsSnapId_WithDummySuffix", DUMMY_VALUE);
            oldPropsMap.put("SnapshotShippingNamePrev", DUMMY_VALUE);
            oldPropsMap.put("dummyProp", DUMMY_VALUE);

            oldJsonMap.put("props", oldPropsMap);
        }
        oldJsonMap.put("flags", 0);
        oldJsonMap.put("vlms", new HashMap<>()); // we dont care about volumes in this test

        ObjectMapper om = new ObjectMapper();
        RscMetaPojo rscMetaPojo = om.readValue(
            om.writeValueAsString(oldJsonMap),
            RscMetaPojo.class
        );

        assertEquals(
            Map.of(
                "Backup/SourceSnapDfnUUID", "dummyValue",
                "BackupShipping/dummyKey", "dummyValue",
                "Shipping/dummyKey", "dummyValue",
                "SnapshotShippingNamePrev", "dummyValue",
                "Satellite/EBS/EbsSnapId_WithDummySuffix", "dummyValue"
            ),
            rscMetaPojo.getSnapProps()
        );
        assertEquals(
            Map.of("dummyProp", "dummyValue"),
            rscMetaPojo.getRscProps()
        );
    }
}

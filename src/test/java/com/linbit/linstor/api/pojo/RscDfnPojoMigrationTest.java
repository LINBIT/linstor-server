package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.pojo.backups.RscDfnMetaPojo;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RscDfnPojoMigrationTest
{
    private static final String DUMMY_VALUE = "dummyValue";

    @Test
    public void migrationTest() throws Exception
    {
        Map<String, Object> oldJsonMap = new HashMap<>();
        {
            Map<String, Object> oldPropsMap = new HashMap<>();
            oldPropsMap.put("BackupShipping/dummyKey", DUMMY_VALUE);
            oldPropsMap.put("Schedule/BackupShippedBySchedule", DUMMY_VALUE);
            oldPropsMap.put("dummyProp", DUMMY_VALUE);

            oldJsonMap.put("props", oldPropsMap);
        }
        oldJsonMap.put("flags", 0);
        oldJsonMap.put("vlmDfns", new HashMap<>()); // we dont care about volumedefinitions in this test

        ObjectMapper om = new ObjectMapper();
        RscDfnMetaPojo rscDfnMetaPojo = om.readValue(
            om.writeValueAsString(oldJsonMap),
            RscDfnMetaPojo.class
        );

        assertEquals(
            Map.of(
                "BackupShipping/dummyKey",
                DUMMY_VALUE,
                "Schedule/BackupShippedBySchedule",
                DUMMY_VALUE
            ),
            rscDfnMetaPojo.getSnapDfnProps()
        );
        assertEquals(
            Map.of(
                "dummyProp",
                DUMMY_VALUE
            ),
            rscDfnMetaPojo.getRscDfnProps()
        );
    }
}

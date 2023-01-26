package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.InvalidNameException;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.storage.LsBlkEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CtrlPhysicalStorageApiCallHandlerTest
{
    private static final int DFLT_DISC_GRAN = 64 * 1024;

    @Test
    public void testGroupLsBlkEntriesByNode() throws InvalidNameException
    {
        Map<NodeName, List<LsBlkEntry>> testData = new HashMap<>();
        final NodeName alpha = new NodeName("alpha");
        final NodeName bravo = new NodeName("bravo");
        final NodeName charlie = new NodeName("charlie");

        List<LsBlkEntry> data = new ArrayList<>();
        data.add(
            new LsBlkEntry(
                "sde",
                512L*1024*1024*1024,
                false,
                "",
                "sde",
                "",
                8,
                9,
                "",
                "",
                "",
                DFLT_DISC_GRAN
            )
        );

        testData.put(alpha, data);

        testData.put(bravo, data);

        List<LsBlkEntry> dataCharlie = new ArrayList<>();
        dataCharlie.add(
            new LsBlkEntry(
                "sde",
                512L*1024*1024*1024,
                false,
                "",
                "sde",
                "",
                8,
                9,
                "",
                "",
                "",
                DFLT_DISC_GRAN
            )
        );
        dataCharlie.add(
            new LsBlkEntry(
                "sdd",
                640L*1024*1024*1024,
                true,
                "",
                "sdd",
                "",
                9,
                0,
                "",
                "",
                "",
                DFLT_DISC_GRAN
            )
        );
        dataCharlie.add(
            new LsBlkEntry(
                "sdf",
                512L*1024*1024*1024,
                true,
                "",
                "sdf",
                "",
                9,
                10,
                "",
                "",
                "",
                DFLT_DISC_GRAN
            )
        );

        testData.put(charlie, dataCharlie);

        final List<JsonGenTypes.PhysicalStorage> physicalStorages =
            CtrlPhysicalStorageApiCallHandler.groupPhysicalStorageByDevice(testData);

        Assert.assertEquals(3, physicalStorages.size());
        Assert.assertEquals(3, physicalStorages.get(0).nodes.size());
        Assert.assertEquals(1, physicalStorages.get(1).nodes.size());
        Assert.assertEquals(1, physicalStorages.get(2).nodes.size());
        Assert.assertTrue(physicalStorages.get(1).nodes.containsKey("charlie"));
        Assert.assertEquals("sdf", physicalStorages.get(1).nodes.get("charlie").get(0).device);
    }
}

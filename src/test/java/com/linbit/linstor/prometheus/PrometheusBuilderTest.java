package com.linbit.linstor.prometheus;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.StderrErrorReporter;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PrometheusBuilderTest
{

    @Test
    public void testNullMetrics() throws IOException {
        long start = System.currentTimeMillis();
        StderrErrorReporter errReporter = new StderrErrorReporter("Test");
        PrometheusBuilder pmb = new PrometheusBuilder(errReporter);

        final String promText = pmb.build(
                null,
                null,
                new ResourceList(),
                null,
                null,
                1,
                start);
        Assert.assertNotNull(promText);
        Assert.assertTrue(promText.contains("linstor_scrape_requests_count"));
    }

    @Test
    public void testSampleMetrics() throws IOException {
        long start = System.currentTimeMillis();
        StderrErrorReporter errReporter = new StderrErrorReporter("Test");
        PrometheusBuilder pmb = new PrometheusBuilder(errReporter);

        ArrayList<NodeApi> nodeList = new ArrayList<>();
        nodeList.add(
            new NodePojo(
                UUID.randomUUID(),
                "testnode",
                "SATELLITE",
                0,
                Collections.emptyList(),
                null,
                Collections.emptyList(),
                Collections.emptyMap(),
                ApiConsts.ConnectionStatus.ONLINE,
                null, // platform
                null, // osVariant
                null,
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                3
            )
        );

        final RscGrpPojo dfltRscGrp = new RscGrpPojo(
            UUID.randomUUID(),
            "DfltRscGrp",
            "",
            Collections.emptyMap(),
            Collections.emptyList(),
            null,
            null
        );

        ArrayList<ResourceDefinitionApi> rscDfns = new ArrayList<>();
        rscDfns.add(
            new RscDfnPojo(
                UUID.randomUUID(),
                dfltRscGrp,
                "testrsc",
                null,
                0,
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList())
        );

        final String promText = pmb.build(
            nodeList,
            rscDfns,
            new ResourceList(),
            null,
            null,
            1,
            start);
        Assert.assertNotNull(promText);
        Assert.assertTrue(promText.contains("linstor_scrape_requests_count"));
        Assert.assertTrue(promText.contains("linstor_node_state"));
        Assert.assertTrue(promText.contains("linstor_resource_definition_count 1.0"));
        Assert.assertTrue(promText.contains("linstor_node_reconnect_attempt_count"));
    }

    private static NodeApi nodeWithFlags(String name, long flags)
    {
        return new NodePojo(
            UUID.randomUUID(),
            name,
            "SATELLITE",
            flags,
            Collections.emptyList(),
            null,
            Collections.emptyList(),
            Collections.emptyMap(),
            ApiConsts.ConnectionStatus.ONLINE,
            null, // platform
            null, // osVariant
            null,
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null,
            0
        );
    }

    private static @Nullable String nodeFlagValue(String promText, String node, Node.Flags flag)
    {
        String value = null;
        for (String line : promText.split("\n"))
        {
            if (line.startsWith("linstor_node_flag{") && line.contains("node=\"" + node + "\"") &&
                line.contains("flag=\"" + flag.name() + "\""))
            {
                value = line.substring(line.lastIndexOf(' ') + 1);
            }
        }
        return value;
    }

    @Test
    public void testNodeFlags() throws IOException
    {
        PrometheusBuilder pmb = new PrometheusBuilder(new StderrErrorReporter("Test"));

        ArrayList<NodeApi> nodeList = new ArrayList<>();
        nodeList.add(nodeWithFlags("plain", 0));
        nodeList.add(nodeWithFlags("evacuating", Node.Flags.EVACUATE.flagValue));
        nodeList.add(nodeWithFlags("evicted", Node.Flags.EVICTED.flagValue));

        nodeList.add(nodeWithFlags("deleting", Node.Flags.DELETE.flagValue | Node.Flags.QIGNORE.flagValue));

        final String promText = pmb.build(nodeList, null, null, null, null, 1, System.currentTimeMillis());

        // every user-visible flag is exported for every node, set or not
        for (String node : new String[] {"plain", "evacuating", "evicted", "deleting"})
        {
            for (Node.Flags flag : new Node.Flags[] {Node.Flags.DELETE, Node.Flags.EVICTED, Node.Flags.EVACUATE})
            {
                Assert.assertNotNull(node + "/" + flag, nodeFlagValue(promText, node, flag));
            }
            // internal flag, never exported
            Assert.assertNull(node + "/QIGNORE", nodeFlagValue(promText, node, Node.Flags.QIGNORE));
        }
        Assert.assertEquals("1.0", nodeFlagValue(promText, "evacuating", Node.Flags.EVACUATE));
        Assert.assertEquals("0.0", nodeFlagValue(promText, "plain", Node.Flags.EVACUATE));
        Assert.assertEquals("0.0", nodeFlagValue(promText, "evacuating", Node.Flags.EVICTED));
        // EVICTED includes the DELETE bit, but an evicted node is not reported as being deleted
        Assert.assertEquals("1.0", nodeFlagValue(promText, "evicted", Node.Flags.EVICTED));
        Assert.assertEquals("0.0", nodeFlagValue(promText, "evicted", Node.Flags.DELETE));
        Assert.assertEquals("1.0", nodeFlagValue(promText, "deleting", Node.Flags.DELETE));
        Assert.assertEquals("0.0", nodeFlagValue(promText, "deleting", Node.Flags.EVICTED));
    }

    private static ResourceApi rscApi(String rscName, String nodeName)
    {
        ResourceApi rsc = Mockito.mock(ResourceApi.class);
        Mockito.when(rsc.getName()).thenReturn(rscName);
        Mockito.when(rsc.getNodeName()).thenReturn(nodeName);
        Mockito.when(rsc.getVlmList()).thenReturn(Collections.emptyList());
        return rsc;
    }

    private static @Nullable String resourceOpenValue(String promText, String rscName, String nodeName)
    {
        String value = null;
        for (String line : promText.split("\n"))
        {
            if (line.startsWith("linstor_resource_open{") && line.contains("name=\"" + rscName + "\"") &&
                line.contains("node=\"" + nodeName + "\""))
            {
                value = line.substring(line.lastIndexOf(' ') + 1);
            }
        }
        return value;
    }

    @Test
    public void testResourceOpen() throws Exception
    {
        PrometheusBuilder pmb = new PrometheusBuilder(new StderrErrorReporter("Test"));

        ResourceList rl = new ResourceList();
        rl.addResource(rscApi("r1", "n1"));
        rl.addResource(rscApi("r1", "n2"));
        rl.addResource(rscApi("r1", "n3"));

        SatelliteState n1 = new SatelliteState();
        n1.setOnResource(new ResourceName("r1"), SatelliteResourceState::setInUse, Boolean.FALSE);
        n1.setOnResource(new ResourceName("r1"), SatelliteResourceState::setOpen, Boolean.TRUE);
        SatelliteState n2 = new SatelliteState();
        n2.setOnResource(new ResourceName("r1"), SatelliteResourceState::setInUse, Boolean.FALSE);
        n2.setOnResource(new ResourceName("r1"), SatelliteResourceState::setOpen, Boolean.FALSE);
        SatelliteState n3 = new SatelliteState();
        // satellite or DRBD too old to report the open state
        n3.setOnResource(new ResourceName("r1"), SatelliteResourceState::setInUse, Boolean.FALSE);
        rl.putSatelliteState(new NodeName("n1"), n1);
        rl.putSatelliteState(new NodeName("n2"), n2);
        rl.putSatelliteState(new NodeName("n3"), n3);

        final String promText = pmb.build(null, null, rl, null, null, 1, System.currentTimeMillis());

        Assert.assertEquals("1.0", resourceOpenValue(promText, "r1", "n1"));
        Assert.assertEquals("0.0", resourceOpenValue(promText, "r1", "n2"));
        Assert.assertEquals("-1.0", resourceOpenValue(promText, "r1", "n3"));
        // open on a Secondary does not change the meaning of linstor_resource_state
        Assert.assertTrue(promText.contains("linstor_resource_state{"));
    }
}

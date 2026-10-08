package com.linbit.linstor.prometheus;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.StderrErrorReporter;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.utils.PairNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    private static ResourceApi rscApi(String rscName, String nodeName, long flags, List<VolumeApi> vlms)
    {
        ResourceApi rsc = Mockito.mock(ResourceApi.class);
        Mockito.when(rsc.getName()).thenReturn(rscName);
        Mockito.when(rsc.getNodeName()).thenReturn(nodeName);
        Mockito.when(rsc.getFlags()).thenReturn(flags);
        Mockito.doReturn(vlms).when(rsc).getVlmList();
        return rsc;
    }

    /**
     * @param layers the layer stack of the volume, top layer first
     */
    private static VolumeApi vlmApi(String devicePath, DeviceLayerKind... layers)
    {
        List<PairNonNull<String, VlmLayerDataApi>> layerData = new ArrayList<>();
        for (DeviceLayerKind kind : layers)
        {
            VlmLayerDataApi vlmLayerData = Mockito.mock(VlmLayerDataApi.class);
            Mockito.when(vlmLayerData.getLayerKind()).thenReturn(kind);
            layerData.add(new PairNonNull<>(kind.name(), vlmLayerData));
        }
        VolumeApi vlm = Mockito.mock(VolumeApi.class);
        Mockito.when(vlm.getVlmNr()).thenReturn(0);
        Mockito.when(vlm.getDevicePath()).thenReturn(devicePath);
        Mockito.when(vlm.getVlmLayerData()).thenReturn(layerData);
        return vlm;
    }

    private static @Nullable String sampleValue(String promText, String metric, String... labels)
    {
        String value = null;
        for (String line : promText.split("\n"))
        {
            boolean match = line.startsWith(metric + "{");
            for (String label : labels)
            {
                match &= line.contains(label);
            }
            if (match)
            {
                value = line.substring(line.lastIndexOf(' ') + 1);
            }
        }
        return value;
    }

    private static @Nullable String volumeStateValue(String promText, String rscName, String nodeName)
    {
        return sampleValue(
            promText, "linstor_volume_state", "resource=\"" + rscName + "\"", "node=\"" + nodeName + "\""
        );
    }

    private static @Nullable String resourceFlagValue(
        String promText,
        String rscName,
        String nodeName,
        Resource.Flags flag
    )
    {
        return sampleValue(
            promText,
            "linstor_resource_flag",
            "name=\"" + rscName + "\"",
            "resource=\"" + rscName + "\"",
            "node=\"" + nodeName + "\"",
            "flag=\"" + flag.name() + "\""
        );
    }

    @Test
    public void testVolumeState() throws Exception
    {
        PrometheusBuilder pmb = new PrometheusBuilder(new StderrErrorReporter("Test"));

        ResourceList rl = new ResourceList();
        rl.addResource(rscApi("drbd", "n1", 0, List.of(vlmApi("/dev/drbd1000", DeviceLayerKind.DRBD,
            DeviceLayerKind.STORAGE))));
        rl.addResource(rscApi("drbdnostate", "n1", 0, List.of(vlmApi("/dev/drbd1001", DeviceLayerKind.DRBD,
            DeviceLayerKind.STORAGE))));
        rl.addResource(rscApi("storage", "n1", 0, List.of(vlmApi("/dev/vg/storage_00000",
            DeviceLayerKind.STORAGE))));
        rl.addResource(rscApi("luks", "n1", 0, List.of(vlmApi("/dev/mapper/luks_00000", DeviceLayerKind.LUKS,
            DeviceLayerKind.STORAGE))));
        // DRBD below another layer does not report a disk state for the volume
        rl.addResource(rscApi("drbdbelow", "n1", 0, List.of(vlmApi("/dev/mapper/drbdbelow", DeviceLayerKind.LUKS,
            DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE))));

        SatelliteState n1 = new SatelliteState();
        n1.setOnVolume(new ResourceName("drbd"), new VolumeNumber(0), SatelliteVolumeState::setDiskState, "UpToDate");
        rl.putSatelliteState(new NodeName("n1"), n1);

        final String promText = pmb.build(null, null, rl, null, null, 1, System.currentTimeMillis());

        Assert.assertEquals("1.0", volumeStateValue(promText, "drbd", "n1"));
        // a disk state is expected but missing
        Assert.assertEquals("-1.0", volumeStateValue(promText, "drbdnostate", "n1"));
        // volumes without DRBD on top are reported as "Created", as the client does
        Assert.assertEquals("2.0", volumeStateValue(promText, "storage", "n1"));
        Assert.assertEquals("2.0", volumeStateValue(promText, "luks", "n1"));
        Assert.assertEquals("2.0", volumeStateValue(promText, "drbdbelow", "n1"));
    }

    @Test
    public void testResourceFlags() throws Exception
    {
        PrometheusBuilder pmb = new PrometheusBuilder(new StderrErrorReporter("Test"));

        ResourceList rl = new ResourceList();
        rl.addResource(rscApi("r1", "plain", Resource.Flags.DRBD_DISKLESS.flagValue, Collections.emptyList()));
        rl.addResource(rscApi("r1", "inactive", Resource.Flags.INACTIVE.flagValue, Collections.emptyList()));
        rl.addResource(
            rscApi("r1", "permanently", Resource.Flags.INACTIVE_PERMANENTLY.flagValue, Collections.emptyList())
        );
        rl.addResource(rscApi("r1", "evicted", Resource.Flags.EVICTED.flagValue, Collections.emptyList()));

        final String promText = pmb.build(null, null, rl, null, null, 1, System.currentTimeMillis());

        Assert.assertEquals("0.0", resourceFlagValue(promText, "r1", "plain", Resource.Flags.INACTIVE));
        Assert.assertEquals("0.0", resourceFlagValue(promText, "r1", "plain", Resource.Flags.EVICTED));
        Assert.assertEquals("1.0", resourceFlagValue(promText, "r1", "inactive", Resource.Flags.INACTIVE));
        Assert.assertEquals("0.0", resourceFlagValue(promText, "r1", "inactive", Resource.Flags.EVICTED));
        Assert.assertEquals("1.0", resourceFlagValue(promText, "r1", "permanently", Resource.Flags.INACTIVE));
        Assert.assertEquals("0.0", resourceFlagValue(promText, "r1", "permanently", Resource.Flags.EVICTED));
        // EVICTED includes the INACTIVE bit
        Assert.assertEquals("1.0", resourceFlagValue(promText, "r1", "evicted", Resource.Flags.INACTIVE));
        Assert.assertEquals("1.0", resourceFlagValue(promText, "r1", "evicted", Resource.Flags.EVICTED));
        // only the exported flags
        Assert.assertNull(resourceFlagValue(promText, "r1", "plain", Resource.Flags.DRBD_DISKLESS));
    }
}

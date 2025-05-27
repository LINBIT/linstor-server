package com.linbit.linstor.api;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts.ConnectionStatus;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoPlaceApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import static com.linbit.linstor.storage.kinds.DeviceLayerKind.DRBD;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.LUKS;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.STORAGE;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.LVM;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.LVM_THIN;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.ZFS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.ZFS_THIN;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

@SuppressWarnings({"checkstyle:magicnumber", "checkstyle:descendenttokencheck"})
public class RscAutoPlaceApiTest extends ApiTestBase
{
    private static final long KB = 1;
    private static final long MB = 1_000 * KB;
    private static final long GB = 1_000 * MB;
    private static final long TB = 1_000 * GB;

    private static final String TEST_RSC_NAME = "TestRsc";
    private static final int TEST_TCP_PORT_NR = 8000;

    private static final int MINOR_NR_MIN = 1000;
    private static final AtomicInteger MINOR_GEN = new AtomicInteger(MINOR_NR_MIN);

    @Inject private CtrlRscAutoPlaceApiCallHandler rscAutoPlaceApiCallHandler;
    @Inject private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;

    private static final StorPoolName DFLT_DISKLESS_STOR_POOL_NAME;

    static
    {
        try
        {
            DFLT_DISKLESS_STOR_POOL_NAME = new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Bind @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;
    private ResourceGroup dfltRscGrp;

    @Before
    @Override
    public void setUp() throws Exception
    {
        seedDefaultPeerRule.setDefaultPeerAccessContext(BOB_ACC_CTX);
        super.setUp();
        dfltRscGrp = createDefaultResourceGroup(BOB_ACC_CTX);
        createRscDfn(TEST_RSC_NAME);
        MINOR_GEN.set(MINOR_NR_MIN);

        Mockito.when(mockPeer.getAccessContext()).thenReturn(BOB_ACC_CTX);

        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any())).thenReturn(Mono.just(Collections.emptyMap()));

        @Nullable Props optAutoplacerNamespace = ctrlConf.getNamespace(ApiConsts.NAMESPC_AUTOPLACER_WEIGHTS);
        if (optAutoplacerNamespace != null)
        {
            optAutoplacerNamespace.clear();
        }

        commitAndCleanUp(true);
    }

    @After
    @Override
    public void tearDown() throws Exception
    {
        commitAndCleanUp(false);
    }

    @Test
    public void basicTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED,
                ApiConsts.CREATED,
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * TB)
        );
        expectDeployed(
            "slow1",
            TEST_RSC_NAME,
            "stlt1", "stlt2"
        );
    }

    @Test
    public void chooseLargerSatelliteTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                1,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            // Name and order the options so that the expected choice is in the middle in terms of creation sequence
            // and lexicographic order in order to minimize the chances of choosing correctly by accident
            .stltBuilder("stlt1")
                .addStorPool("pool", 10 * MB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("pool", 30 * MB)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("pool", 20 * MB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * MB)
        );
        expectDeployed(
            "pool",
            TEST_RSC_NAME,
            "stlt2"
        );
    }

    @Test
    public void chooseLargerPoolTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                1,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt")
                // Name and order the options so that the expected choice is in the middle in terms of creation sequence
                // and lexicographic order in order to minimize the chances of choosing correctly by accident
                .addStorPool("pool1", 10 * MB)
                .addStorPool("pool2", 30 * MB)
                .addStorPool("pool3", 20 * MB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * MB)
        );
        expectDeployed(
            "pool2",
            TEST_RSC_NAME,
            "stlt"
        );
    }

    @Test
    public void preferredStorPoolNotEnoughSpaceTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                false,
                ApiConsts.FAIL_NOT_ENOUGH_NODES
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * TB)
            .addStorPool("fast1")
        );
        expectNotDeployed(TEST_RSC_NAME);
    }

    @Test
    public void preferredStorPoolSuccessTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 50 * GB)
            .addStorPool("fast1")
        );

        expectDeployed(
            "fast1",
            TEST_RSC_NAME,
            "stlt1", "stlt2"
        );
    }

    @Test
    public void doNotPlaceWithRscTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("slow1", 80 * GB)
                .build()
            .stltBuilder("stlt4")
                .addStorPool("slow1", 80 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 50 * GB)

            .doNotPlaceWith("avoid1")

            .addRscDfn("avoid1")
            .addVlmDfn("avoid1", 0, 2 * TB)
            .addRsc("avoid1", "slow1", "stlt1", "stlt2")
        );

        // do not place with is a node-level check, not a storpool-level
        expectDeployed(
            "slow1",
            TEST_RSC_NAME,
            "stlt3", "stlt4"
        );
    }

    @Test
    public void notEnoughNodesTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                3,
                false,
                ApiConsts.FAIL_NOT_ENOUGH_NODES
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * TB)
        );
        expectNotDeployed(TEST_RSC_NAME);
    }

    @Test
    public void doNotPlaceWithRscAndStorPoolTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                false,
                ApiConsts.FAIL_NOT_ENOUGH_NODES
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * TB)

            .doNotPlaceWith("avoid1")
                .addStorPool("slow1")

            .addRscDfn("avoid1")
            .addVlmDfn("avoid1", 0, 2 * TB)
            .addRsc("avoid1", "slow1", "stlt1", "stlt2")
        );
        expectNotDeployed(TEST_RSC_NAME);
    }

    @Test
    public void doNotPlaceWithRscRegexTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("slow2", 20 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("slow2", 20 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("fast1", 80 * GB)
                .build()
            .stltBuilder("stlt4")
                .addStorPool("fast1", 80 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 50 * GB)

            .setDoNotPlaceWithRegex("avoid.*")

            .addRscDfn("avoid1")
            .addVlmDfn("avoid1", 0, 2 * TB)
                .addRsc("avoid1", "slow1", "stlt1", "stlt2")
            .addRscDfn("avoid2")
            .addVlmDfn("avoid2", 0, 2 * TB)
                .addRsc("avoid2", "slow2", "stlt1", "stlt2")
        );

        expectDeployed(
            "fast1",
            TEST_RSC_NAME,
            "stlt3", "stlt4"
        );
    }

    @Test
    public void doNotPlaceWithRscSimpleRegexPrefixTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("slow2", 20 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("slow2", 20 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("fast1", 80 * GB)
                .build()
            .stltBuilder("stlt4")
                .addStorPool("fast1", 80 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 50 * GB)

            .setDoNotPlaceWithRegex("avoid") // no trailing ".*"

            .addRscDfn("avoid1")
            .addVlmDfn("avoid1", 0, 2 * TB)
                .addRsc("avoid1", "slow1", "stlt1", "stlt2")
            .addRscDfn("avoid2")
            .addVlmDfn("avoid2", 0, 2 * TB)
                .addRsc("avoid2", "slow2", "stlt1", "stlt2")
        );

        // do not place with is a node-level check, not a storpool-level
        expectDeployed(
            "fast1",
            TEST_RSC_NAME,
            "stlt3", "stlt4"
        );
    }

    @Test
    public void replicasCombinedTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("stlt.A1.B1.1")
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "1")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A1.B1.2")
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "1")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A1.B2.1")
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "2")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A1.B2.2")
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "2")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A1.B3.1")
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "3")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A1.B3.2")
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "3")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A2.B1.1")
                .setNodeProp("Aux/A", "2")
                .setNodeProp("Aux/B", "1")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A2.B1.2")
                .setNodeProp("Aux/A", "2")
                .setNodeProp("Aux/B", "1")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A2.B2.1")
                .setNodeProp("Aux/A", "2")
                .setNodeProp("Aux/B", "2")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A2.B2.2")
                .setNodeProp("Aux/A", "2")
                .setNodeProp("Aux/B", "2")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A2.B3.1")
                .setNodeProp("Aux/A", "2")
                .setNodeProp("Aux/B", "3")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("stlt.A2.B3.2")
                .setNodeProp("Aux/A", "2")
                .setNodeProp("Aux/B", "3")
                .addStorPool("stor", 10 * GB)
                .build()

            .addReplicasOnSameNodeProp("Aux/A")
            .addReplicasOnDifferentNodeProp("Aux/B")
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode()) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());

        Props firstNodeProps = deployedNodes.get(0).getProps(GenericDbBase.SYS_CTX);
        Props secondNodeProps = deployedNodes.get(1).getProps(GenericDbBase.SYS_CTX);

        assertEquals(firstNodeProps.getProp("Aux/A"), secondNodeProps.getProp("Aux/A"));
        assertNotEquals(firstNodeProps.getProp("Aux/B"), secondNodeProps.getProp("Aux/B"));
    }

    @Test
    public void replicasOnCombinedWithValueTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                1,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("node0.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node1.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node2.val2")
                .setNodeProp("Aux/A", "2")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node3.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node4.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 10 * GB)
                .build()

            .addReplicasOnSameNodeProp("Aux/A=1")
            .addReplicasOnDifferentNodeProp("Aux/A=0")
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(1, deployedNodes.size());

        String nodePropVal = deployedNodes.get(0).getProps(GenericDbBase.SYS_CTX).getProp("Aux/A");

        assertEquals("1", nodePropVal);
        assertNotEquals("0", nodePropVal);
    }

    @Test
    public void replicasOnDifferentWithValueGithub79Test() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
                .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
                .stltBuilder("node1")
                    .setNodeProp("Aux/key", "val1")
                    .addStorPool("stor", 10 * GB)
                    .build()
                .stltBuilder("node2")
                    .setNodeProp("Aux/key", "val2")
                    .addStorPool("stor", 10 * GB)
                    .build()
                .stltBuilder("node3")
                    .setNodeProp("Aux/key", "val3")
                    .addStorPool("stor", 10 * GB)
                    .build()
                .stltBuilder("node4")
                    .setNodeProp("Aux/key", "val1") // same as "node1"
                    .addStorPool("stor", 10 * GB)
                    .build()
                .stltBuilder("node5")
                    .setNodeProp("Aux/key", "val2") // same as "node2"
                    .addStorPool("stor", 9 * GB) // make node2 a bit better
                    .build()

                .addReplicasOnDifferentNodeProp("Aux/key=val1")
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .sorted((n1, n2) -> n1.getName().compareTo(n2.getName()))
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());

        String nodePropVal = deployedNodes.get(0).getProps(GenericDbBase.SYS_CTX).getProp("Aux/key");

        assertNotEquals("val1", nodePropVal);

        assertEquals("node2", deployedNodes.get(0).getName().displayValue);
        assertEquals("node3", deployedNodes.get(1).getName().displayValue);
    }

    @Test
    public void replicasOnDifferentWithOutValueTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("node0.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node1.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node2.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node3.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 100 * GB)
                .build()
            .stltBuilder("node4.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 100 * GB)
                .build()

            .addReplicasOnDifferentNodeProp("Aux/A")
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());

        String firstNodePropVal = deployedNodes.get(0).getProps(GenericDbBase.SYS_CTX).getProp("Aux/A");
        String secondNodePropVal = deployedNodes.get(1).getProps(GenericDbBase.SYS_CTX).getProp("Aux/A");

        assertNotEquals(secondNodePropVal, firstNodePropVal);
    }

    @Test
    public void replicasOnDifferentWithValueTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
                .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
                .stltBuilder("node0.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
                .stltBuilder("node1.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 10 * GB)
                .build()
                .stltBuilder("node2.val2")
                .setNodeProp("Aux/A", "2")
                .addStorPool("stor", 10 * GB)
                .build()
                .stltBuilder("node3.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
                .stltBuilder("node4.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 10 * GB)
                .build()

                .addReplicasOnDifferentNodeProp("Aux/A=0")
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());

        String firstNodePropVal = deployedNodes.get(0).getProps(GenericDbBase.SYS_CTX).getProp("Aux/A");
        String secondNodePropVal = deployedNodes.get(1).getProps(GenericDbBase.SYS_CTX).getProp("Aux/A");

        assertNotEquals("0", firstNodePropVal);
        assertNotEquals("0", secondNodePropVal);
    }

    @Test
    public void replicasOnDifferentWithRollbackTest() throws Exception
    {
        /*
         * This test aims to force the autoplacer to make the first selection (which should make it into the final
         * decision), then make the autoplacer make the second selection. This second choice should not yield to any
         * result (dead-end), so the autoplacer will need to rollback this second selection.
         *
         * The point of this test is to test the rollback mechanism in combination with replicas-on-differnt. The new
         * second selection must still consider the first decisions node-prop for replicas-on-differnt.
         */
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                3,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // rsc autoplace
                ApiConsts.CREATED // rsc autoplace
            )
                .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("node0.00")
                // best candidate, should also be selected in the end
                .setNodeProp("Aux/A", "0")
                .setNodeProp("Aux/B", "0")
                .addStorPool("stor", 100 * GB)
                .build()
            .stltBuilder("node1.11")
                // second best, but should be rolled back
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "1")
                .addStorPool("stor", 90 * GB)
                .build()
            .stltBuilder("node2.10")
                // cannot be selected while node1.11 is selected
                // but also cannot be selected while node0.00 is selected
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "0")
                .addStorPool("stor", 80 * GB)
                .build()
            .stltBuilder("node3.21")
                // cannot be selected while node1.11 is selected
                .setNodeProp("Aux/A", "2")
                .setNodeProp("Aux/B", "1")
                .addStorPool("stor", 70 * GB)
                .build()
            .stltBuilder("node4.12")
                // also cannot be selected while node 1.11 is selected
                .setNodeProp("Aux/A", "1")
                .setNodeProp("Aux/B", "2")
                .addStorPool("stor", 10 * GB)
                .build()

            .addReplicasOnDifferentNodeProp("Aux/A")
            .addReplicasOnDifferentNodeProp("Aux/B")
        );

        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 3 nodes
            .collect(Collectors.toList());
        assertEquals(3, deployedNodes.size());

        for (int idx1 = 0; idx1 < deployedNodes.size(); idx1++)
        {
            Node firstNode = deployedNodes.get(idx1);
            Props firstProps = firstNode.getProps(SYS_CTX);
            for (int idx2 = idx1 + 1; idx2 < deployedNodes.size(); idx2++)
            {
                Node secondNode = deployedNodes.get(idx2);
                Props secondProps = secondNode.getProps(SYS_CTX);

                assertNotEquals(firstProps.getProp("Aux/A"), secondProps.getProp("Aux/A"));
                assertNotEquals(firstProps.getProp("Aux/B"), secondProps.getProp("Aux/B"));
            }
        }

        assertEquals("node0.00", deployedNodes.get(0).getName().displayValue);
        assertEquals("node3.21", deployedNodes.get(1).getName().displayValue);
        assertEquals("node4.12", deployedNodes.get(2).getName().displayValue);
    }

    @Test
    public void xReplicasOnDifferentSimpleTest() throws Exception
    {
        /*
         * Simple test, we have 5 datacenter locations with 3 nodes each and want to have an
         * "--x-replicas-on-different 2 DC --place-count 5".
         * Expected result should be a 2+2+1 distribution, not a 1+1+1+1+1
         */
        RscAutoPlaceApiCall rscAutoPlaceApiCall = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            5,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB);
        int nodeId = 0;
        final int nodesPerLocation = 3;
        TreeSet<String> dcLocations = new TreeSet<>(
            Arrays.asList(
                "Vienna",
                "Berlin",
                "Frankfurt",
                "London",
                "Paris"
            )
        );
        for (String datacenter : dcLocations)
        {
            for (int i = 1; i <= nodesPerLocation; i++)
            {
                rscAutoPlaceApiCall = rscAutoPlaceApiCall
                    .stltBuilder(String.format("node%02d.%s%d", nodeId++, datacenter, i))
                        .setNodeProp("Aux/DC", datacenter)
                        .addStorPool("stor", 10 * GB)
                    .build();
            }
        }
        evaluateTest(
            rscAutoPlaceApiCall
                .putXReplicasOnDifferent("Aux/DC", 2)
                .addReplicasOnDifferentNodeProp("Aux/DC")
        );

        // all SPs are equal, so we expect alphanummerically the first two DCs to contain 2 replicas each and the third
        // DC to have a single replica
        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(5, deployedNodes.size());


        Map<String, Integer> selectedDcs = new TreeMap<>();
        for (String dc : dcLocations)
        {
            selectedDcs.put(dc, 0);
        }
        for (Node node : deployedNodes)
        {
            String dc = node.getProps(SYS_CTX).getProp("Aux/DC");
            selectedDcs.put(dc, selectedDcs.get(dc) + 1);
        }

        Iterator<String> dcLocIterator = dcLocations.iterator();
        assertEquals(2, (int) selectedDcs.get(dcLocIterator.next()));
        assertEquals(2, (int) selectedDcs.get(dcLocIterator.next()));
        assertEquals(1, (int) selectedDcs.get(dcLocIterator.next()));
        while (dcLocIterator.hasNext())
        {
            assertEquals(0, (int) selectedDcs.get(dcLocIterator.next()));
        }
    }

    @Test
    public void xReplicasOnDifferentOnlyOnePartiallyFilledGroupTest() throws Exception
    {
        /*
         * 5 datacenter locations, 3 nodes each, but try to provoke the autoplacer in choosing a 1+1+1+1+1, which is not
         * allowed. This provocation is done by giving one node of every datacenter a much higher score (more free
         * space) than the other two nodes of the given datacenter.
         * The autoplacer is expected to try the 1+1+1+1+1 first (since this combination has the highest score), but
         * should rollback and retry until it finishes in a valid 2+2+1 setup.
         */
        RscAutoPlaceApiCall rscAutoPlaceApiCall = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            5,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB);
        int nodeId = 0;
        TreeSet<String> dcLocations = new TreeSet<>(
            Arrays.asList(
                "Vienna",
                "Berlin",
                "Frankfurt",
                "London",
                "Paris"
            )
        );
        for (String datacenter : dcLocations)
        {
            rscAutoPlaceApiCall = rscAutoPlaceApiCall
                .stltBuilder(String.format("node%02d.%s1", nodeId++, datacenter))
                    .setNodeProp("Aux/DC", datacenter)
                    .addStorPool("stor", 100 * GB)
                .build()
                .stltBuilder(String.format("node%02d.%s2", nodeId++, datacenter))
                    .setNodeProp("Aux/DC", datacenter)
                    .addStorPool("stor", 10 * GB)
                .build()
                .stltBuilder(String.format("node%02d.%s3", nodeId++, datacenter))
                    .setNodeProp("Aux/DC", datacenter)
                    .addStorPool("stor", 10 * GB)
                .build();
        }
        evaluateTest(
            rscAutoPlaceApiCall
                .putXReplicasOnDifferent("Aux/DC", 2)
                .addReplicasOnDifferentNodeProp("Aux/DC")
        );

        // although not all SPs are equal, we still expect alphanummerically the first two DCs to contain 2 replicas
        // each and the third DC to have a single replica, since all nodes have the same equally good storage pool
        // (100G) and two not-that-good SPs (10G). so there is no "preferred" node, which means, alphanumeric sorting is
        // still expected
        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(5, deployedNodes.size());

        Map<String, Integer> selectedDcs = new TreeMap<>();
        for (String dc : dcLocations)
        {
            selectedDcs.put(dc, 0);
        }
        for (Node node : deployedNodes)
        {
            String dc = node.getProps(SYS_CTX).getProp("Aux/DC");
            selectedDcs.put(dc, selectedDcs.get(dc) + 1);
        }

        Iterator<String> dcLocIterator = dcLocations.iterator();
        assertEquals(2, (int) selectedDcs.get(dcLocIterator.next()));
        assertEquals(2, (int) selectedDcs.get(dcLocIterator.next()));
        assertEquals(1, (int) selectedDcs.get(dcLocIterator.next()));
        while (dcLocIterator.hasNext())
        {
            assertEquals(0, (int) selectedDcs.get(dcLocIterator.next()));
        }
    }

    @Test
    public void xReplicasOnDifferentOnlyOnePartiallyFilledGroup2Test() throws Exception
    {
        /*
         * 3 datacenter locations, 3 nodes each, but try to provoke the autoplacer in choosing a 2+2, which is not
         * allowed. This provocation is done by giving 2 nodes of every datacenter a much higher score (more free
         * space) than the remaining node of the given datacenter.
         * The autoplacer is expected to try the 2+2 first (since this combination has the highest score), but
         * should rollback and retry until it finishes in a valid 3+1 setup.
         */
        RscAutoPlaceApiCall rscAutoPlaceApiCall = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            4,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB);
        int nodeId = 0;
        TreeSet<String> dcLocations = new TreeSet<>(
            Arrays.asList(
                "Vienna",
                "Berlin",
                "Frankfurt"
            )
        );
        for (String datacenter : dcLocations)
        {
            rscAutoPlaceApiCall = rscAutoPlaceApiCall
                .stltBuilder(String.format("node%02d.%s1", nodeId++, datacenter))
                .setNodeProp("Aux/DC", datacenter)
                .addStorPool("stor", 100 * GB)
                .build()
                .stltBuilder(String.format("node%02d.%s2", nodeId++, datacenter))
                .setNodeProp("Aux/DC", datacenter)
                .addStorPool("stor", 100 * GB)
                .build()
                .stltBuilder(String.format("node%02d.%s3", nodeId++, datacenter))
                .setNodeProp("Aux/DC", datacenter)
                .addStorPool("stor", 10 * GB)
                .build();
        }
        evaluateTest(
            rscAutoPlaceApiCall
                .putXReplicasOnDifferent("Aux/DC", 3)
                .addReplicasOnDifferentNodeProp("Aux/DC")
                .addReplicasOnDifferentNodeProp("Aux/DC2")
        );

        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(4, deployedNodes.size());

        Map<String, Integer> selectedDcs = new TreeMap<>();
        for (String dc : dcLocations)
        {
            selectedDcs.put(dc, 0);
        }
        for (Node node : deployedNodes)
        {
            String dc = node.getProps(SYS_CTX).getProp("Aux/DC");
            selectedDcs.put(dc, selectedDcs.get(dc) + 1);
        }

        Iterator<String> dcLocIterator = dcLocations.iterator();
        assertEquals(3, (int) selectedDcs.get(dcLocIterator.next()));
        assertEquals(1, (int) selectedDcs.get(dcLocIterator.next()));
        while (dcLocIterator.hasNext())
        {
            assertEquals(0, (int) selectedDcs.get(dcLocIterator.next()));
        }
    }

    @Test
    public void xReplicasOnDifferentWithPreexistingResourcesTest() throws Exception
    {
        /*
         * 5 datacenters, 3 nodes each, but 2 nodes (different DCs) have already a resource deployed.
         * Expected result: the already selected DCs should either be filled up or one of the pre-selected DCs should be
         * the on with only 1 replica
         */
        RscAutoPlaceApiCall rscAutoPlaceApiCall = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            5,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB);
        int nodeId = 0;
        final int nodesPerLocation = 3;
        TreeSet<String> dcLocations = new TreeSet<>(
            Arrays.asList(
                "Vienna",
                "Berlin",
                "Frankfurt",
                "London",
                "Paris"
            )
        );
        for (String datacenter : dcLocations)
        {
            for (int i = 1; i <= nodesPerLocation; i++)
            {
                rscAutoPlaceApiCall
                    .stltBuilder(String.format("node%02d.%s%d", nodeId++, datacenter, i))
                    .setNodeProp("Aux/DC", datacenter)
                    .addStorPool("stor", 10 * GB);
            }
        }
        rscAutoPlaceApiCall.addRsc(
            TEST_RSC_NAME,
            "stor",
            "node12.Vienna1",
            "node02.Berlin3"
        );

        evaluateTest(
            rscAutoPlaceApiCall
                .putXReplicasOnDifferent("Aux/DC", 2)
                .addReplicasOnDifferentNodeProp("Aux/DC")
        );

        // although not all SPs are equal, we still expect alphanummerically the first two DCs to contain 2 replicas
        // each and the third DC to have a single replica, since all nodes have the same equally good storage pool
        // (100G) and two not-that-good SPs (10G). so there is no "preferred" node, which means, alphanumeric sorting is
        // still expected
        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(5, deployedNodes.size());

        Map<String, Integer> selectedDcs = new TreeMap<>();
        for (Node node : deployedNodes)
        {
            String dc = node.getProps(SYS_CTX).getProp("Aux/DC");
            @Nullable Integer count = selectedDcs.get(dc);
            selectedDcs.put(dc, count == null ? 1 : count + 1);
        }

        assertEquals(3, selectedDcs.size());
        assertTrue(selectedDcs.containsKey("Vienna"));
        assertTrue(selectedDcs.containsKey("Berlin"));
    }

    @Test
    public void xReplicasOnDifferentWithPreexistingResourcesFillingAGroupTest() throws Exception
    {
        /*
         * 5 datacenters, 3 nodes each, 2 nodes of the same DC have a resource already deployed.
         */
        RscAutoPlaceApiCall rscAutoPlaceApiCall = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            5,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB);
        int nodeId = 0;
        final int nodesPerLocation = 3;
        TreeSet<String> dcLocations = new TreeSet<>(
            Arrays.asList(
                "Vienna",
                "Berlin",
                "Frankfurt",
                "London",
                "Paris"
            )
        );
        for (String datacenter : dcLocations)
        {
            for (int i = 1; i <= nodesPerLocation; i++)
            {
                rscAutoPlaceApiCall
                    .stltBuilder(String.format("node%02d.%s%d", nodeId++, datacenter, i))
                    .setNodeProp("Aux/DC", datacenter)
                    .addStorPool("stor", 10 * GB);
            }
        }
        rscAutoPlaceApiCall.addRsc(
            TEST_RSC_NAME,
            "stor",
            "node12.Vienna1",
            "node13.Vienna2"
        );

        evaluateTest(
            rscAutoPlaceApiCall
                .putXReplicasOnDifferent("Aux/DC", 2)
                .addReplicasOnDifferentNodeProp("Aux/DC")
        );

        // although not all SPs are equal, we still expect alphanummerically the first two DCs to contain 2 replicas
        // each and the third DC to have a single replica, since all nodes have the same equally good storage pool
        // (100G) and two not-that-good SPs (10G). so there is no "preferred" node, which means, alphanumeric sorting is
        // still expected
        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(5, deployedNodes.size());

        Map<String, Integer> selectedDcs = new TreeMap<>();
        for (Node node : deployedNodes)
        {
            String dc = node.getProps(SYS_CTX).getProp("Aux/DC");
            @Nullable Integer count = selectedDcs.get(dc);
            selectedDcs.put(dc, count == null ? 1 : count + 1);
        }

        assertEquals(3, selectedDcs.size());
        assertTrue(selectedDcs.containsKey("Vienna"));
        assertTrue(selectedDcs.containsKey("Berlin"));
    }

    @Test
    public void xReplicasOnDifferentWithExcludingSpecificValueTest() throws Exception
    {
        /*
         * 5 datacenters, 3 nodes each, one DC is highly attractive, but excluded via
         * "--replicas-on-different Aux/DC=Vienna"
         */
        RscAutoPlaceApiCall rscAutoPlaceApiCall = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            5,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB);
        int nodeId = 0;
        final int nodesPerLocation = 3;
        TreeSet<String> dcLocations = new TreeSet<>(
            Arrays.asList(
                "Berlin",
                "Frankfurt",
                "London",
                "Paris"
            )
        );
        for (String datacenter : dcLocations)
        {
            for (int i = 1; i <= nodesPerLocation; i++)
            {
                rscAutoPlaceApiCall
                    .stltBuilder(String.format("node%02d.%s%d", nodeId++, datacenter, i))
                    .setNodeProp("Aux/DC", datacenter)
                    .addStorPool("stor", 10 * GB);
            }
        }
        dcLocations.add("Vienna");
        for (int i = 1; i <= nodesPerLocation; i++)
        {
            rscAutoPlaceApiCall
                .stltBuilder(String.format("node%02d.%s%d", nodeId++, "Vienna", i))
                .setNodeProp("Aux/DC", "Vienna")
                .addStorPool("stor", 100 * GB);
        }

        evaluateTest(
            rscAutoPlaceApiCall
                .putXReplicasOnDifferent("Aux/DC", 2)
                .addReplicasOnDifferentNodeProp("Aux/DC=Vienna")
        );

        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(5, deployedNodes.size());

        Map<String, Integer> selectedDcs = new TreeMap<>();
        for (Node node : deployedNodes)
        {
            String dc = node.getProps(SYS_CTX).getProp("Aux/DC");
            @Nullable Integer count = selectedDcs.get(dc);
            selectedDcs.put(dc, count == null ? 1 : count + 1);
        }

        assertEquals(3, selectedDcs.size());
        assertFalse(selectedDcs.containsKey("Vienna"));
    }

    @Test
    public void xReplicasOnDifferentForceRollbackTest() throws Exception
    {
        /*
         * 5 datacenters, 3 nodes each, but the first (most attractive) DC has only one node (i.e. forces a
         * rollback).
         * requires a "--x-replicas-on-different X ... --place-count P" such that X % P = 0
         */
        RscAutoPlaceApiCall rscAutoPlaceApiCall = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            4,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB);
        int nodeId = 0;
        final int nodesPerLocation = 3;
        TreeSet<String> dcLocations = new TreeSet<>(
            Arrays.asList(
                "Berlin",
                "Frankfurt",
                "London",
                "Paris"
            )
        );
        for (String datacenter : dcLocations)
        {
            for (int i = 1; i <= nodesPerLocation; i++)
            {
                rscAutoPlaceApiCall
                    .stltBuilder(String.format("node%02d.%s%d", nodeId++, datacenter, i))
                    .setNodeProp("Aux/DC", datacenter)
                    .addStorPool("stor", 10 * GB);
            }
        }

        rscAutoPlaceApiCall
            .stltBuilder("node99.Vienna")
            .setNodeProp("Aux/DC", "Vienna")
            .addStorPool("stor", 100 * GB);

        evaluateTest(
            rscAutoPlaceApiCall
                .putXReplicasOnDifferent("Aux/DC", 2)
                .addReplicasOnDifferentNodeProp("Aux/DC")
        );

        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(4, deployedNodes.size());

        Map<String, Integer> selectedDcs = new TreeMap<>();
        for (Node node : deployedNodes)
        {
            String dc = node.getProps(SYS_CTX).getProp("Aux/DC");
            @Nullable Integer count = selectedDcs.get(dc);
            selectedDcs.put(dc, count == null ? 1 : count + 1);
        }

        assertEquals(2, selectedDcs.size());
        assertFalse(selectedDcs.containsKey("Vienna"));
    }

    @Test
    public void xReplicasOnDifferentNotEnoughNodesTest() throws Exception
    {
        /*
         * "--x-replicas-on-different 2 DC" test, but all DCs have only 1 node -> must fail
         */
        RscAutoPlaceApiCall rscAutoPlaceApiCall = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            4,
            false,
            ApiConsts.FAIL_NOT_ENOUGH_NODES
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB);
        int nodeId = 0;
        final int nodesPerLocation = 1;
        TreeSet<String> dcLocations = new TreeSet<>(
            Arrays.asList(
                "Berlin",
                "Frankfurt",
                "London",
                "Paris"
            )
        );
        for (String datacenter : dcLocations)
        {
            for (int i = 1; i <= nodesPerLocation; i++)
            {
                rscAutoPlaceApiCall
                    .stltBuilder(String.format("node%02d.%s%d", nodeId++, datacenter, i))
                    .setNodeProp("Aux/DC", datacenter)
                    .addStorPool("stor", 10 * GB);
            }
        }

        evaluateTest(
            rscAutoPlaceApiCall
                .putXReplicasOnDifferent("Aux/DC", 2)
                .addReplicasOnDifferentNodeProp("Aux/DC")
        );

        List<Node> deployedNodes = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(0, deployedNodes.size());
    }

    @Test
    public void replicasOnSameTest() throws Exception
    {

        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                5,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("node0.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node1.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node2.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node3.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()
            .stltBuilder("node4.val0")
                .setNodeProp("Aux/A", "0")
                .addStorPool("stor", 10 * GB)
                .build()

            .stltBuilder("node5.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 20 * GB)
                .build()
            .stltBuilder("node6.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 20 * GB)
                .build()
            .stltBuilder("node7.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 20 * GB)
                .build()
            .stltBuilder("node8.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 20 * GB)
                .build()
            .stltBuilder("node9.val1")
                .setNodeProp("Aux/A", "1")
                .addStorPool("stor", 20 * GB)
                .build()

            .addReplicasOnSameNodeProp("Aux/A")
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode) // we should have now only 2 nodes
            .collect(Collectors.toList());
        assertEquals(5, deployedNodes.size());

        for (int idx = 0; idx < 5; idx++)
        {
            assertEquals("1", deployedNodes.get(idx).getProps(GenericDbBase.SYS_CTX).getProp("Aux/A"));
        }
    }

    @Test
    public void replicasCombinedGithub89Test() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 100 * GB)
            .stltBuilder("m13c12")
                .addStorPool("thindata", null, 179 * GB, 900 * GB, LVM_THIN)
                .setNodeProp("Aux/moonshot", "13")
                .setNodeProp("Aux/opennebula-1", "true")
                .build()
            .stltBuilder("m14c21")
                .addStorPool("thindata", null, 203 * GB, 900 * GB, LVM_THIN)
                .setNodeProp("Aux/moonshot", "14")
                .setNodeProp("Aux/opennebula-1", "true")
                .build()
            .stltBuilder("m10c12")
                .addStorPool("thindata", null, 900 * GB, 900 * GB, LVM_THIN)
                .setNodeProp("Aux/moonshot", "10")
                .setNodeProp("Aux/opennebula-1", "true")
                .build()
            .stltBuilder("m15c12")
                .addStorPool("thindata", null, 900 * GB, 900 * GB, LVM_THIN)
                .setNodeProp("Aux/moonshot", "15")
                .setNodeProp("Aux/opennebula-1", "true")
                .build()

            .addReplicasOnSameNodeProp("Aux/opennebula-1")
            .addReplicasOnDifferentNodeProp("Aux/moonshot")
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode)
            .collect(Collectors.toList());

        assertEquals(2, deployedNodes.size());
        assertEquals("m10c12", deployedNodes.get(0).getName().displayValue);
        assertEquals("m15c12", deployedNodes.get(1).getName().displayValue);
    }

    @Test
    public void avoidNodeWithoutPropWhenUsingReplOnSameTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB)
                // prefered from free space, but missing "rack" property
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 90 * GB)
                // second prefered from free space, but has no second node with same "rack" property
                .setNodeProp("Aux/rack", "1")
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 10 * GB)
                .setNodeProp("Aux/rack", "2")
                .build()
            .stltBuilder("stlt4")
                .addStorPool("sp1", 10 * GB)
                .setNodeProp("Aux/rack", "2")
                .build()

            .addReplicasOnSameNodeProp("Aux/rack")
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(Resource::getNode)
            .collect(Collectors.toList());

        assertEquals(2, deployedNodes.size());
        assertEquals("stlt3", deployedNodes.get(0).getName().displayValue);
        assertEquals("stlt4", deployedNodes.get(1).getName().displayValue);
    }

    @Test
    public void disklessRemainingTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("stlt1").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt2").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt3").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt4").addStorPool("stor", 10 * GB).build()
            .disklessOnRemaining(true)
        );

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode()) // we should have now only 2 diskfull and 2 diskless nodes
            .collect(Collectors.toList());
        assertEquals(4, deployedNodes.size());

        long disklessNodes = deployedNodes.stream().filter(
            node ->
            {
                assertEquals(1, node.getResourceCount()); // just to be sure
                try
                {
                    return node.getResource(GenericDbBase.SYS_CTX, new ResourceName(TEST_RSC_NAME))
                        .getStateFlags()
                        .isSet(GenericDbBase.SYS_CTX, Resource.Flags.DRBD_DISKLESS);
                }
                catch (AccessDeniedException | InvalidNameException exc)
                {
                    throw new RuntimeException(exc);
                }
            }
        ).count();
        long diskfullNodes = deployedNodes.size() - disklessNodes;

        assertEquals(2, disklessNodes);
        assertEquals(2, diskfullNodes);
    }

    @Test
    public void idempotencyTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("stlt1").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt2").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt3").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt4").addStorPool("stor", 10 * GB).build()
            .disklessOnRemaining(false)
        );

        // we should now have some resources deployed. We do not really care where they are
        // but still make sure that they are 2.

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());

        // rerun the same apiCall, but this time we should receive a different RC
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                false,
                ApiConsts.MASK_CRT | ApiConsts.MASK_RSC | ApiConsts.WARN_RSC_ALREADY_DEPLOYED // rsc autoplace
            )
            // no need for addVlmDfn or stltBuilderCalls. We are in the same instance, the controller
            // should still know about the previously configured objects
        );

        // recheck
        deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());
    }

    @Test
    public void extendAutoPlacedRscTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("stlt1").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt2").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt3").addStorPool("stor", 10 * GB).build()
            .stltBuilder("stlt4").addStorPool("stor", 10 * GB).build()
            .disklessOnRemaining(false)
        );

        // we should now have some resources deployed. We do not really care where they are
        // but still make sure that they are 2.

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());

        // rerun the same apiCall, but this time with +1 replicas
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                3,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            // no need for addVlmDfn or stltBuilderCalls. We are in the same instance, the controller
            // should still know about the previously configured objects
            .disklessOnRemaining(true)
        );

        // recheck
        deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(4, deployedNodes.size());

        long disklessNodes = deployedNodes.stream().filter(
            node ->
            {
                assertEquals(1, node.getResourceCount()); // just to be sure
                try
                {
                    return node.getResource(GenericDbBase.SYS_CTX, new ResourceName(TEST_RSC_NAME))
                        .getStateFlags()
                        .isSet(GenericDbBase.SYS_CTX, Resource.Flags.DRBD_DISKLESS);
                }
                catch (AccessDeniedException | InvalidNameException exc)
                {
                    throw new RuntimeException(exc);
                }
            }
        ).count();
        long diskfullNodes = deployedNodes.size() - disklessNodes;

        assertEquals(1, disklessNodes);
        assertEquals(3, diskfullNodes);
    }

    @Test
    public void extendAutoPlacedRscOnDifferentStorPoolsTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("stlt1").addStorPool("stor", 100 * GB).build()
            .stltBuilder("stlt2").addStorPool("stor", 100 * GB).build()
            .stltBuilder("stlt3").addStorPool("stor", 10 * GB).addStorPool("stor2", 10 * GB).build()
            .stltBuilder("stlt4").addStorPool("stor", 10 * GB).addStorPool("stor2", 10 * GB).build()
            .disklessOnRemaining(false)
        );

        // we should now have some resources deployed, namely on stlt1 and stlt2 (size of storpools)

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());

        assertThat(deployedNodes.stream().map(node -> node.getName().displayValue).collect(Collectors.toList()))
            .contains("stlt1", "stlt2");

        // rerun the same apiCall, but this time with +1 replicas
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                4,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
                // no need for addVlmDfn or stltBuilderCalls. We are in the same instance, the controller
                // should still know about the previously configured objects
                .addStorPool(
                    "stor2"
                )
            .disklessOnRemaining(true)
        );

        // recheck
        deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(4, deployedNodes.size());

        ResourceName rscName = new ResourceName(TEST_RSC_NAME);
        Assert.assertEquals(
            "stor",
            nodesMap.get(new NodeName("stlt1"))
                .getResource(GenericDbBase.SYS_CTX, rscName)
                .getLayerData(SYS_CTX) // drbd layer
                .getSingleChild() // storage layer
                .getVlmProviderObject(new VolumeNumber(0))
                .getStorPool()
                .getName()
                .displayValue
        );
        Assert.assertEquals(
            "stor",
            nodesMap.get(new NodeName("stlt2"))
                .getResource(GenericDbBase.SYS_CTX, rscName)
                .getLayerData(SYS_CTX) // drbd layer
                .getSingleChild() // storage layer
                .getVlmProviderObject(new VolumeNumber(0))
                .getStorPool()
                .getName()
                .displayValue
        );
        Assert.assertEquals(
            "stor2",
            nodesMap.get(new NodeName("stlt3"))
                .getResource(GenericDbBase.SYS_CTX, rscName)
                .getLayerData(SYS_CTX) // drbd layer
                .getSingleChild() // storage layer
                .getVlmProviderObject(new VolumeNumber(0))
                .getStorPool()
                .getName()
                .displayValue
        );
        Assert.assertEquals(
            "stor2",
            nodesMap.get(new NodeName("stlt4"))
                .getResource(GenericDbBase.SYS_CTX, rscName)
                .getLayerData(SYS_CTX) // drbd layer
                .getSingleChild() // storage layer
                .getVlmProviderObject(new VolumeNumber(0))
                .getStorPool()
                .getName()
                .displayValue
        );
    }

    @Test
    public void layerStackTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("stlt1")
                .addStorPool("stor", 10 * GB)
                .setSupportedLayers(DRBD, LUKS, STORAGE)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("stor", 10 * GB)
                .setSupportedLayers(DRBD, LUKS, STORAGE)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("stor", 100 * GB)
                .setSupportedLayers(LUKS, STORAGE) // no DRBD :(
                .build()
            .setLayerStack(DRBD, STORAGE)
            .disklessOnRemaining(false)
        );

        // although stlt3 has the most storage, as it does not support DRBD, the other two
        // should be selected

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());
        assertEquals("stlt1", deployedNodes.get(0).getName().displayValue);
        assertEquals("stlt2", deployedNodes.get(1).getName().displayValue);
    }

    @Test
    public void providerTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("stlt1")
                .addStorPool("stor", 10 * GB, LVM)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("stor", 10 * GB, LVM)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("stor", 100 * GB, ZFS) // no LVM
                .build()
            .setProvider(LVM, LVM_THIN)
            .disklessOnRemaining(false)
        );

        // although stlt3 has the most storage, as it does not support LVM, the other two
        // should be selected

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());
        assertEquals("stlt1", deployedNodes.get(0).getName().displayValue);
        assertEquals("stlt2", deployedNodes.get(1).getName().displayValue);
    }

    @Test
    public void repsectNodePropertiesFixedByAlreadyDepoyedResource() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * GB)
            .stltBuilder("stlt1")
                .addStorPool("stor", 100 * GB, LVM)
                .setNodeProp("Aux/a", "a")
                .build()
            .stltBuilder("stlt2")
                .addStorPool("stor", 90 * GB, LVM)
                .setNodeProp("Aux/a", "a")
                .build()
            .stltBuilder("stlt3")
                .addStorPool("stor", 70 * GB, LVM)
                .setNodeProp("Aux/a", "a")
                .build()
            .stltBuilder("stlt4")
                .addStorPool("stor", 80 * GB, LVM)
                .setNodeProp("Aux/a", "b")
                .build()
            .disklessOnRemaining(false)
        );
        // will prioritize stlt1 and stlt2 based on free space

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(2, deployedNodes.size());
        assertEquals("stlt1", deployedNodes.get(0).getName().displayValue);
        assertEquals("stlt2", deployedNodes.get(1).getName().displayValue);

        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                3,
                true,
                ApiConsts.CREATED, // property set
                ApiConsts.CREATED // rsc autoplace
            )
            .addReplicasOnSameNodeProp("Aux/a")
        );

        // although stlt 4 has more free space, the replicasOnSame forces the selection to stlt3
        deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .collect(Collectors.toList());
        assertEquals(3, deployedNodes.size());
        deployedNodes.stream().map(node -> node.getName().displayValue).forEach(System.out::println);
        assertEquals("stlt1", deployedNodes.get(0).getName().displayValue);
        assertEquals("stlt2", deployedNodes.get(1).getName().displayValue);
        assertEquals("stlt3", deployedNodes.get(2).getName().displayValue);
    }

    @Test
    public void scalingTest() throws Exception
    {
        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            2,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        ).addVlmDfn(TEST_RSC_NAME, 0, 1 * GB);

        for (int nodeIdx = 0; nodeIdx < 512; nodeIdx++)
        {
            int propA = (nodeIdx >> 8) % 2;
            int propB = (nodeIdx >> 7) % 2;
            int propC = (nodeIdx >> 6) % 2;
            int propD = (nodeIdx >> 5) % 2;
            int propE = (nodeIdx >> 4) % 2;
            int propF = (nodeIdx >> 3) % 2;
            int propG = (nodeIdx >> 2) % 2;
            int propH = (nodeIdx >> 1) % 2;
            int propI = nodeIdx % 2;

            call = call.stltBuilder(String.format("stlt.%03d", nodeIdx)) // fill with leading zeroes
                .addStorPool("sp" + nodeIdx + "_1", 20 * GB, LVM)
                .setNodeProp("Aux/a", "" + propA)
                .setNodeProp("Aux/b", "" + propB)
                .setNodeProp("Aux/c", "" + propC)
                .setNodeProp("Aux/d", "" + propD)
                .setNodeProp("Aux/e", "" + propE)
                .setNodeProp("Aux/f", "" + propF)
                .setNodeProp("Aux/g", "" + propG)
                .setNodeProp("Aux/h", "" + propH)
                .setNodeProp("Aux/i", "" + propI)
                .build();
        }

        /*
         * we just created 512 nodes with one SP each. half of the nodes have Aux/a set to 0
         * the other half set to 1.
         * You can imagine these 512 nodes in a truth-table, where the properties
         * a,b,c,d,e,f,g,h and i are boolean variables in all possible combinations.
         */
        /*
         * now we have to configure the autoplacer to choose storage pools which we can
         * deterministically expect :)
         *
         * However, as this is a scaling test, forcing properties to a certain value does not
         * test scaling properly. We have 9 properties. If we set all but one property to a
         * certain value, the very first step (i.e. "Filter") of the new autoplacer will
         * actually remove all non-matching combination, making this test way too easy for
         * the autoplacer.
         *
         * For stressing the autoplacer we use 8 non-fixed replicas-on-same properties, allowing
         * the last property to be different of the two selected nodes
         */

        call = call
            // not adding Aux/a
            .addReplicasOnSameNodeProp("Aux/b")
            .addReplicasOnSameNodeProp("Aux/c")
            .addReplicasOnSameNodeProp("Aux/d")
            .addReplicasOnSameNodeProp("Aux/e")
            .addReplicasOnSameNodeProp("Aux/f")
            .addReplicasOnSameNodeProp("Aux/g")
            .addReplicasOnSameNodeProp("Aux/h")
            .addReplicasOnSameNodeProp("Aux/i");

        /*
         * we know that 8 of the 9 properties have to be the same, that means either 1 or 0.
         * we also know that a (the "most significant bit") will differ.
         * that means, the difference in the index of the two satellites will be 256
         */

        Level logLevel = errorReporter.getCurrentLogLevel();
        errorReporter.setLogLevel(SYS_CTX, Level.TRACE, Level.TRACE);
        long start = System.currentTimeMillis();
        evaluateTest(call);
        System.out.println((System.currentTimeMillis() - start));
        errorReporter.setLogLevel(SYS_CTX, logLevel, logLevel);

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .map(rsc -> rsc.getNode())
            .sorted()
            .collect(Collectors.toList());

        assertEquals(2, deployedNodes.size());
        int lowerId = Integer.parseInt(deployedNodes.get(0).getName().displayValue.substring("stlt.".length()));
        assertEquals(String.format("stlt.%03d", lowerId + 256), deployedNodes.get(1).getName().displayValue);
    }

    @Test
    public void minRscCountStrategyTest() throws Exception
    {
        enterScope();
        ctrlConf.setProp(ApiConsts.NAMESPC_AUTOPLACER_WEIGHTS + "/FreeSpace", "0");
        ctrlConf.setProp(ApiConsts.NAMESPC_AUTOPLACER_WEIGHTS + "/MinRscCount", "1");
        commitAndCleanUp(true);

        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            1,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 90 * GB)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 50 * GB)
                .build()
            .addRscDfn("dummyRsc1")
                .addVlmDfn("dummyRsc1", 0, 12 * MB)
            .addRscDfn("dummyRsc2")
                .addVlmDfn("dummyRsc2", 0, 12 * MB)
            .addRscDfn("dummyRsc3")
                .addVlmDfn("dummyRsc3", 0, 12 * MB)
            .addRsc("dummyRsc1", "sp1", "stlt1")
            .addRsc("dummyRsc2", "sp1", "stlt1")
            .addRsc("dummyRsc3", "sp1", "stlt2");

        evaluateTest(call);

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .map(rsc -> rsc.getNode())
            .sorted()
            .collect(Collectors.toList());

        assertEquals(1, deployedNodes.size());
        assertEquals("stlt3", deployedNodes.get(0).getName().displayValue);
    }

    @Test
    public void maxThroughputStrategyTest() throws Exception
    {
        enterScope();
        ctrlConf.setProp(ApiConsts.NAMESPC_AUTOPLACER_WEIGHTS + "/FreeSpace", "0");
        ctrlConf.setProp(ApiConsts.NAMESPC_AUTOPLACER_WEIGHTS + "/MaxThroughput", "1");
        commitAndCleanUp(true);

        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            1,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
                .setVlmDfnProp(
                    TEST_RSC_NAME,
                    0,
                    ApiConsts.NAMESPC_SYS_FS + "/"  + ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_READ,
                    "100"
                )
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB)
                // sp with highest max throughput - but having already some rscs / vlms deployed
                .setStorPoolProp(
                    "sp1",
                    ApiConsts.NAMESPC_AUTOPLACER + "/" + ApiConsts.KEY_AUTOPLACE_MAX_THROUGHPUT,
                    "1000"
                )
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 100 * GB)
                .setStorPoolProp(
                    "sp1",
                    ApiConsts.NAMESPC_AUTOPLACER + "/" + ApiConsts.KEY_AUTOPLACE_MAX_THROUGHPUT,
                    "800"
                )
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 100 * GB)
                .setStorPoolProp(
                    "sp1",
                    ApiConsts.NAMESPC_AUTOPLACER + "/"  + ApiConsts.KEY_AUTOPLACE_MAX_THROUGHPUT,
                    "500"
                )
                .build()

            .addRscDfn("dummyRsc1")
                .addVlmDfn("dummyRsc1", 0, 12 * MB)
                .setVlmDfnProp(
                    "dummyRsc1",
                    0,
                    ApiConsts.NAMESPC_SYS_FS + "/" + ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_READ,
                    "100"
                )
            .addRscDfn("dummyRsc2")
                .addVlmDfn("dummyRsc2", 0, 12 * MB)
                .setVlmDfnProp(
                    "dummyRsc2",
                    0,
                    ApiConsts.NAMESPC_SYS_FS + "/" + ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_READ,
                    "100"
                )
            .addRscDfn("dummyRsc3")
                .addVlmDfn("dummyRsc3", 0, 12 * MB)
                .setVlmDfnProp(
                    "dummyRsc3",
                    0,
                    ApiConsts.NAMESPC_SYS_FS + "/" + ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_READ,
                    "100"
                )
            .addRsc("dummyRsc1", "sp1", "stlt1")
            .addRsc("dummyRsc2", "sp1", "stlt1")
            .addRsc("dummyRsc3", "sp1", "stlt1");

        evaluateTest(call);

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
                )
            .map(rsc -> rsc.getNode())
            .sorted()
            .collect(Collectors.toList());

        assertEquals(1, deployedNodes.size());
        assertEquals("stlt2", deployedNodes.get(0).getName().displayValue);
    }

    @Test
    public void freeSpaceReversedTest() throws Exception
    {
        enterScope();
        ctrlConf.setProp(
            ApiConsts.NAMESPC_AUTOPLACER_WEIGHTS + "/" + ApiConsts.KEY_AUTOPLACE_STRAT_WEIGHT_MAX_FREESPACE,
            "-1"
        );
        commitAndCleanUp(true);

        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            1,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 90 * GB)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 50 * GB)
                .build();

        evaluateTest(call);

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .map(rsc -> rsc.getNode())
            .sorted()
            .collect(Collectors.toList());

        assertEquals(1, deployedNodes.size());
        assertEquals("stlt3", deployedNodes.get(0).getName().displayValue);
    }

    @Test
    public void preventMixedProviderTest() throws Exception
    {
        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            2,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB, LVM)
                .build()
            .stltBuilder("stlt2") // DO NOT select this
                .addStorPool("sp1", 1 * TB, LVM_THIN)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 900 * GB, LVM)
                .build();

        evaluateTest(call);

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .map(rsc -> rsc.getNode())
            .sorted()
            .collect(Collectors.toList());

        assertEquals(2, deployedNodes.size());
        assertEquals("stlt1", deployedNodes.get(0).getName().displayValue);
        assertEquals("stlt3", deployedNodes.get(1).getName().displayValue);
    }

    @Test
    public void createRscWithDrbdProxyTest() throws Exception
    {
        enterScope();
        ctrlConf.setProp(ApiConsts.NAMESPC_DRBD_PROXY + "/" + ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE, "true");
        commitAndCleanUp(true);

        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            3,
            true
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .setNodeProp(ApiConsts.KEY_SITE, "A")
                .addStorPool("sp1", 100 * GB, LVM)
                .setExtToolSupported(ExtTools.DRBD_PROXY, true, 42, 42, 9001)
                .build()
            .stltBuilder("stlt2")
                .setNodeProp(ApiConsts.KEY_SITE, "A")
                .addStorPool("sp1", 100 * GB, LVM)
                .setExtToolSupported(ExtTools.DRBD_PROXY, true, 42, 42, 9001)
                .build()
            .stltBuilder("stlt3")
                .setNodeProp(ApiConsts.KEY_SITE, "B")
                .addStorPool("sp1", 100 * GB, LVM)
                .setExtToolSupported(ExtTools.DRBD_PROXY, true, 42, 42, 9001)
                .build();

        evaluateTest(call, false);

        List<Resource> deployedRscs = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .sorted()
            .collect(Collectors.toList());
        assertEquals(3, deployedRscs.size());

        Resource rsc1 = deployedRscs.get(0);
        Resource rsc2 = deployedRscs.get(1);
        Resource rsc3 = deployedRscs.get(2);

        // expect NO proxy between stlt1 <-> stlt2
        assertNull(rsc1.getAbsResourceConnection(SYS_CTX, rsc2));

        // expect proxy between stlt1 <-> stlt3
        ResourceConnection rscCon13 = rsc1.getAbsResourceConnection(SYS_CTX, rsc3);
        assertNotNull(rscCon13);
        assertNotNull(rscCon13.getDrbdProxyPortSource(SYS_CTX));
        assertTrue(rscCon13.getStateFlags().isSet(SYS_CTX, ResourceConnection.Flags.LOCAL_DRBD_PROXY));

        // expect proxy between stlt2 <-> stlt3
        ResourceConnection rscCon23 = rsc2.getAbsResourceConnection(SYS_CTX, rsc3);
        assertNotNull(rscCon23);
        assertNotNull(rscCon23.getDrbdProxyPortSource(SYS_CTX));
        assertTrue(rscCon23.getStateFlags().isSet(SYS_CTX, ResourceConnection.Flags.LOCAL_DRBD_PROXY));
    }

    @Test
    public void keepFirstCandidateAfterContinuedSearchTest() throws Exception
    {
        /*
         * Test for GitHub issue 139
         *
         * Scenario: The Autoplacer.Selector happily finds the first candidate but is unsure
         * if that is the best candidate or not, so it continues searching for a while.
         * When the selector is confident enough that there cannot be any better candidates
         * it simply returns the stored candidate.
         *
         * 139's bug is that the returned "stored" set of candidates was actually the same
         * reference as the Selector's internal "currentCandidateSet", which gets cleared
         * if a new search starts (i.e. "the search continues" after a candidate was found)
         */

        final String testKey = "Aux/test";
        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            2,
            true,
            ApiConsts.CREATED,
            ApiConsts.CREATED,
            ApiConsts.CREATED
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .setNodeProp(testKey, "a")
                .addStorPool("sp1", 100 * GB, LVM_THIN) // best stor-pool, best candidate
                .build()
            .stltBuilder("stlt2")
                .setNodeProp(testKey, "a")
                // worst stor-pool but the only that can be combined with best candidate
                .addStorPool("sp1", 10 * GB, LVM_THIN)
                .build()
            .stltBuilder("stlt3")
                .setNodeProp(testKey, "b")
                // better than the worst, but not as good as best, also not best candidate
                .addStorPool("sp1", 30 * GB, LVM_THIN)
                .build()
            .stltBuilder("stlt4")
                .setNodeProp(testKey, "b")
                // better than the worst, but not as good as best, also not best candidate
                .addStorPool("sp1", 30 * GB, LVM_THIN)
                .build()
            .stltBuilder("stlt5")
                .setNodeProp(testKey, "c")
                // theoretically combinable to get a score higher than currently best, but
                // practically not combinable to a candidate due to the property-constraint
                .addStorPool("sp1", 90 * GB, LVM_THIN)
                .build()
            .addReplicasOnSameNodeProp(testKey)
            .addStorPool("sp1");

        evaluateTest(call);

        List<Resource> deployedRscs = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .sorted()
            .collect(Collectors.toList());

        assertEquals(2, deployedRscs.size());
    }

    @Test
    public void autoPlaceAdditionalTest() throws Exception
    {
        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            null,
            true,
            ApiConsts.CREATED, // property set
            ApiConsts.CREATED // rsc autoplace
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 90 * GB)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 50 * GB)
                .build()
            .addRsc(TEST_RSC_NAME, "sp1", "stlt1")
            .setAdditionalPlaceCount(1);

        evaluateTest(call);

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .map(rsc -> rsc.getNode())
            .sorted()
            .collect(Collectors.toList());

        assertEquals(2, deployedNodes.size());
    }

    @Test
    public void doNotMixStorPoolsTest() throws Exception
    {
        /*
         * Scenario: We already have a diskful resource in an LVM pool, the
         * autoplace should reject the "best candidate" if it is not an LVM pool.
         */
        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            2,
            true,
            ApiConsts.CREATED,
            ApiConsts.CREATED
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB, LVM_THIN)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 100 * GB, ZFS)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 100 * GB, ZFS_THIN)
                .build()
            .stltBuilder("stlt4")
                .addStorPool("sp1", 10 * GB, LVM)
                .build()
            .stltBuilder("stlt5")
                .addStorPool("sp1", 90 * GB, LVM)
                .build()
            .addRsc(TEST_RSC_NAME, "sp1", "stlt5");
        evaluateTest(call);

        List<Resource> deployedRscs = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .sorted()
            .collect(Collectors.toList());
        assertEquals(2, deployedRscs.size());
        assertEquals("stlt4", deployedRscs.get(0).getNode().getName().displayValue);
        assertEquals("stlt5", deployedRscs.get(1).getNode().getName().displayValue);
    }

    @Test
    public void allowMixStorPoolWithRecentEnoughDrbdTest() throws Exception
    {
        /*
         * Scenario: We already have a diskful resource in an LVM pool, the
         * autoplace should reject the "best candidate" if it is not an LVM pool.
         */
        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            2,
            true,
            ApiConsts.CREATED,
            ApiConsts.CREATED
        )
            .setRscDfnProp(TEST_RSC_NAME, ApiConsts.KEY_RSC_ALLOW_MIXING_DEVICE_KIND, ApiConsts.VAL_TRUE)
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB, LVM_THIN)
                .setExtToolSupported(ExtTools.DRBD9_KERNEL, true, 9, 1, 18)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 100 * GB, ZFS)
                .setExtToolSupported(ExtTools.DRBD9_KERNEL, true, 9, 1, 18)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 100 * GB, ZFS_THIN)
                .setExtToolSupported(ExtTools.DRBD9_KERNEL, true, 9, 1, 18)
                .build()
            .stltBuilder("stlt4")
                .addStorPool("sp1", 10 * GB, LVM)
                .setExtToolSupported(ExtTools.DRBD9_KERNEL, true, 9, 1, 18)
                .build()
            .stltBuilder("stlt5")
                .addStorPool("sp1", 90 * GB, LVM)
                .setExtToolSupported(ExtTools.DRBD9_KERNEL, true, 9, 1, 18)
                .build()
            .addRsc(TEST_RSC_NAME, "sp1", "stlt5");
        evaluateTest(call);

        List<Resource> deployedRscs = nodesMap.values()
            .stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .sorted()
            .collect(Collectors.toList());
        assertEquals(2, deployedRscs.size());
        assertEquals("stlt1", deployedRscs.get(0).getNode().getName().displayValue);
        assertEquals("stlt5", deployedRscs.get(1).getNode().getName().displayValue);
    }

    @Test
    public void extToolsTest() throws Exception
    {
        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            1,
            true,
            ApiConsts.CREATED,
            ApiConsts.CREATED
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB, ZFS)
                .setExtToolSupported(ExtTools.ZSTD, true, 1, 0, 0)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 200 * GB, ZFS) // larger but no ZSTD support
                .build()
            .addRequiredExtTools(ExtTools.ZSTD, 0, 0, 0);
        evaluateTest(call);

        List<Resource> deployedRscs = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .sorted()
            .collect(Collectors.toList());
        assertEquals(1, deployedRscs.size());
        assertEquals("stlt1", deployedRscs.get(0).getNode().getName().displayValue);
    }


    @Test
    public void autoPlaceAllowTargetNodePropTest() throws Exception
    {
        RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
            TEST_RSC_NAME,
            2,
            true,
            ApiConsts.CREATED,
            ApiConsts.CREATED,
            ApiConsts.CREATED
        )
            .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
            .stltBuilder("stlt1")
                .addStorPool("sp1", 100 * GB)
                .setNodeProp(ApiConsts.KEY_AUTOPLACE_ALLOW_TARGET, "false")
                .build()
            .stltBuilder("stlt2")
                .addStorPool("sp1", 90 * GB)
                .build()
            .stltBuilder("stlt3")
                .addStorPool("sp1", 50 * GB)
                .build();

        evaluateTest(call);

        List<Node> deployedNodes = nodesMap.values().stream()
            .flatMap(this::streamResources)
            .filter(
                rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
            )
            .map(rsc -> rsc.getNode())
            .sorted()
            .collect(Collectors.toList());

        assertEquals(2, deployedNodes.size());
        assertEquals("stlt2", deployedNodes.get(0).getName().displayValue);
        assertEquals("stlt3", deployedNodes.get(1).getName().displayValue);
    }

    // @Test
    // public void autoPlaceAllowTargetStorPoolPropTest() throws Exception
    // {
    // RscAutoPlaceApiCall call = new RscAutoPlaceApiCall(
    // TEST_RSC_NAME,
    // 2,
    // true,
    // ApiConsts.CREATED,
    // ApiConsts.CREATED,
    // ApiConsts.CREATED
    // )
    // .addVlmDfn(TEST_RSC_NAME, 0, 1 * GB)
    // .stltBuilder("stlt1")
    // .addStorPool("sp1", 100 * GB)
    // .setStorPoolProp("sp1", ApiConsts.KEY_AUTOPLACE_ALLOW_TARGET, "false")
    // .build()
    // .stltBuilder("stlt2")
    // .addStorPool("sp1", 90 * GB)
    // .build()
    // .stltBuilder("stlt3")
    // .addStorPool("sp1", 50 * GB)
    // .build();
    //
    // evaluateTest(call);
    //
    // List<Node> deployedNodes = nodesMap.values().stream()
    // .flatMap(this::streamResources)
    // .filter(
    // rsc -> rsc.getResourceDefinition().getName().displayValue.equals(TEST_RSC_NAME)
    // )
    // .map(rsc -> rsc.getNode())
    // .sorted()
    // .collect(Collectors.toList());
    //
    // assertEquals(2, deployedNodes.size());
    // assertEquals("stlt2", deployedNodes.get(0).getName().displayValue);
    // assertEquals("stlt3", deployedNodes.get(1).getName().displayValue);
    // }

    private void expectDeployed(
        String storPoolNameStr,
        String rscNameStr,
        String... nodeNameStrs
    )
        throws Exception
    {
        StorPoolName storPoolName = new StorPoolName(storPoolNameStr);
        ResourceName rscName = new ResourceName(rscNameStr);

        for (String nodeNameStr : nodeNameStrs)
        {
            Node node = nodesMap.get(new NodeName(nodeNameStr));
            Resource rsc = node.getResource(GenericDbBase.SYS_CTX, rscName);
            assertNotNull(rsc);

            Iterator<Volume> vlmIt = rsc.iterateVolumes();
            assertTrue(vlmIt.hasNext());

            while (vlmIt.hasNext())
            {
                Volume vlm = vlmIt.next();
                assertEquals(
                    storPoolName,
                    vlm.getAbsResource()
                        .getLayerData(SYS_CTX) // drbd layer
                        .getSingleChild() // storage layer
                        .getVlmProviderObject(vlm.getVolumeDefinition().getVolumeNumber())
                        .getStorPool()
                        .getName()
                );
            }
        }
    }

    private void expectNotDeployed(String rscNameStr) throws Exception
    {
        ResourceName rscName = new ResourceName(rscNameStr);

        ResourceDefinition rscDfn = rscDfnMap.get(rscName);

        assertThat(rscDfn.getResourceCount()).isEqualTo(0);
    }


    private ResourceDefinition createRscDfn(String rscNameStr)
        throws Exception
    {
        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.sharedSecret = "NotTellingYou";
        drbdRscDfn.transportType = TransportType.IP;
        ResourceDefinition rscDfn = resourceDefinitionFactory.create(
            BOB_ACC_CTX,
            new ResourceName(rscNameStr),
            null,
            null,
            Arrays.asList(DRBD, STORAGE),
            payload,
            dfltRscGrp
        );

        rscDfnMap.put(rscDfn.getName(), rscDfn);

        return rscDfn;
    }

    private Stream<Resource> streamResources(Node node)
    {
        Stream<Resource> ret;
        try
        {
            ret = node.streamResources(GenericDbBase.SYS_CTX);
        }
        catch (AccessDeniedException exc)
        {
            throw new RuntimeException(exc);
        }
        return ret;
    }

    private class RscAutoPlaceApiCall extends AbsApiCallTester
    {
        private final String rscNameStr;
        private @Nullable Integer placeCount;
        private @Nullable Integer additionalPlaceCount;

        private final List<String> doNotPlaceWithRscList = new ArrayList<>();
        private final List<String> nodeNameList = new ArrayList<>();
        private final List<String> storPoolNameList = new ArrayList<>();
        private final List<String> storPoolDisklessNameList = new ArrayList<>();
        private @Nullable String doNotPlaceWithRscRegexStr = null;

        private final List<String> replicasOnSameNodePropList = new ArrayList<>();
        private final List<String> replicasOnDifferentNodePropList = new ArrayList<>();
        private final Map<String, Integer> xReplicasOnDifferentMap = new TreeMap<>();
        private boolean disklessOnRemaining;
        private List<String> skipAlreadyPlacedOnNodeCheck;
        private boolean skipAlreadyPlacedOnAllNodeCheck = false;

        private final List<DeviceLayerKind> layerStack = new ArrayList<>(Arrays.asList(DRBD, STORAGE));
        private final List<DeviceProviderKind> providerList =
            new ArrayList<>(Arrays.asList(DeviceProviderKind.values()));
        private @Nullable String disklessType;
        private Map<ExtTools, ExtToolsInfo.Version> requiredExtTools = null;
        private @Nullable Integer portCount = null;

        RscAutoPlaceApiCall(
            String rscNameStrRef,
            Integer placeCountRef,
            boolean expectDeployment,
            long... expectedRetCodes
        )
        {
            super(
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_CRT,
                expectDeployment ?
                    // When the resources are successfully registered in the DB, the API call handler should try to
                    // deploy them on the satellites. We deliberately cause this to fail. Hence we expect a failure
                    // response after the registration success responses.
                    LongStream.concat(
                        LongStream.of(expectedRetCodes),
                        LongStream.of(ApiConsts.FAIL_UNKNOWN_ERROR)
                    ).toArray() :
                    expectedRetCodes
            );
            rscNameStr = rscNameStrRef;
            placeCount = placeCountRef;

            // user should not be able to enter this :)
            providerList.remove(DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER);
        }

        public RscAutoPlaceApiCall addRequiredExtTools(ExtTools extTool, Integer major, Integer minor, Integer patch)
        {
            if (requiredExtTools == null)
            {
                requiredExtTools = new HashMap<>();
            }
            requiredExtTools.put(extTool, new Version(major, minor, patch));
            return this;
        }

        RscAutoPlaceApiCall setPlaceCount(Integer placeCountRef)
        {
            placeCount = placeCountRef;
            return this;
        }

        RscAutoPlaceApiCall setAdditionalPlaceCount(Integer additionalPlaceCountRef)
        {
            additionalPlaceCount = additionalPlaceCountRef;
            return this;
        }

        public RscAutoPlaceApiCall setProvider(DeviceProviderKind... kinds)
        {
            providerList.clear();
            providerList.addAll(Arrays.asList(kinds));
            return this;
        }

        public RscAutoPlaceApiCall setLayerStack(DeviceLayerKind... kinds)
        {
            layerStack.clear();
            layerStack.addAll(Arrays.asList(kinds));
            return this;
        }

        RscAutoPlaceApiCall setDoNotPlaceWithRegex(String doNotPlaceWithRscRegexStrRef)
        {
            doNotPlaceWithRscRegexStr = doNotPlaceWithRscRegexStrRef;
            return this;
        }

        RscAutoPlaceApiCall addReplicasOnSameNodeProp(String nodePropKey)
        {
            replicasOnSameNodePropList.add(nodePropKey);
            return this;
        }

        RscAutoPlaceApiCall addReplicasOnDifferentNodeProp(String nodePropKey)
        {
            replicasOnDifferentNodePropList.add(nodePropKey);
            return this;
        }

        RscAutoPlaceApiCall putXReplicasOnDifferent(String nodePropKey, Integer countRef)
        {
            xReplicasOnDifferentMap.put(nodePropKey, countRef);
            return this;
        }

        RscAutoPlaceApiCall addNode(String nodeNameRef)
        {
            nodeNameList.add(nodeNameRef);
            return this;
        }

        RscAutoPlaceApiCall addStorPool(String storPoolNameRef)
        {
            storPoolNameList.add(storPoolNameRef);
            return this;
        }

        RscAutoPlaceApiCall addStorPoolDiskless(String storPoolDisklessNameRef)
        {
            storPoolDisklessNameList.add(storPoolDisklessNameRef);
            return this;
        }

        RscAutoPlaceApiCall doNotPlaceWith(String... doNotPlaceWithRsc)
        {
            doNotPlaceWithRscList.addAll(Arrays.asList(doNotPlaceWithRsc));
            return this;
        }

        RscAutoPlaceApiCall disklessOnRemaining(boolean disklessOnRemainingRef)
        {
            disklessOnRemaining = disklessOnRemainingRef;
            return this;
        }

        RscAutoPlaceApiCall skipAlreadyPlacedOnNodeCheck(List<String> skipAlreadyPlacedOnNodeCheckRef)
        {
            skipAlreadyPlacedOnNodeCheck = skipAlreadyPlacedOnNodeCheckRef;
            return this;
        }

        RscAutoPlaceApiCall setDisklessType(String disklessTypeRef)
        {
            disklessType = disklessTypeRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
            throws Exception
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            rscAutoPlaceApiCallHandler.autoPlace(
                rscNameStr,
                new AutoSelectFilterApi()
                {
                    @Override
                    public List<String> getNodeNameList()
                    {
                        return nodeNameList;
                    }

                    @Override
                    public List<String> getStorPoolNameList()
                    {
                        return storPoolNameList;
                    }

                    @Override
                    public List<String> getStorPoolDisklessNameList()
                    {
                        return storPoolDisklessNameList;
                    }

                    @Override
                    public List<String> getReplicasOnSameList()
                    {
                        return replicasOnSameNodePropList;
                    }

                    @Override
                    public List<String> getReplicasOnDifferentList()
                    {
                        return replicasOnDifferentNodePropList;
                    }

                    @Override
                    public Map<String, Integer> getXReplicasOnDifferentMap()
                    {
                        return xReplicasOnDifferentMap;
                    }

                    @Override
                    public Integer getReplicaCount()
                    {
                        return placeCount;
                    }

                    @Override
                    public Integer getAdditionalReplicaCount()
                    {
                        return additionalPlaceCount;
                    }

                    @Override
                    public String getDoNotPlaceWithRscRegex()
                    {
                        return doNotPlaceWithRscRegexStr;
                    }

                    @Override
                    public List<String> getDoNotPlaceWithRscList()
                    {
                        return doNotPlaceWithRscList;
                    }

                    @Override
                    public List<DeviceLayerKind> getLayerStackList()
                    {
                        return layerStack;
                    }

                    @Override
                    public List<DeviceProviderKind> getProviderList()
                    {
                        return providerList;
                    }

                    @Override
                    public Boolean getDisklessOnRemaining()
                    {
                        return disklessOnRemaining;
                    }

                    @Override
                    public List<String> skipAlreadyPlacedOnNodeNamesCheck()
                    {
                        return skipAlreadyPlacedOnNodeCheck;
                    }

                    @Override
                    public Boolean skipAlreadyPlacedOnAllNodeCheck()
                    {
                        return skipAlreadyPlacedOnAllNodeCheck;
                    }

                    @Override
                    public String getDisklessType()
                    {
                        return disklessType;
                    }

                    @Override
                    public Map<ExtTools, Version> getRequiredExtTools()
                    {
                        return requiredExtTools;
                    }

                    @Override
                    public @Nullable Integer getDrbdPortCount()
                    {
                        return portCount;
                    }
                }
            ).contextWrite(contextWrite()).toStream().forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        SatelliteBuilder stltBuilder(String stltName) throws Exception
        {
            enterScope();

            Node stlt = nodeFactory.create(
                ApiTestBase.BOB_ACC_CTX,
                new NodeName(stltName),
                Node.Type.SATELLITE,
                null
            );

            nodesMap.put(stlt.getName(), stlt);
            StorPoolDefinition dfltDisklessStorPoolDfn =
                storPoolDefinitionRepository.get(SYS_CTX, DFLT_DISKLESS_STOR_POOL_NAME);
            if (dfltDisklessStorPoolDfn == null)
            {
                dfltDisklessStorPoolDfn = storPoolDefinitionFactory.create(
                    BOB_ACC_CTX,
                    DFLT_DISKLESS_STOR_POOL_NAME
                );
            }
            FreeSpaceMgr fsm = freeSpaceMgrFactory.getInstance(
                BOB_ACC_CTX,
                new SharedStorPoolName(stlt.getName(), DFLT_DISKLESS_STOR_POOL_NAME)
            );
            storPoolFactory.create(
                BOB_ACC_CTX,
                stlt,
                dfltDisklessStorPoolDfn,
                DeviceProviderKind.DISKLESS,
                fsm,
                false
            );

            commitAndCleanUp(true);

            return new SatelliteBuilder(this, stlt);
        }

        RscAutoPlaceApiCall addVlmDfn(String rscNameStrRef, int vlmNrRef, long sizeRef) throws Exception
        {
            enterScope();

            ResourceName rscName = new ResourceName(rscNameStrRef);
            ResourceDefinition rscDfn = rscDfnMap.get(rscName);

            volumeDefinitionFactory.create(
                ApiTestBase.BOB_ACC_CTX,
                rscDfn,
                new VolumeNumber(vlmNrRef),
                MINOR_GEN.incrementAndGet(),
                sizeRef,
                null
            );

            commitAndCleanUp(true);

            return this;
        }

        RscAutoPlaceApiCall setVlmDfnProp(String rscNameRef, int vlmNrRef, String propKeyRef, String propValRef)
            throws Exception
        {
            enterScope();
            rscDfnMap.get(new ResourceName(rscNameRef))
                .getVolumeDfn(SYS_CTX, new VolumeNumber(vlmNrRef))
                .getProps(SYS_CTX)
                .setProp(propKeyRef, propValRef);
            commitAndCleanUp(true);
            return this;
        }

        RscAutoPlaceApiCall setRscDfnProp(String rscNameRef, String propKeyRef, String propValRef)
            throws Exception
        {
            enterScope();
            rscDfnMap.get(new ResourceName(rscNameRef))
                .getProps(SYS_CTX)
                .setProp(propKeyRef, propValRef);
            commitAndCleanUp(true);
            return this;
        }

        RscAutoPlaceApiCall addRscDfn(String rscNameStrRef) throws Exception
        {
            enterScope();

            createRscDfn(rscNameStrRef);

            commitAndCleanUp(true);

            return this;
        }


        RscAutoPlaceApiCall addRsc(String rscNameStrRef, String storPool, String... stltNameStrs) throws Exception
        {
            enterScope();

            Map<String, String> rscPropsMap = new TreeMap<>();
            rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, storPool);
            for (String stltNameStr : stltNameStrs)
            {
                ctrlRscCrtApiHelper.createResourceDb(
                    stltNameStr,
                    rscNameStrRef,
                    0L,
                    rscPropsMap,
                    Collections.emptyList(),
                    null,
                    null,
                    portCount,
                    null,
                    Collections.emptyList(),
                    Resource.DiskfulBy.USER
                );
            }

            commitAndCleanUp(true);

            return this;
        }
    }

    private final class SatelliteBuilder
    {
        private final RscAutoPlaceApiCall parent;
        private final Node stlt;
        private final Peer mockedPeer;
        private final ExtToolsManager mockedExtToolsMgr;

        SatelliteBuilder(RscAutoPlaceApiCall parentRef, Node stltRef)
        {
            mockedPeer = Mockito.mock(Peer.class);
            mockedExtToolsMgr = Mockito.mock(ExtToolsManager.class);

            // Fail deployment of the new resources so that the API call handler doesn't wait for the resource to
            // be ready
            Mockito.when(mockedPeer.apiCall(anyString(), any()))
                .thenReturn(Flux.error(new RuntimeException("Deployment deliberately failed")));
            Mockito.when(mockedPeer.isOnline()).thenReturn(true);
            Mockito.when(mockedPeer.getConnectionStatus()).thenReturn(ConnectionStatus.ONLINE);
            Mockito.when(mockedPeer.getExtToolsManager()).thenReturn(mockedExtToolsMgr);

            try
            {
                stltRef.setPeer(GenericDbBase.SYS_CTX, mockedPeer);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }

            parent = parentRef;
            stlt = stltRef;

            setSupportedLayers(DeviceLayerKind.values());
            setSupportedProviders(DeviceProviderKind.values());
        }

        public SatelliteBuilder setNodeProp(String key, String value)
            throws Exception
        {
            enterScope();

            stlt.getProps(ApiTestBase.BOB_ACC_CTX).setProp(key, value);

            commitAndCleanUp(true);

            return this;
        }

        SatelliteBuilder addStorPool(String storPoolName, long storPoolSize)
            throws Exception
        {
            return addStorPool(storPoolName, null, storPoolSize, storPoolSize, LVM);
        }

        SatelliteBuilder addStorPool(String storPoolName, long storPoolSize, DeviceProviderKind provider)
            throws Exception
        {
            return addStorPool(storPoolName, null, storPoolSize, storPoolSize, provider);
        }

        SatelliteBuilder addStorPool(
            String storPoolName,
            String freeSpaceMgrName,
            long freeSpace,
            long totalCapacity,
            DeviceProviderKind providerKind
        )
            throws Exception
        {
            enterScope();

            StorPoolDefinition storPoolDfn = storPoolDefinitionRepository.get(
                ApiTestBase.BOB_ACC_CTX,
                new StorPoolName(storPoolName)
            );

            if (storPoolDfn == null)
            {
                storPoolDfn = storPoolDefinitionFactory.create(
                    ApiTestBase.BOB_ACC_CTX,
                    new StorPoolName(storPoolName)
                );

                storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
            }

            FreeSpaceMgr fsm = freeSpaceMgrFactory.getInstance(
                BOB_ACC_CTX,
                freeSpaceMgrName == null ?
                    new SharedStorPoolName(stlt.getName(), storPoolDfn.getName()) :
                    SharedStorPoolName.restoreName(freeSpaceMgrName)
            );

            StorPool storPool = storPoolFactory.create(
                ApiTestBase.BOB_ACC_CTX,
                stlt,
                storPoolDfn,
                providerKind,
                fsm,
                false
            );

            storPool.getFreeSpaceTracker().setCapacityInfo(GenericDbBase.SYS_CTX, freeSpace, totalCapacity);

            commitAndCleanUp(true);

            return this;
        }

        public SatelliteBuilder setStorPoolProp(
            String storPoolNameRef,
            String propKeyRef,
            String propKeyVal
        )
            throws Exception
        {
            enterScope();
            stlt.getStorPool(SYS_CTX, new StorPoolName(storPoolNameRef))
                .getProps(SYS_CTX)
                .setProp(propKeyRef, propKeyVal);
            commitAndCleanUp(true);
            return this;
        }

        public SatelliteBuilder setSupportedLayers(DeviceLayerKind... layers)
        {
            List<DeviceLayerKind> kinds = new ArrayList<>(Arrays.asList(DeviceLayerKind.values()));
            for (DeviceLayerKind supportedLayer : layers)
            {
                Mockito.when(mockedExtToolsMgr.isLayerSupported(supportedLayer)).thenReturn(true);
                kinds.remove(supportedLayer);
            }
            for (DeviceLayerKind unsupportedLayer : kinds)
            {
                Mockito.when(mockedExtToolsMgr.isLayerSupported(unsupportedLayer)).thenReturn(false);
            }
            Mockito.when(mockedExtToolsMgr.getSupportedLayers()).thenReturn(new TreeSet<>(Arrays.asList(layers)));
            return this;
        }

        public SatelliteBuilder setSupportedProviders(DeviceProviderKind... providers)
        {
            for (DeviceProviderKind supportedProvider : providers)
            {
                Mockito.when(mockedExtToolsMgr.isProviderSupported(supportedProvider)).thenReturn(true);
            }
            for (DeviceProviderKind unsupportedProviderLayer : providers)
            {
                Mockito.when(mockedExtToolsMgr.isProviderSupported(unsupportedProviderLayer)).thenReturn(false);
            }
            Mockito.when(mockedExtToolsMgr.getSupportedProviders()).thenReturn(new TreeSet<>(Arrays.asList(providers)));
            return this;
        }

        public SatelliteBuilder setExtToolSupported(
            ExtTools tool,
            boolean supported,
            @Nullable Integer majorVer,
            @Nullable Integer minorVer,
            @Nullable Integer patchVer,
            String... reasonsNotSupported
        )
        {
            Mockito.when(mockedExtToolsMgr.getExtToolInfo(tool)).thenReturn(
                new ExtToolsInfo(tool, supported, majorVer, minorVer, patchVer, Arrays.asList(reasonsNotSupported))
            );
            Version version;
            if (majorVer != null)
            {
                version = new Version(majorVer, minorVer, patchVer);
            }
            else
            {
                version = null;
            }
            Mockito.when(mockedExtToolsMgr.getVersion(tool)).thenReturn(version);
            return this;
        }

        public RscAutoPlaceApiCall build()
        {
            return parent;
        }
    }
}

package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.inject.testing.fieldbinder.Bind;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(JUnitParamsRunner.class)
public class BackupFindUsableNodesTest extends ApiTestBase
{
    private static final String DATA_POOL = "DfltStorPool";
    private static final String META_POOL = "meta";
    private static final String NODE_A = "nodeA";
    private static final String NODE_B = "nodeB";
    private static final String NODE_C = "nodeC";
    private static final String NODE_D = "nodeD";
    private static final String NODE_E = "nodeE";
    private static final String SNAP_NAME = "test-snap";
    @Inject
    private CtrlBackupCreateApiCallHandler backupCrtHandler;
    private boolean allExtTools = true;
    private boolean supportShipping = true;
    private ResourceDefinition rscDfn;
    private SnapshotDefinition snapDfn;
    private Node nodeA;
    private Node nodeB;
    private Node nodeC;
    private Node nodeD;
    private Node nodeE;
    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Override
    @Before
    public void setUp() throws Exception
    {
        System.err.println("before");
        super.setUp();
        backupCrtHandler = Mockito.spy(backupCrtHandler);
        Mockito.when(backupCrtHandler.hasNodeAllExtTools(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
            .thenAnswer(ignore -> allExtTools);
        Mockito.doAnswer(
            ignored -> supportShipping ? new ApiCallRcImpl() :
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                    "Shipping not supported"
                )
        ).when(backupCrtHandler).backupShippingSupported(Mockito.any());

        LayerPayload payload = new LayerPayload();
        payload.drbdRscDfn.tcpPort = 4242;
        rscDfn = resourceDefinitionFactory.create(
            PUBLIC_CTX,
            new ResourceName("test"),
            null,
            new ResourceDefinition.Flags[0],
            null,
            payload,
            createDefaultResourceGroup(PUBLIC_CTX)
        );
        snapDfn = snapshotDefinitionFactory.create(
            PUBLIC_CTX,
            rscDfn,
            new SnapshotName(SNAP_NAME),
            new SnapshotDefinition.Flags[0]
        );
        nodeA = makeNode(NODE_A);
        nodesMap.put(nodeA.getName(), nodeA);
        nodeB = makeNode(NODE_B);
        nodesMap.put(nodeB.getName(), nodeB);
        nodeC = makeNode(NODE_C);
        nodesMap.put(nodeC.getName(), nodeC);
        nodeD = makeNode(NODE_D);
        nodeE = makeNode(NODE_E);
        StorPoolDefinition dataDfn = storPoolDefinitionFactory.create(PUBLIC_CTX, new StorPoolName(DATA_POOL));
        StorPoolDefinition metaDfn = storPoolDefinitionFactory.create(PUBLIC_CTX, new StorPoolName(META_POOL));
        for (Node node : Arrays.asList(nodeA, nodeB, nodeC, nodeD, nodeE))
        {
            FreeSpaceMgr dataFST = freeSpaceMgrFactory.getInstance(
                PUBLIC_CTX,
                new SharedStorPoolName(node.getName(), new StorPoolName(DATA_POOL))
            );
            FreeSpaceMgr metaFST = freeSpaceMgrFactory.getInstance(
                PUBLIC_CTX,
                new SharedStorPoolName(node.getName(), new StorPoolName(META_POOL))
            );
            storPoolFactory.create(PUBLIC_CTX, node, dataDfn, DeviceProviderKind.LVM_THIN, dataFST, false);
            storPoolFactory.create(PUBLIC_CTX, node, metaDfn, DeviceProviderKind.LVM_THIN, metaFST, false);
        }
    }

    @Test
    public void testStorageOnly() throws Exception
    {
        VolumeDefinition vlmDfn = makeVlmDfn();
        makeSnapVlmDfn(vlmDfn);
        singleStorageRsc(nodeA);
        Map<String, Node> usableNodes = backupCrtHandler.findUsableNodes(rscDfn, snapDfn, new HashMap<>());
        assertMap(usableNodes, NODE_A);
        singleStorageRsc(nodeB);
        try
        {
            usableNodes = backupCrtHandler.findUsableNodes(rscDfn, snapDfn, new HashMap<>());
            fail();
        }
        catch (ImplementationError expected)
        {
            // findUsableNodes should throw an error because creating a backup for a storage-only rsc with several
            // replicas is not supported
        }
    }

    @Test
    public void testDrbdFullInc() throws Exception
    {
        VolumeDefinition vlmDfn = makeVlmDfn();
        makeSnapVlmDfn(vlmDfn);
        VolumeDefinition vlmDfn2 = makeVlmDfn();
        makeSnapVlmDfn(vlmDfn2);
        setExtMeta(vlmDfn2.getVolumeNumber().value, true);
        singleDrbdRsc(nodeA);
        singleDrbdRsc(nodeB);
        setExtMeta(vlmDfn2.getVolumeNumber().value, false);
        setExtMeta(vlmDfn.getVolumeNumber().value, true);
        singleDrbdRsc(nodeC);
        Map<String, Node> usableNodes = backupCrtHandler.findUsableNodes(rscDfn, null, new HashMap<>());
        assertMap(usableNodes, NODE_A, NODE_B);
        singleDrbdRsc(nodeD, false);
        singleDrbdRsc(nodeE, false);
        usableNodes = backupCrtHandler.findUsableNodes(rscDfn, snapDfn, new HashMap<>());
        assertMap(usableNodes, NODE_A, NODE_B);
    }

    @Test
    @Parameters(method = "createInput")
    public void test(Pair<Map<String, Map<Integer, Boolean>>, String[]> input) throws Exception
    {
        System.err.println("test");
        boolean firstNode = true;
        for (Entry<String, Map<Integer, Boolean>> nodeData : input.objA.entrySet())
        {
            for (Entry<Integer, Boolean> vlmData : nodeData.getValue().entrySet())
            {
                if (firstNode)
                {
                    VolumeDefinition vlmDfn = makeVlmDfn();
                    makeSnapVlmDfn(vlmDfn);
                }
                setExtMeta(vlmData.getKey(), vlmData.getValue());
            }
            singleDrbdRsc(nodesMap.get(new NodeName(nodeData.getKey())));
            firstNode = false;
        }
        assertEquals(input.objA.values().iterator().next().size(), rscDfn.getVolumeDfnCount(PUBLIC_CTX));
        Map<String, Node> usableNodes = backupCrtHandler.findUsableNodes(rscDfn, snapDfn, new HashMap<>());
        assertMap(usableNodes, input.objB);
    }

    @SuppressWarnings("unused")
    private List<Pair<Map<String, Map<Integer, Boolean>>, String[]>> createInput()
    {
        return new InputBuilder()
            .addMap(NODE_A, false)
            .addMap(NODE_B, false)
            .addMap(NODE_C, false)
            .addToList(NODE_A, NODE_B, NODE_C)
            .nextEntry()
            .addMap(NODE_A, true)
            .addMap(NODE_B, true)
            .addMap(NODE_C, true)
            .addToList(NODE_A, NODE_B, NODE_C)
            .nextEntry()
            .addMap(NODE_A, false)
            .addMap(NODE_B, false)
            .addMap(NODE_C, true)
            .addToList(NODE_A, NODE_B)
            .nextEntry()
            .addMap(NODE_A, true)
            .addMap(NODE_B, true)
            .addMap(NODE_C, false)
            .addToList(NODE_A, NODE_B)
            .nextEntry()
            .addMap(NODE_A, false, false)
            .addMap(NODE_B, false, false)
            .addMap(NODE_C, false, false)
            .addToList(NODE_A, NODE_B, NODE_C)
            .nextEntry()
            .addMap(NODE_A, true, true)
            .addMap(NODE_B, true, true)
            .addMap(NODE_C, true, true)
            .addToList(NODE_A, NODE_B, NODE_C)
            .nextEntry()
            .addMap(NODE_A, false, false)
            .addMap(NODE_B, false, false)
            .addMap(NODE_C, true, true)
            .addToList(NODE_A, NODE_B)
            .nextEntry()
            .addMap(NODE_A, true, true)
            .addMap(NODE_B, true, true)
            .addMap(NODE_C, false, false)
            .addToList(NODE_A, NODE_B)
            .nextEntry()
            .addMap(NODE_A, false, true)
            .addMap(NODE_B, false, true)
            .addMap(NODE_C, true, false)
            .addToList(NODE_A, NODE_B)
            .nextEntry()
            .addMap(NODE_A, true, false)
            .addMap(NODE_B, true, false)
            .addMap(NODE_C, false, true)
            .addToList(NODE_A, NODE_B)
            .nextEntry()
            .getInput();
    }

    private void assertMap(Map<String, Node> map, String... nodes)
    {
        String errMsg = "expected: " + Arrays.asList(nodes) + ", actual: " + map.values();
        assertEquals(errMsg, nodes.length, map.size());
        for (String node : nodes)
        {
            assertTrue(errMsg, map.containsKey(node));
        }
    }

    private void setExtMeta(int vlmNr, boolean set) throws Exception
    {
        VolumeDefinition volumeDfn = rscDfn.getVolumeDfn(PUBLIC_CTX, new VolumeNumber(vlmNr));
        if (set)
        {
            volumeDfn.getProps(PUBLIC_CTX)
                .setProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME, META_POOL);
        }
        else
        {
            volumeDfn.getProps(PUBLIC_CTX)
                .removeProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME);
        }
    }

    private void singleDrbdRsc(Node node) throws Exception
    {
        singleDrbdRsc(node, true);
    }

    private void singleDrbdRsc(Node node, boolean makeSnap) throws Exception
    {
        List<DeviceLayerKind> layerStack = Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE);
        singleRsc(node, layerStack, makeSnap);
    }

    private void singleStorageRsc(Node node) throws Exception
    {
        List<DeviceLayerKind> layerStack = Arrays.asList(DeviceLayerKind.STORAGE);
        singleRsc(node, layerStack, true);
    }

    private void singleRsc(Node node, List<DeviceLayerKind> layerStack, boolean makeSnap) throws Exception
    {
        Resource rsc = makeRsc(node, layerStack);
        List<VolumeDefinition> vlmDfns = rscDfn.streamVolumeDfn(PUBLIC_CTX).collect(Collectors.toList());
        for (VolumeDefinition vlmDfn : vlmDfns)
        {
            makeVlm(rsc, vlmDfn);
        }

        if (makeSnap)
        {
            Snapshot snap = makeSnap(rsc);
            for (VolumeDefinition vlmDfn : vlmDfns)
            {
                SnapshotVolumeDefinition snapVlmDfn = rscDfn.getSnapshotDfn(PUBLIC_CTX, new SnapshotName(SNAP_NAME))
                    .getSnapshotVolumeDefinition(PUBLIC_CTX, vlmDfn.getVolumeNumber());
                makeSnapVlm(rsc, snap, snapVlmDfn);
            }
        }
    }

    private VolumeDefinition makeVlmDfn() throws Exception
    {
        int vlmNr = rscDfn.getVolumeDfnCount(PUBLIC_CTX);
        return volumeDefinitionFactory.create(
            PUBLIC_CTX,
            rscDfn,
            new VolumeNumber(vlmNr),
            1000 + vlmNr,
            42L,
            new VolumeDefinition.Flags[0]
        );
    }

    private SnapshotVolumeDefinition makeSnapVlmDfn(VolumeDefinition vlmDfn) throws Exception
    {
        return snapshotVolumeDefinitionFactory.create(
            PUBLIC_CTX,
            snapDfn,
            vlmDfn,
            vlmDfn.getVolumeSize(PUBLIC_CTX),
            new SnapshotVolumeDefinition.Flags[0]
        );
    }

    private Resource makeRsc(Node node, List<DeviceLayerKind> layerStack) throws Exception
    {
        return resourceFactory.create(PUBLIC_CTX, rscDfn, node, null, new Resource.Flags[0], layerStack);
    }

    private Snapshot makeSnap(Resource rsc) throws Exception
    {
        return snapshotFactory.create(PUBLIC_CTX, rsc, snapDfn, new Snapshot.Flags[0]);
    }

    private Volume makeVlm(Resource rsc, VolumeDefinition vlmDfn) throws Exception
    {
        return volumeFactory.create(PUBLIC_CTX, rsc, vlmDfn, new Volume.Flags[0], new LayerPayload(), null);
    }

    private SnapshotVolume makeSnapVlm(Resource rsc, Snapshot snap, SnapshotVolumeDefinition snapVlmDfn)
        throws Exception
    {
        return snapshotVolumeFactory.create(PUBLIC_CTX, rsc, snap, snapVlmDfn);
    }

    private Node makeNode(String name) throws Exception
    {
        return nodeFactory.create(PUBLIC_CTX, new NodeName(name), Node.Type.SATELLITE, new Node.Flags[0]);
    }

    private class InputBuilder
    {
        private List<Pair<Map<String, Map<Integer, Boolean>>, String[]>> input;
        private Map<String, Map<Integer, Boolean>> map;

        InputBuilder()
        {
            input = new ArrayList<>();
            map = new HashMap<>();
        }

        public InputBuilder addMap(String nodeName, boolean... metaTypes)
        {
            Map<Integer, Boolean> tmp = new HashMap<>();
            for (int idx = 0; idx < metaTypes.length; idx++)
            {
                tmp.put(idx, metaTypes[idx]);
            }
            map.put(nodeName, tmp);
            return this;
        }

        public InputBuilder addToList(String... expectedNodes)
        {
            input.add(new Pair<>(map, expectedNodes));
            return this;
        }

        public InputBuilder nextEntry()
        {
            map = new HashMap<>();
            return this;
        }

        public List<Pair<Map<String, Map<Integer, Boolean>>, String[]>> getInput()
        {
            return input;
        }
    }
}

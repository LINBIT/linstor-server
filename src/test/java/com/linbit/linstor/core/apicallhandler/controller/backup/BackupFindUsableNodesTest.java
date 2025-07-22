package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiConsts.ConnectionStatus;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.backup.nodefinder.BackupNodeFinder;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
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
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.inject.testing.fieldbinder.Bind;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

@RunWith(JUnitParamsRunner.class)
@PrepareForTest(CtrlBackupCreateApiCallHandler.class)
public class BackupFindUsableNodesTest extends ApiTestBase
{
    private static final String DATA_POOL_LVM_THIN = "DfltStorPool";
    private static final String META_POOL_LVM_THIN = "meta";
    private static final String DATA_POOL_ZFS = "zfsData";
    private static final String META_POOL_ZFS = "zfsMeta";
    private static final String NODE_A = "nodeA";
    private static final String NODE_B = "nodeB";
    private static final String NODE_C = "nodeC";
    private static final String NODE_D = "nodeD";
    private static final String NODE_E = "nodeE";
    private static final String SNAP_NAME = "test-snap";
    private static final String REMOTE_NAME = "dummy-remote";
    @Inject
    private BackupNodeFinder backupNodeFinder;
    private boolean supportShipping = true;
    private ResourceDefinition rscDfn;
    private SnapshotDefinition snapDfn;
    private AbsRemote mockedRemote;
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
        backupNodeFinder = Mockito.spy(backupNodeFinder);
        Mockito.doAnswer(
            ignored -> supportShipping ? new ApiCallRcImpl() :
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                    "Shipping not supported"
                )
        ).when(backupNodeFinder).backupShippingSupported(Mockito.any());
        mockedRemote = Mockito.mock(AbsRemote.class);
        Mockito.when(mockedRemote.getName()).thenReturn(new RemoteName(REMOTE_NAME));
        Mockito.when(mockedRemote.getType()).thenReturn(RemoteType.S3);

        LayerPayload payload = new LayerPayload();
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
        nodesMap.put(nodeD.getName(), nodeD);
        nodeE = makeNode(NODE_E);
        nodesMap.put(nodeE.getName(), nodeE);
        StorPoolDefinition dataLvmThinDfn = storPoolDefinitionFactory.create(
            PUBLIC_CTX,
            new StorPoolName(DATA_POOL_LVM_THIN)
        );
        StorPoolDefinition metaLvmThinDfn = storPoolDefinitionFactory.create(
            PUBLIC_CTX,
            new StorPoolName(META_POOL_LVM_THIN)
        );
        StorPoolDefinition dataZfsDfn = storPoolDefinitionFactory.create(PUBLIC_CTX, new StorPoolName(DATA_POOL_ZFS));
        StorPoolDefinition metaZfsDfn = storPoolDefinitionFactory.create(PUBLIC_CTX, new StorPoolName(META_POOL_ZFS));
        for (Node node : Arrays.asList(nodeA, nodeB, nodeC, nodeD, nodeE))
        {
            FreeSpaceMgr dataLvmThinFST = freeSpaceMgrFactory.getInstance(
                PUBLIC_CTX,
                new SharedStorPoolName(node.getName(), new StorPoolName(DATA_POOL_LVM_THIN))
            );
            FreeSpaceMgr metaLvmThinFST = freeSpaceMgrFactory.getInstance(
                PUBLIC_CTX,
                new SharedStorPoolName(node.getName(), new StorPoolName(META_POOL_LVM_THIN))
            );
            FreeSpaceMgr dataZfsFST = freeSpaceMgrFactory.getInstance(
                PUBLIC_CTX,
                new SharedStorPoolName(node.getName(), new StorPoolName(DATA_POOL_ZFS))
            );
            FreeSpaceMgr metaZfsFST = freeSpaceMgrFactory.getInstance(
                PUBLIC_CTX,
                new SharedStorPoolName(node.getName(), new StorPoolName(META_POOL_ZFS))
            );
            storPoolFactory.create(
                PUBLIC_CTX,
                node,
                dataLvmThinDfn,
                DeviceProviderKind.LVM_THIN,
                dataLvmThinFST,
                false
            );
            storPoolFactory.create(
                PUBLIC_CTX,
                node,
                metaLvmThinDfn,
                DeviceProviderKind.LVM_THIN,
                metaLvmThinFST,
                false
            );
            storPoolFactory.create(PUBLIC_CTX, node, dataZfsDfn, DeviceProviderKind.ZFS, dataZfsFST, false);
            storPoolFactory.create(PUBLIC_CTX, node, metaZfsDfn, DeviceProviderKind.ZFS, metaZfsFST, false);
        }
    }

    @Test
    public void testStorageOnly() throws Exception
    {
        /*
         * create storage-only rsc on A
         * make sure A gets chosen for shipping
         * add second replica on B
         * make sure shipping is no longer allowed
         */
        VolumeDefinition vlmDfn = makeVlmDfn();
        makeSnapVlmDfn(vlmDfn);
        singleStorageRsc(nodeA);
        Set<Node> usableNodes = backupNodeFinder.findUsableNodes(
            rscDfn,
            snapDfn,
            mockedRemote
        );
        assertSet(usableNodes, NODE_A);
        singleStorageRsc(nodeB);
        try
        {
            usableNodes = backupNodeFinder.findUsableNodes(
                rscDfn,
                snapDfn,
                mockedRemote
            );
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
        /*
         * group 1 (A, B): vlm 0 int meta, vlm 1 ext meta
         * group 2 (C) : vlm 0 ext meta, vlm 1 int meta
         * group 1 gets chosen for full backup (because more nodes)
         * add D & E to group 2
         * make sure inc still chooses group 1 despite group 2 being bigger now
         */
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
        Set<Node> usableNodes = backupNodeFinder.findUsableNodes(
            rscDfn,
            null,
            mockedRemote
        );
        assertSet(usableNodes, NODE_A, NODE_B);
        singleDrbdRsc(nodeD, false);
        singleDrbdRsc(nodeE, false);
        snapDfn.getSnapDfnProps(SYS_CTX)
            .setProp(
                InternalApiConsts.KEY_BACKUP_SRC_NODE + "/" + REMOTE_NAME,
                nodeA.getName().displayValue,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
        usableNodes = backupNodeFinder.findUsableNodes(rscDfn, snapDfn, mockedRemote);
        assertSet(usableNodes, NODE_A, NODE_B);
    }

    @Test
    public void testChooseAny() throws Exception
    {
        /*
         * A: int, ext
         * B: int, int
         * C: ext, int
         * every node different, make sure all groups are returned
         */
        VolumeDefinition vlmDfn = makeVlmDfn();
        makeSnapVlmDfn(vlmDfn);
        VolumeDefinition vlmDfn2 = makeVlmDfn();
        makeSnapVlmDfn(vlmDfn2);
        setExtMeta(vlmDfn2.getVolumeNumber().value, true);
        singleDrbdRsc(nodeA);
        setExtMeta(vlmDfn2.getVolumeNumber().value, false);
        singleDrbdRsc(nodeB);
        setExtMeta(vlmDfn.getVolumeNumber().value, true);
        singleDrbdRsc(nodeC);
        Set<Node> usableNodes = backupNodeFinder.findUsableNodes(
            rscDfn,
            null,
            mockedRemote
        );
        assertEquals(3, usableNodes.size());
    }

    @Test
    @Parameters(method = "createInput")
    public void test(Input input) throws Exception
    {
        System.err.println("test");
        boolean firstNode = true;
        boolean inc = false;

        for (InputNode inputNode : input.nodes.values())
        {
            for (Entry<Integer, Boolean> vlmNrEntry : inputNode.vlmNrToInternalMDMap.entrySet())
            {
                if (firstNode)
                {
                    VolumeDefinition vlmDfn = makeVlmDfn();
                    makeSnapVlmDfn(vlmDfn);
                }
                Integer vlmNr = vlmNrEntry.getKey();
                setExtMeta(vlmNr, vlmNrEntry.getValue());
                setStorPool(vlmNr, inputNode.vlmNrToKind.get(vlmNr));
            }
            Node node = nodesMap.get(new NodeName(inputNode.nodeName));
            singleDrbdRsc(node);
            /*
             * selectedLast determines that the node it is set on was the source node of the last shipping (which we
             * pretend happened)
             */
            if (inputNode.selectedLast)
            {
                inc = true;
                snapDfn.getSnapDfnProps(SYS_CTX)
                    .setProp(
                        InternalApiConsts.KEY_BACKUP_SRC_NODE + "/" + REMOTE_NAME,
                        inputNode.nodeName,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
            }
            firstNode = false;
        }

        assertEquals(
            input.nodes.values().iterator().next().vlmNrToInternalMDMap.size(),
            rscDfn.getVolumeDfnCount(PUBLIC_CTX)
        );
        Set<Node> usableNodes = backupNodeFinder.findUsableNodes(
            rscDfn,
            inc ? snapDfn : null,
            mockedRemote
        );
        // assertMap(usableNodes, input.objB);

        assertSet(usableNodes, input.expectedSelectedNodes);
    }

    @SuppressWarnings("unused")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private List<Input> createInput()
    {
        return new InputBuilder()
            // if all rscs have the same meta-types, choose all nodes
                .addMap(NODE_A, false)
                .addMap(NODE_B, false)
                .addMap(NODE_C, false)
            .closeInputWithExpectedNodes(NODE_A, NODE_B, NODE_C)
                .addMap(NODE_A, true)
                .addMap(NODE_B, true)
                .addMap(NODE_C, true)
            .closeInputWithExpectedNodes(NODE_A, NODE_B, NODE_C)
            // if rscs have different meta-types, choose biggest group
                .addMap(NODE_A, false)
                .addMap(NODE_B, false)
                .addMap(NODE_C, true)
            .closeInputWithExpectedNodes(NODE_A, NODE_B)
                .addMap(NODE_A, true)
                .addMap(NODE_B, true)
                .addMap(NODE_C, false)
            .closeInputWithExpectedNodes(NODE_A, NODE_B)
            // if all rscs have the same meta-types, choose all nodes
                .addMap(NODE_A, false, false)
                .addMap(NODE_B, false, false)
                .addMap(NODE_C, false, false)
            .closeInputWithExpectedNodes(NODE_A, NODE_B, NODE_C)
                .addMap(NODE_A, true, true)
                .addMap(NODE_B, true, true)
                .addMap(NODE_C, true, true)
            .closeInputWithExpectedNodes(NODE_A, NODE_B, NODE_C)
            // if rscs have different meta-types, choose biggest group
                .addMap(NODE_A, false, false)
                .addMap(NODE_B, false, false)
                .addMap(NODE_C, true, true)
            .closeInputWithExpectedNodes(NODE_A, NODE_B)
                .addMap(NODE_A, true, true)
                .addMap(NODE_B, true, true)
                .addMap(NODE_C, false, false)
            .closeInputWithExpectedNodes(NODE_A, NODE_B)
                .addMap(NODE_A, false, true)
                .addMap(NODE_B, false, true)
                .addMap(NODE_C, true, false)
            .closeInputWithExpectedNodes(NODE_A, NODE_B)
                .addMap(NODE_A, true, false)
                .addMap(NODE_B, true, false)
                .addMap(NODE_C, false, true)
            .closeInputWithExpectedNodes(NODE_A, NODE_B)
            // if rscs have different meta-types but one node is marked as selectedLast, choose that node's group
            // regardless of size
                .addNode(NODE_A)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.LVM_THIN)
                    .build()
                .addNode(NODE_B)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.LVM_THIN)
                    .build()
                .addNode(NODE_C)
                    .withInternalMd(false)
                    .withKinds(DeviceProviderKind.LVM_THIN)
                    .wasSelectedLast(true)
                    .build()
            .closeInputWithExpectedNodes(NODE_C)
                .addNode(NODE_A)
                    .withInternalMd(true, false)
                    .withKinds(DeviceProviderKind.LVM_THIN, DeviceProviderKind.LVM_THIN)
                    .build()
                .addNode(NODE_B)
                    .withInternalMd(true, false)
                    .withKinds(DeviceProviderKind.LVM_THIN, DeviceProviderKind.LVM_THIN)
                    .build()
                .addNode(NODE_C)
                    .withInternalMd(false, true)
                    .withKinds(DeviceProviderKind.LVM_THIN, DeviceProviderKind.LVM_THIN)
                    .wasSelectedLast(true)
                    .build()
                .addNode(NODE_D)
                    .withInternalMd(false, true)
                    .withKinds(DeviceProviderKind.LVM_THIN, DeviceProviderKind.LVM_THIN)
                    .build()
                .addNode(NODE_E)
                    .withInternalMd(true, true)
                    .withKinds(DeviceProviderKind.LVM_THIN, DeviceProviderKind.LVM_THIN)
                    .build()
            .closeInputWithExpectedNodes(NODE_C, NODE_D)
                .addNode(NODE_A)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.LVM_THIN)
                    .build()
                .addNode(NODE_B)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.LVM_THIN)
                    .build()
                .addNode(NODE_C)
                    .withInternalMd(false)
                    .withKinds(DeviceProviderKind.LVM_THIN)
                    .wasSelectedLast(true)
                    .build()
                .addNode(NODE_D)
                    .withInternalMd(false)
                    .withKinds(DeviceProviderKind.LVM_THIN)
                    .build()
                .addNode(NODE_E)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.LVM_THIN)
                    .build()
            .closeInputWithExpectedNodes(NODE_C, NODE_D)
            // for zfs choose all nodes since every group is only 1 node big
                .addNode(NODE_A)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.ZFS)
                    .build()
                .addNode(NODE_B)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.ZFS)
                    .build()
                .addNode(NODE_C)
                    .withInternalMd(false)
                    .withKinds(DeviceProviderKind.ZFS)
                    .build()
            .closeInputWithExpectedNodes(NODE_A, NODE_B, NODE_C)
            // if zfs has a previous shipping, only choose the node that did it, since no other node can continue
                .addNode(NODE_A)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.ZFS)
                    .build()
                .addNode(NODE_B)
                    .withInternalMd(true)
                    .withKinds(DeviceProviderKind.ZFS)
                    .build()
                .addNode(NODE_C)
                    .withInternalMd(false)
                    .withKinds(DeviceProviderKind.ZFS)
                    .wasSelectedLast(true)
                    .build()
            .closeInputWithExpectedNodes(NODE_C)
            .build();
    }

    private void assertSet(Set<Node> set, String... nodes)
    {
        String errMsg = "expected: " + Arrays.asList(nodes) + ", actual: " + set;
        assertEquals(errMsg, nodes.length, set.size());
        List<String> names = set.stream().map(node -> node.getName().displayValue).collect(Collectors.toList());
        for (String node : nodes)
        {
            assertTrue(errMsg, names.contains(node));
        }
    }

    private void setStorPool(int vlmNr, DeviceProviderKind kindRef) throws Exception
    {
        VolumeDefinition volumeDfn = rscDfn.getVolumeDfn(PUBLIC_CTX, new VolumeNumber(vlmNr));
        switch (kindRef)
        {
            case LVM_THIN:
                volumeDfn.getProps(PUBLIC_CTX)
                    .setProp(ApiConsts.KEY_STOR_POOL_NAME, DATA_POOL_LVM_THIN);
                break;
            case ZFS:
                volumeDfn.getProps(PUBLIC_CTX)
                    .setProp(ApiConsts.KEY_STOR_POOL_NAME, DATA_POOL_ZFS);
                break;
            case DISKLESS:
            case EBS_INIT:
            case EBS_TARGET:
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            case FILE:
            case FILE_THIN:
            case LVM:
            case REMOTE_SPDK:
            case SPDK:
            case ZFS_THIN:
            case STORAGE_SPACES:
            case STORAGE_SPACES_THIN:
            default:
                throw new ImplementationError("not implemented in tests");
        }
    }

    private void setExtMeta(int vlmNr, boolean set) throws Exception
    {
        VolumeDefinition volumeDfn = rscDfn.getVolumeDfn(PUBLIC_CTX, new VolumeNumber(vlmNr));
        if (set)
        {
            volumeDfn.getProps(PUBLIC_CTX)
                .setProp(ApiConsts.KEY_STOR_POOL_DRBD_META_NAME, META_POOL_LVM_THIN);
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
        return volumeFactory.create(
            PUBLIC_CTX,
            rsc,
            vlmDfn,
            new Volume.Flags[0],
            new LayerPayload(),
            null,
            Collections.emptyMap(),
            null
        );
    }

    private SnapshotVolume makeSnapVlm(Resource rsc, Snapshot snap, SnapshotVolumeDefinition snapVlmDfn)
        throws Exception
    {
        return snapshotVolumeFactory.create(PUBLIC_CTX, rsc, snap, snapVlmDfn);
    }

    private Node makeNode(String name) throws Exception
    {
        Node node = nodeFactory.create(PUBLIC_CTX, new NodeName(name), Node.Type.SATELLITE, new Node.Flags[0]);
        Peer mockedPeer = Mockito.mock(Peer.class);
        ExtToolsManager extToolsMgr = new ExtToolsManager();
        extToolsMgr.updateExternalToolsInfo(
            Arrays.asList(new ExtToolsInfo(ExtTools.ZSTD, true, 1, 5, 2, Collections.emptyList()))
        );
        // Fail deployment of the new resources so that the API call handler doesn't wait for the resource to be ready
        Mockito.when(mockedPeer.apiCall(anyString(), any()))
            .thenReturn(Flux.error(new RuntimeException("Deployment deliberately failed")));
        Mockito.when(mockedPeer.isOnline()).thenReturn(true);
        Mockito.when(mockedPeer.getConnectionStatus()).thenReturn(ConnectionStatus.ONLINE);
        Mockito.when(mockedPeer.getExtToolsManager()).thenReturn(extToolsMgr);

        try
        {
            node.setPeer(GenericDbBase.SYS_CTX, mockedPeer);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return node;
    }

    private class InputBuilder
    {
        private List<Input> inputList;
        Map<String, InputNode> currentNodes = new HashMap<>();

        InputBuilder()
        {
            inputList = new ArrayList<>();
        }

        public InputBuilder addMap(String nodeName, boolean... metaTypes)
        {
            return addNode(nodeName)
                .withInternalMd(metaTypes)
                .withKinds(repeatKind(DeviceProviderKind.LVM_THIN, metaTypes.length))
                .build();
            // Map<Integer, Boolean> tmp = new HashMap<>();
            // for (int idx = 0; idx < metaTypes.length; idx++)
            // {
            // tmp.put(idx, metaTypes[idx]);
            // }
            // map.put(nodeName, tmp);
            // return this;
        }

        private DeviceProviderKind[] repeatKind(DeviceProviderKind kindRef, int lengthRef)
        {
            DeviceProviderKind[] ret = new DeviceProviderKind[lengthRef];
            for (int idx = 0; idx < lengthRef; idx++)
            {
                ret[idx] = kindRef;
            }
            return ret;
        }

        public InputNodeBuilder addNode(String nodeName)
        {
            return new InputNodeBuilder(this, nodeName);
        }

        public InputBuilder closeInputWithExpectedNodes(String... expectedNodeNamesRef)
        {
            inputList.add(new Input(currentNodes, expectedNodeNamesRef));
            currentNodes = new HashMap<>();
            return this;
        }

        public List<Input> build()
        {
            return inputList;
        }
    }

    private class InputNodeBuilder
    {
        private final InputBuilder inputBuilder;

        private String nodeName;
        private Map<Integer, Boolean> intMdMap = new HashMap<>();
        private Map<Integer, DeviceProviderKind> kindMap = new HashMap<>();
        private boolean selectedLast = false;

        InputNodeBuilder(InputBuilder inputBuilderRef, String nodeNameRef)
        {
            inputBuilder = inputBuilderRef;
            nodeName = nodeNameRef;
        }

        public InputNodeBuilder withInternalMd(boolean... intMds)
        {
            if (!kindMap.isEmpty() && intMds.length != kindMap.size())
            {
                throw new ImplementationError("metadata map and kind map must have equal size");
            }
            for (int vlmNr = 0; vlmNr < intMds.length; vlmNr++)
            {
                intMdMap.put(vlmNr, intMds[vlmNr]);
            }
            return this;
        }

        public InputNodeBuilder withKinds(DeviceProviderKind... kinds)
        {
            if (!intMdMap.isEmpty() && kinds.length != intMdMap.size())
            {
                throw new ImplementationError("metadata map and kind map must have equal size");
            }
            for (int vlmNr = 0; vlmNr < kinds.length; vlmNr++)
            {
                kindMap.put(vlmNr, kinds[vlmNr]);
            }
            return this;
        }

        public InputNodeBuilder wasSelectedLast(boolean selectedLastRef)
        {
            selectedLast = selectedLastRef;
            return this;
        }

        public InputBuilder build()
        {
            inputBuilder.currentNodes.put(nodeName, new InputNode(nodeName, intMdMap, kindMap, selectedLast));
            return inputBuilder;
        }
    }

    private class Input
    {
        Map<String, InputNode> nodes;
        String[] expectedSelectedNodes;

        Input(Map<String, InputNode> nodesRef, String[] expectedSelectedNodesRef)
        {
            nodes = nodesRef;
            expectedSelectedNodes = expectedSelectedNodesRef;
        }
    }

    private class InputNode
    {
        String nodeName;
        Map<Integer, Boolean> vlmNrToInternalMDMap;
        Map<Integer, DeviceProviderKind> vlmNrToKind;
        boolean selectedLast;

        InputNode(
            String nodeNameRef,
            Map<Integer, Boolean> vlmNrToInternalMDMapRef,
            Map<Integer, DeviceProviderKind> vlmNrToKindRef,
            boolean selectedLastRef
        )
        {
            nodeName = nodeNameRef;
            vlmNrToInternalMDMap = vlmNrToInternalMDMapRef;
            vlmNrToKind = vlmNrToKindRef;
            selectedLast = selectedLastRef;
        }

    }
}

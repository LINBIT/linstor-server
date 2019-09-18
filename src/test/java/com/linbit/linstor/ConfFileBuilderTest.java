package com.linbit.linstor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceConnectionData;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteDrbdLayerDriver;
import com.linbit.linstor.dbdrivers.satellite.SatellitePropDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteStorageLayerDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.DummySecurityInitializer;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.ConfFileBuilder;
import com.linbit.linstor.testutils.EmptyErrorReporter;
import com.linbit.linstor.transaction.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@SuppressWarnings("checkstyle:magicnumber")
public class ConfFileBuilderTest
{
    private static final DrbdLayerDatabaseDriver DRBD_LAYER_NO_OP_DRIVER = new SatelliteDrbdLayerDriver();
    private static final StorageLayerDatabaseDriver STORAGE_LAYER_NO_OP_DRIVER = new SatelliteStorageLayerDriver();

    private ErrorReporter errorReporter;
    private AccessContext accessContext;

    private ObjectProtection dummyObjectProtection;
    private WhitelistProps whitelistProps;

    private DrbdRscData localRscData, peerRscData;

    private ConfFileBuilder confFileBuilder;
    private ResourceConnection rscConn;
    private NodeConnection nodeConn;
    private PropsContainer props;
    private Provider<TransactionMgr> transMgrProvider;
    private TransactionObjectFactory transObjFactory;

    private AtomicInteger idGenerator = new AtomicInteger(0);
    private DynamicNumberPool mockedTcpPool;
    private DynamicNumberPool mockedMinorPool;

    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);

        errorReporter = new EmptyErrorReporter();
        accessContext = DummySecurityInitializer.getSystemAccessContext();

        dummyObjectProtection = DummySecurityInitializer.getDummyObjectProtection(accessContext);

        whitelistProps = new WhitelistProps(errorReporter);
        // just let it empty - we do not want test drbd-options anyways here

        TransactionMgr dummyTransMgr = new SatelliteTransactionMgr();
        transMgrProvider = () -> dummyTransMgr;
        transObjFactory = new TransactionObjectFactory(transMgrProvider);
        props = new PropsContainerFactory(
                new SatellitePropDriver(), transMgrProvider)
            .getInstance("TESTINSTANCE");

        mockedTcpPool = Mockito.mock(DynamicNumberPool.class);
        mockedMinorPool = Mockito.mock(DynamicNumberPool.class);

        when(mockedTcpPool.autoAllocate()).thenReturn(9001);
        when(mockedMinorPool.autoAllocate()).thenReturn(99);

        localRscData = makeMockResource(101, "alpha", "1.2.3.4", false, false, false);
        peerRscData = makeMockResource(202, "bravo", "5.6.7.8", false, false, false);
        when(localRscData.getResource().getAssignedNode().getNodeConnection(
                accessContext, peerRscData.getResource().getAssignedNode()))
            .thenReturn(nodeConn);
        when(peerRscData.getResource().getAssignedNode().getNodeConnection(
                accessContext, localRscData.getResource().getAssignedNode()))
            .thenReturn(nodeConn);
        when(rscConn.getProps(accessContext)).thenReturn(props);
        when(nodeConn.getProps(accessContext)).thenReturn(props);
        when(localRscData.getResource().getResourceConnection(accessContext, peerRscData.getResource()))
            .thenReturn(rscConn);
        when(peerRscData.getResource().getResourceConnection(accessContext, localRscData.getResource()))
            .thenReturn(rscConn);

    }

    private void setProps(String[] nodeNames, String... nicNames)
        throws DatabaseException, InvalidValueException, InvalidKeyException
    {
        assertThat(nodeNames.length == 2).isTrue();
        assertThat(nicNames.length == 4).isTrue();
        props.setProp("1/" + nodeNames[0], nicNames[0], "Paths");
        props.setProp("1/" + nodeNames[1], nicNames[1], "Paths");
        props.setProp("2/" + nodeNames[0], nicNames[2], "Paths");
        props.setProp("2/" + nodeNames[1], nicNames[3], "Paths");
    }

    @Test(expected = ImplementationError.class)
    public void testPeerRscsNull() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                null,
                whitelistProps
        );

        confFileBuilder.build();
    }

    @Test
    public void testPeerRscsEmpty() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.emptyList(),
                whitelistProps
        );

        assertThat(confFileBuilder.build().contains("connection\n")).isFalse();
    }

    @Test(expected = ImplementationError.class)
    public void testRscDfnNull() throws Exception
    {
        when(localRscData.getResource().getDefinition()).thenReturn(null);
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );

        confFileBuilder.build();
    }

    @Test
    public void testRscConnNull() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );

        String confFile = confFileBuilder.build();
        assertThat(confFile.contains("path\n")).isFalse();
        assertThat(confFile.contains("1.2.3.4") && confFile.contains("5.6.7.8")).isTrue();
    }

    @Test(expected = ImplementationError.class)
    public void testNoNodesConfigured() throws Exception
    {
        setProps(new String[] {"alpha", "alpha"}, "eth0", "eth1", "eth2", "eth3");
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );

        confFileBuilder.build();
    }

    @Test(expected = ImplementationError.class)
    public void testNodeNamesNotMatching() throws Exception
    {
        setProps(new String[] {"charlie", "delta"}, "eth0", "eth1", "eth2", "eth3");
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );

        confFileBuilder.build();
    }

    @Test(expected = StorageException.class)
    public void testUnknownNicName() throws Exception
    {
        setProps(new String[] {"alpha", "bravo"}, "eth-1", "eth1", "eth2", "eth3");
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );

        confFileBuilder.build();
    }

    @Test(expected = StorageException.class)
    public void testInvalidName()
            throws DatabaseException, InvalidKeyException, InvalidValueException, AccessDeniedException, StorageException
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );
        setProps(new String[] {"alpha", "bravo"}, "666", "666", "666", "666");

        confFileBuilder.build();
    }

    @Test
    public void testNewFormat() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );
        setProps(new String[] {"alpha", "bravo"}, "eth0", "eth1", "eth2", "eth3");

        String confFile = confFileBuilder.build();
        assertThat(confFile.contains("path\n") && confFile.contains("1.2.3.4") && confFile.contains("5.6.7.8"))
                .isTrue();
    }
/*
    @Test(expected = ImplementationError.class)
    public void testInvalidKey()
            throws SQLException, InvalidKeyException, InvalidValueException, AccessDeniedException, StorageException
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRsc,
                Collections.singletonList(peerRsc),
                whitelistProps
        );
        setProps(new String[] {"alpha", "bravo"}, null, null, null, null);
        confFileBuilder.build();
    }
*/
    @SuppressWarnings("checkstyle:magicnumber")
    private DrbdRscData makeMockResource(
            final int volumeNumber,
            final String nodeName,
            final String ipAddr,
            final boolean volumeDeleted,
            final boolean resourceDeleted,
            final boolean diskless
    )
            throws Exception
    {
        Resource resource = Mockito.mock(Resource.class);
        ResourceDefinition resourceDefinition = Mockito.mock(ResourceDefinition.class);
        ResourceGroup rscGrp = Mockito.mock(ResourceGroup.class);
        VolumeGroup vlmGrp = Mockito.mock(VolumeGroup.class);
        StateFlags<Resource.Flags> rscStateFlags = Mockito.mock(ResourceStateFlags.class);
        StateFlags<ResourceConnection.RscConnFlags> rscConnStateFlags = Mockito.mock(ResourceConnStateFlags.class);
        Node assignedNode = Mockito.mock(Node.class);
        NetInterface netInterface = Mockito.mock(NetInterface.class);
        Volume volume = Mockito.mock(Volume.class);
        StateFlags<Volume.VlmFlags> volumeFlags = Mockito.mock(VolumeStateFlags.class);
        VolumeDefinition volumeDefinition = Mockito.mock(VolumeDefinition.class);
        StorPool storPool = Mockito.mock(StorPool.class);
        rscConn = Mockito.mock(ResourceConnectionData.class);
        nodeConn = Mockito.mock(NodeConnection.class);

        Props storPoolProps = Mockito.mock(Props.class);
        Props vlmProps = Mockito.mock(Props.class);
        Props vlmDfnProps = Mockito.mock(Props.class);
        Props rscProps = Mockito.mock(Props.class);
        Props nodeProps = Mockito.mock(Props.class);
        Props rscDfnProps = Mockito.mock(Props.class);
        Props rscGrpProps = Mockito.mock(Props.class);
        Props vlmGrpProps = Mockito.mock(Props.class);
        Optional<Props> drbdprops = Optional.empty();

        when(storPool.getProps(accessContext)).thenReturn(storPoolProps);

        when(volumeDefinition.getVolumeNumber()).thenReturn(new VolumeNumber(volumeNumber));
        when(volumeDefinition.getResourceDefinition()).thenReturn(resourceDefinition);

        when(volumeFlags.isUnset(any(AccessContext.class), eq(Volume.VlmFlags.DELETE)))
            .thenReturn(!volumeDeleted);

        when(volume.getFlags()).thenReturn(volumeFlags);
        when(volume.getVolumeDefinition()).thenReturn(volumeDefinition);
        when(volume.getResourceDefinition()).thenReturn(resourceDefinition);
        when(volume.getProps(accessContext)).thenReturn(vlmProps);
        when(volume.getResource()).thenReturn(resource);

        when(netInterface.getAddress(any(AccessContext.class)))
                .thenReturn(new LsIpAddress(ipAddr));

        when(assignedNode.getName()).thenReturn(new NodeName(nodeName));
        when(assignedNode.streamNetInterfaces(any(AccessContext.class)))
                .thenAnswer(makeStreamAnswer(netInterface));

        when(assignedNode.getNetInterface(
                accessContext, new NetInterfaceName("eth0"))).thenReturn(netInterface);
        when(assignedNode.getNetInterface(
                accessContext, new NetInterfaceName("eth1"))).thenReturn(netInterface);
        when(assignedNode.getNetInterface(
                accessContext, new NetInterfaceName("eth2"))).thenReturn(netInterface);
        when(assignedNode.getNetInterface(
                accessContext, new NetInterfaceName("eth3"))).thenReturn(netInterface);
        when(assignedNode.getNetInterface(
                accessContext, new NetInterfaceName("eth-1"))).thenReturn(null);
        when(netInterface.getNode()).thenReturn(assignedNode);

        when(rscStateFlags.isUnset(any(AccessContext.class), eq(Resource.Flags.DELETE)))
            .thenReturn(!resourceDeleted);
        when(rscStateFlags.isUnset(any(AccessContext.class), eq(Resource.Flags.DISKLESS)))
            .thenReturn(!diskless);
        when(rscStateFlags.isSet(any(AccessContext.class), eq(Resource.Flags.DISKLESS)))
            .thenReturn(diskless);

        when(resourceDefinition.getName()).thenReturn(new ResourceName("testResource"));
        when(resourceDefinition.getProps(accessContext)).thenReturn(rscDfnProps);
        when(rscDfnProps.getNamespace(any(String.class))).thenReturn(Optional.empty());

        when(resourceDefinition.getResourceGroup()).thenReturn(rscGrp);
        when(rscGrp.getName()).thenReturn(new ResourceGroupName("testGroup"));
        when(rscGrp.getProps(accessContext)).thenReturn(rscGrpProps);
        when(rscGrp.getVolumeGroup(accessContext, new VolumeNumber(0))).thenReturn(vlmGrp);
        when(vlmGrp.getProps(accessContext)).thenReturn(vlmGrpProps);
        when(rscGrp.getVolumeGroupProps(accessContext, new VolumeNumber(0))).thenReturn(vlmGrpProps);

        when(volumeDefinition.getProps(accessContext)).thenReturn(vlmDfnProps);
        when(vlmDfnProps.getNamespace(ApiConsts.NAMESPC_DRBD_DISK_OPTIONS)).thenReturn(drbdprops);

        when(resource.getObjProt()).thenReturn(dummyObjectProtection);
        when(resource.getDefinition()).thenReturn(resourceDefinition);
        when(resource.getStateFlags()).thenReturn(rscStateFlags);
        when(rscConn.getStateFlags()).thenReturn(rscConnStateFlags);
        when(resource.getAssignedNode()).thenReturn(assignedNode);
        when(resource.iterateVolumes()).thenAnswer(makeIteratorAnswer(volume));
        when(resource.streamVolumes()).thenAnswer(makeStreamAnswer(volume));
        when(resource.getProps(accessContext)).thenReturn(rscProps);
        when(resource.disklessForPeers(accessContext)).thenReturn(diskless);

        when(assignedNode.getProps(accessContext)).thenReturn(nodeProps);


        List<DrbdRscData> rscDataList = new ArrayList<>();

        DrbdRscDfnData rscDfnData = new DrbdRscDfnData(
            resourceDefinition,
            "",
            InternalApiConsts.DEFAULT_PEER_COUNT,
            InternalApiConsts.DEFAULT_AL_STRIPES,
            InternalApiConsts.DEFAULT_AL_SIZE,
            42,
            null,
            "SuperSecretPassword",
            rscDataList,
            new TreeMap<>(),
            mockedTcpPool,
            DRBD_LAYER_NO_OP_DRIVER,
            transObjFactory,
            transMgrProvider
        );

        Map<VolumeNumber, DrbdVlmDfnData> drbdVlmDfnMap = new HashMap<>();
        DrbdRscData rscData;
        {
            Set<RscLayerObject> drbdRscDataChildren = new HashSet<>();
            Map<VolumeNumber, DrbdVlmData> drbdRscDataVlmMap = new HashMap<>();

            rscData = new DrbdRscData(
                idGenerator.incrementAndGet(),
                resource,
                null,
                rscDfnData,
                drbdRscDataChildren,
                drbdRscDataVlmMap,
                "",
                new NodeId(13),
                null, // copied from rscDfnData
                null, // copied from rscDfnData
                null, // copied from rscDfnData
                resource.isDiskless(accessContext) ?
                    DrbdRscObject.DrbdRscFlags.DISKLESS.flagValue : 0,
                DRBD_LAYER_NO_OP_DRIVER,
                transObjFactory,
                transMgrProvider
            );
            rscDataList.add(rscData);

            Map<VolumeNumber, VlmProviderObject> vlmProviderMap = new HashMap<>();
            StorageRscData storRscData = new StorageRscData(
                -1, // satellite does not care about rscLayerIds (database index only)
                rscData,
                resource,
                "",
                vlmProviderMap,
                STORAGE_LAYER_NO_OP_DRIVER,
                transObjFactory,
                transMgrProvider
            );
            for (Volume vlm : resource.streamVolumes().collect(Collectors.toList()))
            {
                VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();

                DrbdVlmDfnData drbdVlmDfnData = new DrbdVlmDfnData(
                    vlm.getVolumeDefinition(),
                    "",
                    99,
                    mockedMinorPool,
                    rscDfnData,
                    DRBD_LAYER_NO_OP_DRIVER,
                    transMgrProvider
                );
                drbdVlmDfnMap.put(vlmNr, drbdVlmDfnData);

                DrbdVlmData drbdVlmData = new DrbdVlmData(
                    vlm,
                    rscData,
                    drbdVlmDfnData,
                    null,
                    DRBD_LAYER_NO_OP_DRIVER,
                    transObjFactory,
                    transMgrProvider
                );
                drbdRscDataVlmMap.put(vlmNr, drbdVlmData);

                vlmProviderMap.put(
                    vlmNr,
                    new LvmData(
                        vlm,
                        storRscData,
                        storPool,
                        STORAGE_LAYER_NO_OP_DRIVER,
                        transObjFactory,
                        transMgrProvider
                    )
                );
            }

            drbdRscDataChildren.add(storRscData);
        }

        return rscData;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testMatchingBraceCounts() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
            errorReporter,
            accessContext,
            localRscData,
            Collections.singletonList(peerRscData),
            whitelistProps
        );
        String confFile = confFileBuilder.build();

        int leftBraceCount = countOccurrences(confFile, "\\{");
        int rightBraceCount = countOccurrences(confFile, "\\}");

        assertThat(leftBraceCount).isEqualTo(rightBraceCount);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testKeywordOccurrences() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );
        String confFile = confFileBuilder.build();

        assertThat(countOccurrences(confFile, "^ *resource")).isEqualTo(1);
        assertThat(countOccurrences(confFile, "^ *cram-hmac-alg ")).isEqualTo(1);
        assertThat(countOccurrences(confFile, "^ *shared-secret ")).isEqualTo(1);
        assertThat(countOccurrences(confFile, "^ *on ")).isEqualTo(2);
        assertThat(countOccurrences(confFile, "^ *volume ")).isEqualTo(2);
        assertThat(countOccurrences(confFile, "^ *disk ")).isEqualTo(2);
        assertThat(countOccurrences(confFile, "^ *meta-disk ")).isEqualTo(2);
        assertThat(countOccurrences(confFile, "^ *device ")).isEqualTo(2);
        assertThat(countOccurrences(confFile, "^ *node-id ")).isEqualTo(2);
        assertThat(countOccurrences(confFile, "^ *connection")).isEqualTo(1);
        assertThat(countOccurrences(confFile, "^ *host ")).isEqualTo(2);
        assertThat(countOccurrences(confFile, " address ")).isEqualTo(2);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testDeletedVolume() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );
        String confFileNormal = confFileBuilder.build();

        String confFileDeleted = new ConfFileBuilder(
            errorReporter,
            accessContext,
            makeMockResource(101, "testNode", "1.2.3.4", true, false, false),
            Collections.singletonList(makeMockResource(202, "testNode", "5.6.7.8", true, false, false)),
            whitelistProps
        ).build();

        assertThat(countOccurrences(confFileNormal, "^ *volume ")).isEqualTo(2);
        assertThat(countOccurrences(confFileDeleted, "^ *volume ")).isEqualTo(0);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testDeletedPeerResource() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                Collections.singletonList(peerRscData),
                whitelistProps
        );
        String confFileNormal = confFileBuilder.build();

        String confFileDeleted = new ConfFileBuilder(
            errorReporter,
            accessContext,
            makeMockResource(101, "testNode", "1.2.3.4", false, false, false),
            Collections.singletonList(makeMockResource(202, "testNode", "5.6.7.8", false, true, false)),
            whitelistProps
        ).build();

        assertThat(countOccurrences(confFileNormal, "^ *on ")).isEqualTo(2);
        assertThat(countOccurrences(confFileNormal, "^ *connection")).isEqualTo(1);
        assertThat(countOccurrences(confFileDeleted, "^ *on ")).isEqualTo(1);
        assertThat(countOccurrences(confFileDeleted, "^ *connection")).isEqualTo(0);
    }

    @Test
    public void testNoConnectionBetweenDiskless() throws Exception
    {
        {
            List<DrbdRscData> peerRscs = new ArrayList<>();
            peerRscs.add(makeMockResource(0, "testNode1", "5.6.7.8", false, false, true));
            peerRscs.add(makeMockResource(0, "testNode2", "9.10.11.12", false, false, true));
            String confFileNormal = new ConfFileBuilder(
                errorReporter,
                accessContext,
                makeMockResource(0, "localNode", "1.2.3.4", false, false, false),
                peerRscs,
                whitelistProps
            ).build();

            assertThat(countOccurrences(confFileNormal, "^ *connection")).isEqualTo(2);
        }

        {
            List<DrbdRscData> peerRscs = new ArrayList<>();
            peerRscs.add(makeMockResource(0, "testNode1", "5.6.7.8", false, false, false));
            peerRscs.add(makeMockResource(0, "testNode2", "9.10.11.12", false, false, true));
            String confFileNormal = new ConfFileBuilder(
                errorReporter,
                accessContext,
                makeMockResource(0, "localNode", "1.2.3.4", false, false, true),
                peerRscs,
                whitelistProps
            ).build();

            assertThat(countOccurrences(confFileNormal, "^ *connection")).isEqualTo(1);
        }

        {
            List<DrbdRscData> peerRscs = new ArrayList<>();
            peerRscs.add(makeMockResource(0, "testNode1", "5.6.7.8", false, false, false));
            peerRscs.add(makeMockResource(0, "testNode2", "9.10.11.12", false, false, false));
            String confFileNormal = new ConfFileBuilder(
                errorReporter,
                accessContext,
                makeMockResource(0, "localNode", "1.2.3.4", false, false, false),
                peerRscs,
                whitelistProps
            ).build();

            assertThat(countOccurrences(confFileNormal, "^ *connection")).isEqualTo(2);
        }
    }

    private int countOccurrences(final String str, final String regex)
    {
        Matcher matcher = Pattern.compile(regex, Pattern.MULTILINE).matcher(str);

        int count = 0;
        while (matcher.find())
        {
            count++;
        }

        return count;
    }

    @SafeVarargs
    private final <T> Answer<Iterator<T>> makeIteratorAnswer(final T... ts)
    {
        return new Answer<Iterator<T>>()
        {
            @Override
            public Iterator<T> answer(final InvocationOnMock invocation)
            {
                return Arrays.asList(ts).iterator();
            }
        };
    }

    @SafeVarargs
    private final <T> Answer<Stream<T>> makeStreamAnswer(final T... ts)
    {
        return new Answer<Stream<T>>()
        {
            @Override
            public Stream<T> answer(InvocationOnMock invocation) throws Throwable
            {
                return Arrays.asList(ts).stream();
            }
        };
    }

    private interface ResourceStateFlags extends StateFlags<Resource.Flags>
    {
    }

    private interface ResourceConnStateFlags extends StateFlags<ResourceConnection.RscConnFlags>
    {
    }

    private interface VolumeStateFlags extends StateFlags<Volume.VlmFlags>
    {
    }

}

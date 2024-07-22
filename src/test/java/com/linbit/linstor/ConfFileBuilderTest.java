package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.drbd.DrbdVersion;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
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
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SatelliteLayerDrbdRscDbDriver;
import com.linbit.linstor.dbdrivers.SatelliteLayerDrbdRscDfnDbDriver;
import com.linbit.linstor.dbdrivers.SatelliteLayerDrbdVlmDbDriver;
import com.linbit.linstor.dbdrivers.SatelliteLayerDrbdVlmDfnDbDriver;
import com.linbit.linstor.dbdrivers.SatelliteLayerResourceIdDriver;
import com.linbit.linstor.dbdrivers.SatelliteLayerStorageRscDbDriver;
import com.linbit.linstor.dbdrivers.SatelliteLayerStorageVlmDbDriver;
import com.linbit.linstor.dbdrivers.SatellitePropDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.layer.drbd.utils.ConfFileBuilder;
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
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.testutils.EmptyErrorReporter;
import com.linbit.linstor.timer.CoreTimerImpl;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:magicnumber")
public class ConfFileBuilderTest
{
    private static final LayerDrbdRscDfnDatabaseDriver LAYER_DRBD_RSC_DFN_NO_OP_DRIVER;
    private static final LayerDrbdVlmDfnDatabaseDriver LAYER_DRBD_VLM_DFN_NO_OP_DRIVER;
    private static final LayerDrbdRscDatabaseDriver LAYER_DRBD_RSC_NO_OP_DRIVER;
    private static final LayerDrbdVlmDatabaseDriver LAYER_DRBD_VLM_NO_OP_DRIVER;
    private static final LayerStorageRscDatabaseDriver LAYER_STORAGE_RSC_NO_OP_DRIVER;
    private static final LayerStorageVlmDatabaseDriver LAYER_STORAGE_VLM_NO_OP_DRIVER;

    static
    {
        LAYER_DRBD_RSC_DFN_NO_OP_DRIVER = new SatelliteLayerDrbdRscDfnDbDriver();
        LAYER_DRBD_VLM_DFN_NO_OP_DRIVER = new SatelliteLayerDrbdVlmDfnDbDriver();

        SatelliteLayerResourceIdDriver dummyLRIDriver = new SatelliteLayerResourceIdDriver();

        LAYER_DRBD_RSC_NO_OP_DRIVER = new SatelliteLayerDrbdRscDbDriver(dummyLRIDriver);
        LAYER_DRBD_VLM_NO_OP_DRIVER = new SatelliteLayerDrbdVlmDbDriver(dummyLRIDriver);
        LAYER_STORAGE_RSC_NO_OP_DRIVER = new SatelliteLayerStorageRscDbDriver(dummyLRIDriver);
        LAYER_STORAGE_VLM_NO_OP_DRIVER = new SatelliteLayerStorageVlmDbDriver(dummyLRIDriver);
    }

    private ErrorReporter errorReporter;
    private AccessContext accessContext;

    private ObjectProtection dummyObjectProtection;
    private WhitelistProps whitelistProps;

    private DrbdRscData<Resource> localRscData, peerRscData;

    private ConfFileBuilder confFileBuilder;
    private ResourceConnection rscConn;
    private NodeConnection nodeConn;
    private PropsContainer rscConnProps;
    private PropsContainer nodeConnProps;
    private Provider<TransactionMgr> transMgrProvider;
    private TransactionObjectFactory transObjFactory;

    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private DynamicNumberPool mockedTcpPool;
    private DynamicNumberPool mockedMinorPool;
    private Props stltProps;
    private DrbdVersion drbdVersion;

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
        PropsContainerFactory propsContainerFactory = new PropsContainerFactory(
                new SatellitePropDriver(), transMgrProvider);
        rscConnProps = propsContainerFactory.getInstance("TESTINSTANCE", null, LinStorObject.RSC_CONN);
        nodeConnProps = propsContainerFactory.getInstance("TESTINSTANCE", null, LinStorObject.NODE_CONN);

        mockedTcpPool = Mockito.mock(DynamicNumberPool.class);
        mockedMinorPool = Mockito.mock(DynamicNumberPool.class);

        when(mockedTcpPool.autoAllocate()).thenReturn(9001);
        when(mockedMinorPool.autoAllocate()).thenReturn(99);

        localRscData = makeMockResource(101, "alpha", "1.2.3.4", false, false, false);
        peerRscData = makeMockResource(202, "bravo", "5.6.7.8", false, false, false);
        when(localRscData.getAbsResource().getNode().getNodeConnection(
                accessContext, peerRscData.getAbsResource().getNode()))
            .thenReturn(nodeConn);
        when(peerRscData.getAbsResource().getNode().getNodeConnection(
                accessContext, localRscData.getAbsResource().getNode()))
            .thenReturn(nodeConn);
        when(rscConn.getProps(accessContext)).thenReturn(rscConnProps);
        when(nodeConn.getProps(accessContext)).thenReturn(nodeConnProps);
        when(localRscData.getAbsResource().getAbsResourceConnection(accessContext, peerRscData.getAbsResource()))
            .thenReturn(rscConn);
        when(peerRscData.getAbsResource().getAbsResourceConnection(accessContext, localRscData.getAbsResource()))
            .thenReturn(rscConn);

        stltProps = propsContainerFactory.getInstance("STLT_CFG", null, LinStorObject.SATELLITE);
        drbdVersion = new DrbdVersion(new CoreTimerImpl(), new EmptyErrorReporter());
    }

    private void setProps(String[] nodeNames, String... nicNames)
        throws DatabaseException, InvalidValueException, InvalidKeyException
    {
        assertThat(nodeNames.length == 2).isTrue();
        assertThat(nicNames.length == 4).isTrue();
        rscConnProps.setProp("1/" + nodeNames[0], nicNames[0], "Paths");
        rscConnProps.setProp("1/" + nodeNames[1], nicNames[1], "Paths");
        rscConnProps.setProp("2/" + nodeNames[0], nicNames[2], "Paths");
        rscConnProps.setProp("2/" + nodeNames[1], nicNames[3], "Paths");
        nodeConnProps.setProp("1/" + nodeNames[0], nicNames[0], "Paths");
        nodeConnProps.setProp("1/" + nodeNames[1], nicNames[1], "Paths");
        nodeConnProps.setProp("2/" + nodeNames[0], nicNames[2], "Paths");
        nodeConnProps.setProp("2/" + nodeNames[1], nicNames[3], "Paths");
    }

    @Test(expected = ImplementationError.class)
    public void testPeerRscsNull() throws Exception
    {
        confFileBuilder = new ConfFileBuilder(
                errorReporter,
                accessContext,
                localRscData,
                null,
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
        );

        assertThat(confFileBuilder.build().contains("connection\n")).isFalse();
    }

    @Test(expected = ImplementationError.class)
    public void testRscDfnNull() throws Exception
    {
        when(localRscData.getAbsResource().getResourceDefinition()).thenReturn(null);
        confFileBuilder = new ConfFileBuilder(
            errorReporter,
            accessContext,
            localRscData,
            Collections.singletonList(peerRscData),
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
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
    private DrbdRscData<Resource> makeMockResource(
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
        StateFlags<ResourceConnection.Flags> rscConnStateFlags = Mockito.mock(ResourceConnStateFlags.class);
        Node assignedNode = Mockito.mock(Node.class);
        NetInterface netInterface = Mockito.mock(NetInterface.class);
        Volume volume = Mockito.mock(Volume.class);
        StateFlags<Volume.Flags> volumeFlags = Mockito.mock(VolumeStateFlags.class);
        VolumeDefinition volumeDefinition = Mockito.mock(VolumeDefinition.class);
        StorPool storPool = Mockito.mock(StorPool.class);
        rscConn = Mockito.mock(ResourceConnection.class);
        nodeConn = Mockito.mock(NodeConnection.class);

        Props storPoolProps = Mockito.mock(Props.class);
        Props vlmProps = Mockito.mock(Props.class);
        Props vlmDfnProps = Mockito.mock(Props.class);
        Props rscProps = Mockito.mock(Props.class);
        Props nodeProps = Mockito.mock(Props.class);
        Props rscDfnProps = Mockito.mock(Props.class);
        Props rscGrpProps = Mockito.mock(Props.class);
        Props vlmGrpProps = Mockito.mock(Props.class);

        Props drbdprops = Mockito.mock(Props.class);

        when(storPool.getProps(accessContext)).thenReturn(storPoolProps);

        when(volumeDefinition.getVolumeNumber()).thenReturn(new VolumeNumber(volumeNumber));
        when(volumeDefinition.getResourceDefinition()).thenReturn(resourceDefinition);

        when(volumeFlags.isUnset(any(AccessContext.class), eq(Volume.Flags.DELETE)))
            .thenReturn(!volumeDeleted);

        when(volume.getFlags()).thenReturn(volumeFlags);
        when(volume.getVolumeDefinition()).thenReturn(volumeDefinition);
        when(volume.getResourceDefinition()).thenReturn(resourceDefinition);
        when(volume.getProps(accessContext)).thenReturn(vlmProps);
        when(volume.getAbsResource()).thenReturn(resource);
        when(volume.getVolumeNumber()).thenReturn(new VolumeNumber(volumeNumber));

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
        when(rscStateFlags.isUnset(any(AccessContext.class), eq(Resource.Flags.DRBD_DISKLESS)))
            .thenReturn(!diskless);
        when(rscStateFlags.isSet(any(AccessContext.class), eq(Resource.Flags.DRBD_DISKLESS)))
            .thenReturn(diskless);
        when(rscStateFlags.isUnset(any(AccessContext.class), eq(Resource.Flags.NVME_INITIATOR)))
            .thenReturn(!diskless);
        when(rscStateFlags.isSet(any(AccessContext.class), eq(Resource.Flags.NVME_INITIATOR)))
            .thenReturn(diskless);
        when(rscStateFlags.isUnset(any(AccessContext.class), eq(Resource.Flags.EBS_INITIATOR)))
            .thenReturn(!diskless);
        when(rscStateFlags.isSet(any(AccessContext.class), eq(Resource.Flags.EBS_INITIATOR)))
            .thenReturn(diskless);

        when(resourceDefinition.getName()).thenReturn(new ResourceName("testResource"));
        when(resourceDefinition.getProps(accessContext)).thenReturn(rscDfnProps);
        when(rscDfnProps.getNamespace(any(String.class))).thenReturn(null);

        when(resourceDefinition.getResourceGroup()).thenReturn(rscGrp);
        when(rscGrp.getName()).thenReturn(new ResourceGroupName("testGroup"));
        when(rscGrp.getProps(accessContext)).thenReturn(rscGrpProps);
        when(rscGrp.getVolumeGroup(accessContext, new VolumeNumber(0))).thenReturn(vlmGrp);
        when(vlmGrp.getProps(accessContext)).thenReturn(vlmGrpProps);
        when(rscGrp.getVolumeGroupProps(accessContext, new VolumeNumber(0))).thenReturn(vlmGrpProps);

        when(volumeDefinition.getProps(accessContext)).thenReturn(vlmDfnProps);
        when(vlmDfnProps.getNamespace(ApiConsts.NAMESPC_DRBD_DISK_OPTIONS)).thenReturn(drbdprops);

        when(resource.getObjProt()).thenReturn(dummyObjectProtection);
        when(resource.getResourceDefinition()).thenReturn(resourceDefinition);
        when(resource.getResourceDefinition()).thenReturn(resourceDefinition);
        when(resource.getStateFlags()).thenReturn(rscStateFlags);
        when(rscConn.getStateFlags()).thenReturn(rscConnStateFlags);
        when(resource.getNode()).thenReturn(assignedNode);
        when(resource.iterateVolumes()).thenAnswer(makeIteratorAnswer(volume));
        when(resource.streamVolumes()).thenAnswer(makeStreamAnswer(volume));
        when(resource.getProps(accessContext)).thenReturn(rscProps);
        when(resource.disklessForDrbdPeers(accessContext)).thenReturn(diskless);

        when(assignedNode.getProps(accessContext)).thenReturn(nodeProps);


        List<DrbdRscData<Resource>> rscDataList = new ArrayList<>();

        DrbdRscDfnData<Resource> rscDfnData = new DrbdRscDfnData<>(
            resourceDefinition.getName(),
            null,
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
            LAYER_DRBD_RSC_DFN_NO_OP_DRIVER,
            transObjFactory,
            transMgrProvider
        );

        Map<VolumeNumber, DrbdVlmDfnData<Resource>> drbdVlmDfnMap = new HashMap<>();
        DrbdRscData<Resource> rscData;
        {
            Set<AbsRscLayerObject<Resource>> drbdRscDataChildren = new HashSet<>();
            Map<VolumeNumber, DrbdVlmData<Resource>> drbdRscDataVlmMap = new HashMap<>();

            rscData = new DrbdRscData<>(
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
                resource.isDrbdDiskless(accessContext) ?
                    DrbdRscObject.DrbdRscFlags.DISKLESS.flagValue : 0,
                LAYER_DRBD_RSC_NO_OP_DRIVER,
                LAYER_DRBD_VLM_NO_OP_DRIVER,
                transObjFactory,
                transMgrProvider
            );
            rscDataList.add(rscData);

            Map<VolumeNumber, VlmProviderObject<Resource>> vlmProviderMap = new HashMap<>();
            StorageRscData<Resource> storRscData = new StorageRscData<>(
                -1, // satellite does not care about rscLayerIds (database index only)
                rscData,
                resource,
                "",
                vlmProviderMap,
                LAYER_STORAGE_RSC_NO_OP_DRIVER,
                LAYER_STORAGE_VLM_NO_OP_DRIVER,
                transObjFactory,
                transMgrProvider
            );
            for (Volume vlm : resource.streamVolumes().collect(Collectors.toList()))
            {
                VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();

                DrbdVlmDfnData<Resource> drbdVlmDfnData = new DrbdVlmDfnData<>(
                    vlm.getVolumeDefinition(),
                    vlm.getResourceDefinition().getName(),
                    null,
                    "",
                    vlm.getVolumeNumber(),
                    99,
                    mockedMinorPool,
                    rscDfnData,
                    LAYER_DRBD_VLM_DFN_NO_OP_DRIVER,
                    transMgrProvider
                );
                drbdVlmDfnMap.put(vlmNr, drbdVlmDfnData);

                DrbdVlmData<Resource> drbdVlmData = new DrbdVlmData<>(
                    vlm,
                    rscData,
                    drbdVlmDfnData,
                    null,
                    LAYER_DRBD_VLM_NO_OP_DRIVER,
                    transObjFactory,
                    transMgrProvider
                );
                drbdRscDataVlmMap.put(vlmNr, drbdVlmData);

                vlmProviderMap.put(
                    vlmNr,
                    new LvmData<>(
                        vlm,
                        storRscData,
                        storPool,
                        LAYER_STORAGE_VLM_NO_OP_DRIVER,
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
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
        );
        String confFile = confFileBuilder.build();

        assertThat(countOccurrences(confFile, "^ *resource")).isEqualTo(1);
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
            whitelistProps,
            stltProps,
            drbdVersion
        );
        String confFileNormal = confFileBuilder.build();

        String confFileDeleted = new ConfFileBuilder(
            errorReporter,
            accessContext,
            makeMockResource(101, "testNode", "1.2.3.4", true, false, false),
            Collections.singletonList(makeMockResource(202, "testNode", "5.6.7.8", true, false, false)),
            whitelistProps,
            stltProps,
            drbdVersion
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
            whitelistProps,
            stltProps,
            drbdVersion
        );
        String confFileNormal = confFileBuilder.build();

        String confFileDeleted = new ConfFileBuilder(
            errorReporter,
            accessContext,
            makeMockResource(101, "testNode", "1.2.3.4", false, false, false),
            Collections.singletonList(makeMockResource(202, "testNode", "5.6.7.8", false, true, false)),
            whitelistProps,
            stltProps,
            drbdVersion
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
            List<DrbdRscData<Resource>> peerRscs = new ArrayList<>();
            peerRscs.add(makeMockResource(0, "testNode1", "5.6.7.8", false, false, true));
            peerRscs.add(makeMockResource(0, "testNode2", "9.10.11.12", false, false, true));
            String confFileNormal = new ConfFileBuilder(
                errorReporter,
                accessContext,
                makeMockResource(0, "localNode", "1.2.3.4", false, false, false),
                peerRscs,
                whitelistProps,
                stltProps,
                drbdVersion
            ).build();

            assertThat(countOccurrences(confFileNormal, "^ *connection")).isEqualTo(2);
        }

        {
            List<DrbdRscData<Resource>> peerRscs = new ArrayList<>();
            peerRscs.add(makeMockResource(0, "testNode1", "5.6.7.8", false, false, false));
            peerRscs.add(makeMockResource(0, "testNode2", "9.10.11.12", false, false, true));
            String confFileNormal = new ConfFileBuilder(
                errorReporter,
                accessContext,
                makeMockResource(0, "localNode", "1.2.3.4", false, false, true),
                peerRscs,
                whitelistProps,
                stltProps,
                drbdVersion
            ).build();

            assertThat(countOccurrences(confFileNormal, "^ *connection")).isEqualTo(1);
        }

        {
            List<DrbdRscData<Resource>> peerRscs = new ArrayList<>();
            peerRscs.add(makeMockResource(0, "testNode1", "5.6.7.8", false, false, false));
            peerRscs.add(makeMockResource(0, "testNode2", "9.10.11.12", false, false, false));
            String confFileNormal = new ConfFileBuilder(
                errorReporter,
                accessContext,
                makeMockResource(0, "localNode", "1.2.3.4", false, false, false),
                peerRscs,
                whitelistProps,
                stltProps,
                drbdVersion
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
        return new Answer<>()
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
        return new Answer<>()
        {
            @Override
            public Stream<T> answer(InvocationOnMock invocation)
            {
                return Arrays.stream(ts);
            }
        };
    }

    private interface ResourceStateFlags extends StateFlags<Resource.Flags>
    {
    }

    private interface ResourceConnStateFlags extends StateFlags<ResourceConnection.Flags>
    {
    }

    private interface VolumeStateFlags extends StateFlags<Volume.Flags>
    {
    }

}

package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscMakeAvailableApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemoteControllerFactory;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

/**
 * make-available semantics for DRBD-over-EBS resource definitions.
 *
 * <p>The supported EBS deployment model is: <code>rg spawn</code> places one EBS_TARGET resource per availability
 * zone (the targets own the EBS volumes, DRBD does not run there). The <b>first</b> <code>r mkavail</code> on a
 * node in an AZ creates the EBS initiator of that AZ, which attaches the target's volume and is the DRBD
 * <b>diskful</b> peer. Every <b>further</b> <code>r mkavail</code> in an AZ that already has an initiator creates a
 * plain DRBD client (DRBD_DISKLESS + CLIENT role) on the node's diskless pool.</p>
 *
 * <p>Regression guard for GitHub issue #513: since v1.34.0 make-available passed <code>drbdClient=true</code> for
 * the initiator as well, and the resource creation coerced the initiator into DRBD_DISKLESS + CLIENT because the
 * EBS_INIT pool has no backing device, so nothing ever attached the volume.</p>
 *
 * <p>Fixture: EBS targets are EBS_TARGET nodes with exactly one EBS_TARGET pool whose remote defines the AZ.
 * Satellites carry an EBS_INIT pool (same remote) plus a DISKLESS pool, like a real node with the default
 * diskless pool. Target volumes carry the EbsVlmId property the target satellite would report after creating the
 * AWS volume.</p>
 */
@SuppressWarnings("checkstyle:magicnumber")
public class RscMakeAvailableEbsApiTest extends ApiTestBase
{
    private static final String AZ_A = "eu-central-1a";
    private static final String AZ_B = "eu-central-1b";
    private static final String EBS_NAMESPC = ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS;
    private static final String EBS_REMOTE_KEY_NAMESPC = ApiConsts.NAMESPC_STORAGE_DRIVER + "/" +
        ApiConsts.NAMESPC_EBS;
    private static final long VLM_SIZE_KIB = 100_000;

    @Inject
    private Provider<CtrlRscMakeAvailableApiCallHandler> rscMakeAvailableApiCallHandlerProvider;
    @Inject
    private CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    @Inject
    private EbsRemoteControllerFactory ebsRemoteFactory;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    // Mocked so the create flow does not block on the resource-ready event: with more than one resource in the
    // definition, waitResourcesReady() waits up to 15s per resource for a resourceStateEvent that no (mocked)
    // satellite ever fires. Returning an empty stream lets the wait complete immediately.
    @Bind
    @Mock
    protected EventWaiter eventWaiter;

    @Mock
    protected Peer mockTargetA;
    @Mock
    protected Peer mockTargetB;
    @Mock
    protected Peer mockInitiatorA1;
    @Mock
    protected Peer mockInitiatorA2;
    @Mock
    protected Peer mockInitiatorB1;
    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    private final ResourceName rscName;
    private final StorPoolName ebsTargetPoolName;
    private final StorPoolName ebsInitPoolName;
    private final StorPoolName disklessPoolName;
    private final NodeName targetAName;
    private final NodeName targetBName;
    private final NodeName initiatorA1Name;
    private final NodeName initiatorA2Name;
    private final NodeName initiatorB1Name;
    private final VolumeNumber vlmNr;

    private ResourceDefinition rscDfn;

    public RscMakeAvailableEbsApiTest() throws Exception
    {
        vlmNr = new VolumeNumber(0);
        rscName = new ResourceName("EbsRsc");
        ebsTargetPoolName = new StorPoolName("EbsPool");
        ebsInitPoolName = new StorPoolName("EbsInitPool");
        disklessPoolName = new StorPoolName("DfltDisklessStorPool");
        targetAName = new NodeName("EbsTargetA");
        targetBName = new NodeName("EbsTargetB");
        initiatorA1Name = new NodeName("SatelliteA1");
        initiatorA2Name = new NodeName("SatelliteA2");
        initiatorB1Name = new NodeName("SatelliteB1");
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(freeCapacityFetcher.fetchThinFreeCapacities(any()))
            .thenReturn(Mono.just(Collections.emptyMap()));
        Mockito.when(eventWaiter.waitForStream(any(), any())).thenReturn(Flux.empty());

        for (Peer peer : Arrays.asList(mockTargetA, mockTargetB, mockInitiatorA1, mockInitiatorA2, mockInitiatorB1))
        {
            stubSatellitePeer(peer, mockExtToolsMgr, new SatelliteState(), true);
        }
        stubAllExtToolsSupported(mockExtToolsMgr);
        // EBS_TARGET + EBS_INIT pools in one resource definition count as "mixed storage pools", which requires a
        // DRBD version on every involved satellite (MixedStorPoolHelper)
        for (ExtTools drbdTool : Arrays.asList(ExtTools.DRBD9_KERNEL, ExtTools.DRBD9_UTILS))
        {
            Mockito.when(mockExtToolsMgr.getExtToolInfo(drbdTool))
                .thenReturn(new ExtToolsInfo(drbdTool, true, 9, 2, 14, Collections.emptyList()));
        }

        LayerPayload payload = new LayerPayload();
        DrbdRscDfnPayload drbdRscDfn = payload.getDrbdRscDfn();
        drbdRscDfn.sharedSecret = "NotTellingYou";
        drbdRscDfn.transportType = TransportType.IP;
        rscDfn = resourceDefinitionFactory.create(
            rscName,
            null,
            null,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            payload,
            createDefaultResourceGroup()
        );
        rscDfnMap.put(rscName, rscDfn);
        volumeDefinitionFactory.create(rscDfn, vlmNr, null, VLM_SIZE_KIB, null);

        ctrlConf.setProp(
            InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
            randomUUID().toString(),
            ApiConsts.NAMESPC_CLUSTER
        );

        commitAndCleanUp(true);
    }

    @After
    @Override
    public void tearDown() throws Exception
    {
        commitAndCleanUp(false);
    }

    @Test
    public void makeAvailableFirstInAzCreatesEbsInitiator() throws Exception
    {
        Resource targetA = createAz(AZ_A, targetAName, mockTargetA, "vol-0a", initiatorA1Name, mockInitiatorA1);

        ApiCallRc rc = makeAvailable(initiatorA1Name, false);

        assertNoError(rc);
        assertEbsInitiator(initiatorA1Name, ebsInitPoolName, "vol-0a");
        assertConnectedInitiator(targetA, initiatorA1Name);
        assertThat(rscDfn.getResourceCount()).isEqualTo(2);
    }

    @Test
    public void makeAvailableSecondInAzCreatesDrbdClient() throws Exception
    {
        Resource targetA = createAz(AZ_A, targetAName, mockTargetA, "vol-0a", initiatorA1Name, mockInitiatorA1);
        createEbsInitiatorDirectly(initiatorA1Name);
        addSatellite(initiatorA2Name, mockInitiatorA2, remoteName(AZ_A));

        ApiCallRc rc = makeAvailable(initiatorA2Name, false);

        assertNoError(rc);
        assertDrbdClient(initiatorA2Name, disklessPoolName);
        // the initiator of the AZ is untouched and the target still belongs to it
        assertEbsInitiator(initiatorA1Name, ebsInitPoolName, "vol-0a");
        assertConnectedInitiator(targetA, initiatorA1Name);
        assertThat(rscDfn.getResourceCount()).isEqualTo(3);
    }

    @Test
    public void makeAvailableFirstInSecondAzCreatesSecondInitiator() throws Exception
    {
        Resource targetA = createAz(AZ_A, targetAName, mockTargetA, "vol-0a", initiatorA1Name, mockInitiatorA1);
        createEbsInitiatorDirectly(initiatorA1Name);
        Resource targetB = createAz(AZ_B, targetBName, mockTargetB, "vol-0b", initiatorB1Name, mockInitiatorB1);

        ApiCallRc rc = makeAvailable(initiatorB1Name, false);

        assertNoError(rc);
        // an initiator in another AZ is a diskful DRBD peer, but this AZ's target is still free: initiator again
        assertEbsInitiator(initiatorB1Name, ebsInitPoolName, "vol-0b");
        assertConnectedInitiator(targetB, initiatorB1Name);
        assertConnectedInitiator(targetA, initiatorA1Name);
        assertThat(rscDfn.getResourceCount()).isEqualTo(4);
    }

    @Test
    public void makeAvailableSecondInAzWithForeignInitiatorStillCreatesDrbdClient() throws Exception
    {
        // AZ-a: target + initiator, AZ-b: target + initiator. A further node in AZ-a gets a client, not a
        // third initiator, even though AZ-b's initiator makes hasDrbdDiskfulPeer true for a different reason
        createAz(AZ_A, targetAName, mockTargetA, "vol-0a", initiatorA1Name, mockInitiatorA1);
        createEbsInitiatorDirectly(initiatorA1Name);
        createAz(AZ_B, targetBName, mockTargetB, "vol-0b", initiatorB1Name, mockInitiatorB1);
        createEbsInitiatorDirectly(initiatorB1Name);
        addSatellite(initiatorA2Name, mockInitiatorA2, remoteName(AZ_A));

        ApiCallRc rc = makeAvailable(initiatorA2Name, false);

        assertNoError(rc);
        assertDrbdClient(initiatorA2Name, disklessPoolName);
        assertThat(rscDfn.getResourceCount()).isEqualTo(5);
    }

    @Test
    public void makeAvailableInitiatorFailsWhileTargetVolumeNotProvisioned() throws Exception
    {
        // the target satellite has not reported the AWS volume id yet (credentials are available)
        createAz(AZ_A, targetAName, mockTargetA, null, initiatorA1Name, mockInitiatorA1);

        ApiCallRc rc = makeAvailable(initiatorA1Name, false);

        assertContainsRc(rc, ApiConsts.FAIL_MISSING_EBS_TARGET);
        assertThat(getRsc(initiatorA1Name)).isNull();
    }

    @Test
    public void makeAvailableInitiatorFailsWithoutDecryptedCredentials() throws Exception
    {
        // controller restarted without master passphrase: the remote has no decrypted keys, the target can never
        // create the volume, so the user is pointed at "encryption enter-passphrase" instead of a retry
        createAz(AZ_A, targetAName, mockTargetA, null, initiatorA1Name, mockInitiatorA1);
        enterScope();
        EbsRemote remote = (EbsRemote) remoteMap.get(new RemoteName(remoteName(AZ_A)));
        remote.setDecryptedAccessKey(null);
        remote.setDecryptedSecretKey(null);
        commitAndCleanUp(true);

        ApiCallRc rc = makeAvailable(initiatorA1Name, false);

        assertContainsRc(rc, ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY);
        assertThat(getRsc(initiatorA1Name)).isNull();
    }

    /*
     * fixture
     */

    private static String remoteName(String az)
    {
        return "ebs-rem-" + az;
    }

    /**
     * Creates the EBS remote of the given AZ, an EBS_TARGET node with its single EBS_TARGET pool, the target
     * resource (optionally with the EbsVlmId the target satellite would have reported) and one satellite node
     * with an EBS_INIT pool of that AZ plus a diskless pool.
     *
     * @return the target resource
     */
    private Resource createAz(
        String az,
        NodeName targetNodeName,
        Peer targetPeer,
        @Nullable String ebsVlmId,
        NodeName satelliteNodeName,
        Peer satellitePeer
    )
        throws Exception
    {
        String remote = remoteName(az);
        createEbsRemote(remote, az);

        Node targetNode = addNode(targetNodeName, Node.Type.EBS_TARGET, targetPeer);
        createEbsStorPool(targetNode, ebsTargetPoolName, DeviceProviderKind.EBS_TARGET, remote);
        Resource targetRsc = createRscDirectly(targetNodeName, 0L, ebsTargetPoolName);
        if (ebsVlmId != null)
        {
            enterScope();
            getVolume(targetRsc).getProps().setProp(InternalApiConsts.KEY_EBS_VLM_ID, ebsVlmId, EBS_NAMESPC);
            commitAndCleanUp(true);
        }

        addSatellite(satelliteNodeName, satellitePeer, remote);
        return targetRsc;
    }

    private void createEbsRemote(String remoteNameStr, String az) throws Exception
    {
        enterScope();
        RemoteName name = new RemoteName(remoteNameStr);
        String region = az.substring(0, az.length() - 1);
        EbsRemote remote = ebsRemoteFactory.create(
            name,
            0L,
            new URI("https://ec2." + region + ".amazonaws.com").toURL(),
            region,
            az,
            new byte[0],
            new byte[0]
        );
        remote.setDecryptedAccessKey("accessKey");
        remote.setDecryptedSecretKey("secretKey");
        remoteMap.put(name, remote);
        commitAndCleanUp(true);
    }

    private Node addNode(NodeName nodeName, Node.Type type, Peer peer) throws Exception
    {
        enterScope();
        Node node = nodeFactory.create(nodeName, type, null);
        node.setPeer(peer);
        nodesMap.put(nodeName, node);
        commitAndCleanUp(true);
        return node;
    }

    private void addSatellite(NodeName nodeName, Peer peer, String ebsRemote) throws Exception
    {
        Node node = addNode(nodeName, Node.Type.SATELLITE, peer);
        createEbsStorPool(node, ebsInitPoolName, DeviceProviderKind.EBS_INIT, ebsRemote);
        createStorPool(node, disklessPoolName, DeviceProviderKind.DISKLESS);
    }

    private void createEbsStorPool(Node node, StorPoolName poolName, DeviceProviderKind kind, String ebsRemote)
        throws Exception
    {
        StorPool storPool = createStorPool(node, poolName, kind);
        enterScope();
        storPool.getProps().setProp(ApiConsts.KEY_REMOTE, ebsRemote, EBS_REMOTE_KEY_NAMESPC);
        commitAndCleanUp(true);
    }

    private StorPool createStorPool(Node node, StorPoolName storPoolName, DeviceProviderKind kind) throws Exception
    {
        enterScope();
        StorPoolDefinition storPoolDfn = storPoolDfnMap.get(storPoolName);
        if (storPoolDfn == null)
        {
            storPoolDfn = storPoolDefinitionFactory.create(storPoolName);
            storPoolDfnMap.put(storPoolName, storPoolDfn);
        }
        FreeSpaceMgr fsm = freeSpaceMgrFactory.getInstance(new SharedStorPoolName(node.getName(), storPoolName));
        StorPool storPool = storPoolFactory.create(node, storPoolDfn, kind, fsm, false);
        storPool.getFreeSpaceTracker().setCapacityInfo(10_000_000, 10_000_000);
        commitAndCleanUp(true);
        return storPool;
    }

    /**
     * Creates the resource through the same DB helper make-available uses, but bypassing make-available's
     * decision logic, so tests about the second call do not depend on the first call being correct.
     */
    private Resource createRscDirectly(NodeName nodeName, long flags, StorPoolName storPoolName) throws Exception
    {
        enterScope();
        Map<String, String> rscProps = new TreeMap<>();
        rscProps.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolName.displayValue);
        ctrlRscCrtApiHelper.createResourceDb(
            nodeName.displayValue,
            rscName.displayValue,
            flags,
            rscProps,
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            Collections.emptyList(),
            Resource.DiskfulBy.USER,
            false
        );
        commitAndCleanUp(true);
        Resource rsc = getRsc(nodeName);
        assertThat(rsc).isNotNull();
        return rsc;
    }

    private void createEbsInitiatorDirectly(NodeName nodeName) throws Exception
    {
        createRscDirectly(nodeName, Resource.Flags.EBS_INITIATOR.flagValue, ebsInitPoolName);
    }

    /*
     * call + assertions
     */

    private ApiCallRc makeAvailable(NodeName nodeName, boolean diskful)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        rscMakeAvailableApiCallHandlerProvider.get().makeResourceAvailable(
            nodeName.displayValue,
            rscName.displayValue,
            new ArrayList<>(),
            diskful,
            null,
            false,
            Collections.emptyList(),
            false
        )
            .contextWrite(contextWrite())
            .toStream()
            .forEach(apiCallRc::addEntries);
        return apiCallRc;
    }

    private void assertNoError(ApiCallRc rc)
    {
        List<String> errors = new ArrayList<>();
        for (RcEntry entry : rc)
        {
            if ((entry.getReturnCode() & ApiConsts.MASK_ERROR) == ApiConsts.MASK_ERROR)
            {
                errors.add(entry.getMessage());
            }
        }
        assertThat(errors).as("make-available reported errors").isEmpty();
    }

    private void assertContainsRc(ApiCallRc rc, long expectedRc)
    {
        List<String> entries = new ArrayList<>();
        boolean found = false;
        for (RcEntry entry : rc)
        {
            long code = entry.getReturnCode() & ~ApiConsts.MASK_BITS_OBJ & ~ApiConsts.MASK_BITS_OP;
            entries.add(
                (code & ~ApiConsts.MASK_BITS_TYPE) + ": " + entry.getMessage() +
                    " | cause: " + entry.getCause() + " | details: " + entry.getDetails()
            );
            found |= code == expectedRc;
        }
        assertThat(found)
            .as("expected rc %d in:%n%s", expectedRc & ~ApiConsts.MASK_BITS_TYPE, String.join("\n", entries))
            .isTrue();
    }

    private @Nullable Resource getRsc(NodeName nodeName)
    {
        Node node = nodesMap.get(nodeName);
        return node == null ? null : node.getResource(rscName);
    }

    private Volume getVolume(Resource rsc)
    {
        Volume vlm = rsc.getVolume(vlmNr);
        assertThat(vlm).isNotNull();
        return vlm;
    }

    private DrbdRscData<Resource> getDrbdRscData(Resource rsc)
    {
        @Nullable DrbdRscData<Resource> drbdRscData = getDrbdRscDataOrNull(rsc);
        assertThat(drbdRscData).isNotNull();
        return drbdRscData;
    }

    /**
     * Returns the casted given resource's root layer-data if it is an instance of {@link DrbdRscData}, otherwise
     * {@code null}.
     */
    @SuppressWarnings("unchecked")
    private @Nullable DrbdRscData<Resource> getDrbdRscDataOrNull(Resource rsc)
    {
        @Nullable DrbdRscData<Resource> ret = null;
        AbsRscLayerObject<Resource> root = rsc.getLayerData();
        if (root instanceof DrbdRscData drbdRscData)
        {
            ret = drbdRscData;
        }
        return ret;
    }

    private String describe(Resource rsc)
    {
        @Nullable DrbdRscData<Resource> drbdRscData = getDrbdRscDataOrNull(rsc);

        return rsc.getNode().getName() + ": rsc flags " + rsc.getStateFlags().getFlagsBits() +
            " (EBS_INITIATOR=" + rsc.getStateFlags().isSet(Resource.Flags.EBS_INITIATOR) +
            ", DRBD_DISKLESS=" + rsc.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS) +
            "), drbd CLIENT=" + drbdRscData == null ?
                "NOT DRBD" :
                drbdRscData.getFlags().isSet(DrbdRscFlags.CLIENT) +
            ", pool=" + rsc.getProps().getProp(ApiConsts.KEY_STOR_POOL_NAME);
    }

    private void assertEbsInitiator(NodeName nodeName, StorPoolName expectedPool, String expectedEbsVlmId)
    {
        Resource rsc = getRsc(nodeName);
        assertThat(rsc).as("resource on " + nodeName).isNotNull();
        String details = describe(rsc);
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.EBS_INITIATOR))
            .as("first make-available in an AZ must create the EBS initiator; " + details).isTrue();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS))
            .as("the EBS initiator is the DRBD diskful peer (GitHub issue #513); " + details)
            .isFalse();
        assertThat(getDrbdRscData(rsc).getFlags().isSet(DrbdRscFlags.CLIENT))
            .as("the EBS initiator must not carry the DRBD client role; " + details).isFalse();
        assertThat(rsc.getProps().getProp(ApiConsts.KEY_STOR_POOL_NAME))
            .as("initiator storage pool; " + details).isEqualTo(expectedPool.displayValue);
        assertThat(getVolume(rsc).getProps().getProp(InternalApiConsts.KEY_EBS_VLM_ID, EBS_NAMESPC))
            .as("initiator must have taken over the target's volume id; " + details).isEqualTo(expectedEbsVlmId);
    }

    private void assertDrbdClient(NodeName nodeName, StorPoolName expectedPool)
    {
        Resource rsc = getRsc(nodeName);
        assertThat(rsc).as("resource on " + nodeName).isNotNull();
        String details = describe(rsc);
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.EBS_INITIATOR))
            .as("an AZ has only one EBS initiator; " + details).isFalse();
        assertThat(rsc.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS))
            .as("further make-available in an AZ must be DRBD diskless; " + details).isTrue();
        assertThat(getDrbdRscData(rsc).getFlags().isSet(DrbdRscFlags.CLIENT))
            .as("make-available diskless resources are DRBD clients; " + details).isTrue();
        assertThat(rsc.getProps().getProp(ApiConsts.KEY_STOR_POOL_NAME))
            .as("a DRBD client must use the diskless pool, not the EBS_INIT pool; " + details)
            .isEqualTo(expectedPool.displayValue);
    }

    private void assertConnectedInitiator(Resource targetRsc, NodeName expectedInitiator)
    {
        assertThat(targetRsc.getProps().getProp(InternalApiConsts.KEY_EBS_CONNECTED_INIT_NODE_NAME, EBS_NAMESPC))
            .as("target " + targetRsc.getNode().getName() + " must be claimed by its AZ's initiator")
            .isEqualTo(expectedInitiator.displayValue);
    }
}

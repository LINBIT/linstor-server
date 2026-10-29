package com.linbit.linstor.layer.drbd;

import com.linbit.drbd.DrbdVersion;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdEventPublisher;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdStateStore;
import com.linbit.linstor.layer.drbd.resfiles.DrbdResourceFileUtils;
import com.linbit.linstor.layer.drbd.utils.DrbdAdm;
import com.linbit.linstor.layer.drbd.utils.DrbdAdm.CbtStatus;
import com.linbit.linstor.layer.drbd.utils.WindowsFirewall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * A CBT snapshot runs {@code cbt-flush} before and {@code cbt-start} after the snapshot below DRBD is taken, within
 * the I/O suspension of the second round trip, and only once per volume.
 */
@SuppressWarnings("unchecked")
public class DrbdLayerCbtSnapshotTest
{
    private static final String BOOKMARK_ID = "0123456789ABCDEF";

    private DrbdAdm drbdAdm;
    private DeviceHandler deviceHandler;
    private DrbdLayer drbdLayer;

    private StateFlags<SnapshotDefinition.Flags> snapDfnFlags;
    private Snapshot snap;
    private AbsRscLayerObject<Snapshot> snapDrbd;
    private AbsRscLayerObject<Snapshot> snapStorage;

    private StateFlags<Resource.Flags> rscFlags;
    private DrbdRscData<Resource> rscDrbd;
    private DrbdVlmData<Resource> drbdVlm;

    private ApiCallRcImpl apiCallRc;

    @Before
    public void setUp() throws Exception
    {
        drbdAdm = mock(DrbdAdm.class);
        deviceHandler = mock(DeviceHandler.class);
        drbdLayer = new DrbdLayer(
            drbdAdm,
            mock(DrbdResourceFileUtils.class),
            mock(DrbdStateStore.class),
            mock(DrbdEventPublisher.class),
            mock(ErrorReporter.class),
            mock(CtrlStltSerializer.class),
            mock(ControllerPeerConnector.class),
            () -> deviceHandler,
            mock(ExtCmdFactory.class),
            mock(StltConfigAccessor.class),
            mock(DrbdVersion.class),
            mock(WindowsFirewall.class)
        );
        VolumeNumber vlmNr = new VolumeNumber(0);

        // live resource: DRBD, suspended (round trip 2)
        NodeName nodeName = mock(NodeName.class);
        Resource rsc = mock(Resource.class);
        rscFlags = mock(StateFlags.class);
        when(rsc.getStateFlags()).thenReturn(rscFlags);
        rscDrbd = mock(DrbdRscData.class);
        when(rscDrbd.getLayerKind()).thenReturn(DeviceLayerKind.DRBD);
        when(rscDrbd.getResourceNameSuffix()).thenReturn(RscLayerSuffixes.SUFFIX_DATA);
        when(rscDrbd.exists()).thenReturn(true);
        when(rscDrbd.getShouldSuspendIo()).thenReturn(true);
        when(rscDrbd.getSuffixedResourceName()).thenReturn("rsc");
        drbdVlm = mock(DrbdVlmData.class);
        when(drbdVlm.getVlmNr()).thenReturn(vlmNr);
        when(drbdVlm.getRscLayerObject()).thenReturn(rscDrbd);
        when(rscDrbd.getVlmLayerObjects()).thenReturn(Map.of(vlmNr, drbdVlm));
        when(rsc.getLayerData()).thenReturn(rscDrbd);
        ResourceDefinition rscDfn = mock(ResourceDefinition.class);
        when(rscDfn.getResource(nodeName)).thenReturn(rsc);

        // CBT snapshot to be taken: DRBD -> STORAGE
        SnapshotDefinition snapDfn = mock(SnapshotDefinition.class);
        snapDfnFlags = mock(StateFlags.class);
        when(snapDfnFlags.isSet(SnapshotDefinition.Flags.CBT)).thenReturn(true);
        when(snapDfn.getFlags()).thenReturn(snapDfnFlags);
        Props snapDfnProps = mock(Props.class);
        when(snapDfnProps.getProp(ApiConsts.KEY_CBT_ID, ApiConsts.NAMESPC_CBT)).thenReturn(BOOKMARK_ID);
        when(snapDfn.getSnapDfnProps()).thenReturn(snapDfnProps);
        snap = mock(Snapshot.class);
        StateFlags<Snapshot.Flags> snapFlags = mock(StateFlags.class);
        when(snapFlags.isUnset(Snapshot.Flags.DELETE)).thenReturn(true);
        when(snap.getFlags()).thenReturn(snapFlags);
        when(snap.getTakeSnapshot()).thenReturn(true);
        when(snap.getNodeName()).thenReturn(nodeName);
        when(snap.getResourceDefinition()).thenReturn(rscDfn);
        when(snap.getSnapshotDefinition()).thenReturn(snapDfn);
        snapDrbd = mock(AbsRscLayerObject.class);
        when(snapDrbd.getAbsResource()).thenReturn(snap);
        when(snapDrbd.getResourceNameSuffix()).thenReturn(RscLayerSuffixes.SUFFIX_DATA);
        when(snapDrbd.getVlmLayerObjects())
            .thenReturn(Map.<VolumeNumber, VlmProviderObject<Snapshot>>of(vlmNr, mock(VlmProviderObject.class)));
        snapStorage = mock(AbsRscLayerObject.class);
        when(snapDrbd.getChildren()).thenReturn(Set.of(snapStorage));

        when(drbdAdm.cbtStatus(drbdVlm)).thenReturn(new CbtStatus(null, CbtStatus.STATE_STOPPED));

        apiCallRc = new ApiCallRcImpl();
    }

    @Test
    public void flushBeforeAndStartAfterTheSnapshotBelow() throws Exception
    {
        assertThat(drbdLayer.processSnapshot(snapDrbd, apiCallRc)).isTrue();

        InOrder order = inOrder(drbdAdm, deviceHandler);
        order.verify(drbdAdm).cbtFlush(drbdVlm);
        order.verify(deviceHandler).processSnapshot(snapStorage, apiCallRc);
        order.verify(drbdAdm).cbtStart(drbdVlm, BOOKMARK_ID);
    }

    @Test
    public void startsAfterAnIntervalOfAnotherSnapshot() throws Exception
    {
        when(drbdAdm.cbtStatus(drbdVlm)).thenReturn(new CbtStatus("FEDCBA9876543210", "valid"));

        drbdLayer.processSnapshot(snapDrbd, apiCallRc);

        verify(drbdAdm).cbtFlush(drbdVlm);
        verify(drbdAdm).cbtStart(drbdVlm, BOOKMARK_ID);
    }

    @Test
    public void volumeWithStartedIntervalIsDone() throws Exception
    {
        // takeSnapshot stays set until the last round trip: the volume already started the interval of this snapshot
        when(drbdAdm.cbtStatus(drbdVlm)).thenReturn(new CbtStatus(BOOKMARK_ID, "valid"));

        assertThat(drbdLayer.processSnapshot(snapDrbd, apiCallRc)).isTrue();

        verify(drbdAdm, never()).cbtFlush(any());
        verify(drbdAdm, never()).cbtStart(any(), anyString());
        verify(deviceHandler).processSnapshot(snapStorage, apiCallRc);
    }

    @Test
    public void nonCbtSnapshotIsLeftToTheDeviceHandler() throws Exception
    {
        when(snapDfnFlags.isSet(SnapshotDefinition.Flags.CBT)).thenReturn(false);

        assertThat(drbdLayer.processSnapshot(snapDrbd, apiCallRc)).isFalse();

        verifyNoInteractions(drbdAdm, deviceHandler);
    }

    @Test
    public void nothingBeforeTheSnapshotIsToBeTaken() throws Exception
    {
        // round trip 1: suspending
        when(snap.getTakeSnapshot()).thenReturn(false);

        assertThat(drbdLayer.processSnapshot(snapDrbd, apiCallRc)).isFalse();

        verifyNoInteractions(drbdAdm, deviceHandler);
    }

    @Test
    public void nothingAfterResume() throws Exception
    {
        // round trip 3: resumed, takeSnapshot is still set
        when(rscDrbd.getShouldSuspendIo()).thenReturn(false);

        assertThat(drbdLayer.processSnapshot(snapDrbd, apiCallRc)).isFalse();

        verifyNoInteractions(drbdAdm, deviceHandler);
    }

    @Test
    public void nothingForInactiveResource() throws Exception
    {
        when(
            rscFlags.isSomeSet(
                Resource.Flags.INACTIVE,
                Resource.Flags.INACTIVE_PERMANENTLY,
                Resource.Flags.INACTIVATING
            )
        ).thenReturn(true);

        assertThat(drbdLayer.processSnapshot(snapDrbd, apiCallRc)).isFalse();

        verifyNoInteractions(drbdAdm, deviceHandler);
    }

    @Test
    public void refusedFlushFailsWithoutTakingTheSnapshot() throws Exception
    {
        doThrow(cmdFailed("resync running")).when(drbdAdm).cbtFlush(drbdVlm);

        assertThatThrownBy(() -> drbdLayer.processSnapshot(snapDrbd, apiCallRc))
            .isInstanceOfSatisfying(
                StorageException.class,
                exc -> assertThat(exc.getNumericCode()).isEqualTo(ApiConsts.FAIL_CBT_START)
            );
        verifyNoInteractions(deviceHandler);
        verify(drbdAdm, never()).cbtStart(any(), anyString());
    }

    @Test
    public void refusedStartFails() throws Exception
    {
        doThrow(cmdFailed("no free bitmap slot")).when(drbdAdm).cbtStart(drbdVlm, BOOKMARK_ID);

        assertThatThrownBy(() -> drbdLayer.processSnapshot(snapDrbd, apiCallRc))
            .isInstanceOfSatisfying(
                StorageException.class,
                exc -> assertThat(exc.getNumericCode()).isEqualTo(ApiConsts.FAIL_CBT_START)
            );
    }

    @Test
    public void failingStatusMeansNotSupported() throws Exception
    {
        when(drbdAdm.cbtStatus(drbdVlm)).thenThrow(cmdFailed("unknown command"));

        assertThatThrownBy(() -> drbdLayer.processSnapshot(snapDrbd, apiCallRc))
            .isInstanceOfSatisfying(
                StorageException.class,
                exc -> assertThat(exc.getNumericCode()).isEqualTo(ApiConsts.FAIL_CBT_NOT_SUPPORTED)
            );
        verifyNoInteractions(deviceHandler);
    }

    private static ExtCmdFailedException cmdFailed(String reason)
    {
        return new ExtCmdFailedException(new String[] {"drbdsetup"}, new IOException(reason));
    }
}

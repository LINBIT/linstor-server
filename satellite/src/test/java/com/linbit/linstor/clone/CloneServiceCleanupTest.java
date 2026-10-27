package com.linbit.linstor.clone;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.DeviceHandler.CloneStrategy;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * What the clone service resets when a clone is over: a source snapshot and a target with a layer above the storage
 * layer (e.g. DRBD), which the clone passed through.
 */
@SuppressWarnings("unchecked")
public class CloneServiceCleanupTest
{
    /** the argument of the constructor of {@link CloneDaemon} that is called when the clone is over */
    private static final int CLONE_DAEMON_ARG_AFTER_CLONE = 4;

    private DeviceHandler deviceHandler;
    private CloneService cloneService;
    private VolumeNumber vlmNr;

    private AbsRscLayerObject<Snapshot> srcRootRscData;
    private StorageRscData<Snapshot> srcStorRscData;
    private LvmThinData<Snapshot> srcVlmData;
    private AbsRscLayerObject<Resource> tgtRootRscData;
    private StorageRscData<Resource> tgtStorRscData;
    private LvmThinData<Resource> tgtVlmData;

    @Before
    public void setUp() throws Exception
    {
        vlmNr = new VolumeNumber(0);
        deviceHandler = mock(DeviceHandler.class);
        cloneService = new CloneService(
            mock(ErrorReporter.class),
            mock(ControllerPeerConnector.class, Answers.RETURNS_DEEP_STUBS),
            mock(CtrlStltSerializer.class, Answers.RETURNS_DEEP_STUBS),
            mock(VolumeDiskStateEvent.class, Answers.RETURNS_DEEP_STUBS),
            mock(ExtCmdFactory.class),
            mock(StltConfigAccessor.class),
            new ReentrantReadWriteLock(),
            () -> deviceHandler
        );
        cloneService.start();

        srcRootRscData = mock(AbsRscLayerObject.class);
        srcStorRscData = mock(StorageRscData.class);
        srcVlmData = mock(LvmThinData.class);
        initLayerData(srcRootRscData, srcStorRscData, srcVlmData, "base", "base_00000_CF_1a2b3c4d");

        tgtRootRscData = mock(AbsRscLayerObject.class);
        tgtStorRscData = mock(StorageRscData.class);
        tgtVlmData = mock(LvmThinData.class);
        initLayerData(tgtRootRscData, tgtStorRscData, tgtVlmData, "myclone", "myclone_00000");
        when(tgtVlmData.getVolumeGroup()).thenReturn("scratch");
        when(tgtVlmData.getProviderKind()).thenReturn(DeviceProviderKind.LVM_THIN);
        when(tgtStorRscData.getVlmLayerObjects()).thenReturn(Map.of(vlmNr, tgtVlmData));
        Resource tgtRsc = mock(Resource.class);
        when(tgtStorRscData.getAbsResource()).thenReturn(tgtRsc);
        when(tgtRsc.getLayerData()).thenReturn(tgtRootRscData);
        when(tgtRootRscData.getVlmProviderObject(vlmNr)).thenReturn(mock(VlmProviderObject.class));
    }

    private <RSC extends AbsResource<RSC>> void initLayerData(
        AbsRscLayerObject<RSC> rootRscData,
        StorageRscData<RSC> storRscData,
        LvmThinData<RSC> vlmData,
        String rscName,
        String identifier
    )
        throws Exception
    {
        when(rootRscData.getLayerKind()).thenReturn(DeviceLayerKind.DRBD);
        when(rootRscData.getChildren()).thenReturn(Set.of(storRscData));
        when(storRscData.getParent()).thenReturn(rootRscData);
        when(storRscData.getLayerKind()).thenReturn(DeviceLayerKind.STORAGE);
        when(storRscData.getResourceName()).thenReturn(new ResourceName(rscName));
        when(storRscData.getResourceNameSuffix()).thenReturn("");
        when(vlmData.getRscLayerObject()).thenReturn(storRscData);
        when(vlmData.getVlmNr()).thenReturn(vlmNr);
        when(vlmData.getIdentifier()).thenReturn(identifier);
    }

    /** Runs a clone of the source into the target until it is over */
    private void cloneUntilOver() throws Exception
    {
        AtomicReference<Consumer<Boolean>> afterClone = new AtomicReference<>();
        try (
            MockedStatic<LvmUtils> ignoredLvmUtils = mockStatic(LvmUtils.class);
            MockedStatic<LvmCommands> ignoredLvmCommands = mockStatic(LvmCommands.class);
            MockedConstruction<CloneDaemon> ignoredDaemons = mockConstruction(
                CloneDaemon.class,
                (daemon, context) -> afterClone.set(
                    (Consumer<Boolean>) context.arguments().get(CLONE_DAEMON_ARG_AFTER_CLONE)
                )
            )
        )
        {
            cloneService.startClone(srcVlmData, tgtVlmData, CloneStrategy.LVM_THIN_CLONE, null);
            afterClone.get().accept(true);
        }
    }

    private void verifyClonePassthroughModeRemoved()
    {
        // keyed by the target's layer data, also on the layers above the one the clone was done on
        for (AbsRscLayerObject<Resource> tgtRscData : Set.of(tgtRootRscData, tgtStorRscData))
        {
            verify(srcRootRscData).removeClonePassthroughMode(tgtRscData);
            verify(srcStorRscData).removeClonePassthroughMode(tgtRscData);
        }
        verify(tgtRootRscData).removeClonePassthroughMode(tgtRootRscData);
        verify(tgtStorRscData).removeClonePassthroughMode(tgtStorRscData);
    }

    @Test
    public void clonePassthroughModeRemovedFromAllLayers() throws Exception
    {
        cloneUntilOver();

        verify(deviceHandler).processAfterClone(any(), any(), any());
        verify(deviceHandler).closeCloneTarget(tgtVlmData);
        verifyClonePassthroughModeRemoved();
    }

    @Test
    public void clonePassthroughModeRemovedIfProcessingAfterCloneFails() throws Exception
    {
        doThrow(new StorageException("drbdmeta check-resize failed"))
            .when(deviceHandler).processAfterClone(any(), any(), any());

        cloneUntilOver();

        verify(deviceHandler).closeCloneTarget(tgtVlmData);
        verifyClonePassthroughModeRemoved();
    }

    @Test
    public void clonePassthroughModeRemovedIfClosingFails() throws Exception
    {
        doThrow(new StorageException("close failed")).when(deviceHandler).closeCloneTarget(tgtVlmData);

        cloneUntilOver();

        verifyClonePassthroughModeRemoved();
    }
}

package com.linbit.linstor.clone;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.DeviceHandler.CloneStrategy;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.layer.ReadOnlySnapshotHandle;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.utils.ExceptionThrowingFunction;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * The clone source is a LINSTOR snapshot: the snapshot-based strategies work on the storage identifier of that
 * snapshot, never on the live source volume.
 */
@SuppressWarnings("unchecked")
public class CloneServiceSnapshotSourceTest
{
    private static final String VG = "scratch";
    private static final String ZPOOL = "tank";
    private static final String SRC_LV = "base_00000";
    private static final String TGT_LV = "myclone_00000";
    private static final String TGT_RSC_NAME = "myclone";

    private CloneService cloneService;
    private StorageRscData<Snapshot> srcRscData;
    private StorageRscData<Resource> tgtRscData;
    private VolumeNumber vlmNr;

    @Before
    public void setUp() throws Exception
    {
        vlmNr = new VolumeNumber(0);
        srcRscData = mock(StorageRscData.class);
        when(srcRscData.getResourceName()).thenReturn(new ResourceName("base"));
        when(srcRscData.getResourceNameSuffix()).thenReturn("");
        tgtRscData = mock(StorageRscData.class);
        when(tgtRscData.getResourceName()).thenReturn(new ResourceName(TGT_RSC_NAME));
        when(tgtRscData.getResourceNameSuffix()).thenReturn("");

        cloneService = new CloneService(
            mock(ErrorReporter.class),
            mock(ControllerPeerConnector.class),
            mock(CtrlStltSerializer.class),
            mock(VolumeDiskStateEvent.class, Answers.RETURNS_DEEP_STUBS),
            mock(ExtCmdFactory.class),
            mock(StltConfigAccessor.class),
            new ReentrantReadWriteLock(),
            () -> mock(DeviceHandler.class)
        );
        cloneService.start();
    }

    private <T extends com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject<?>> T init(
        T vlmData,
        StorageRscData<?> rscData,
        String identifier
    )
    {
        when(vlmData.getIdentifier()).thenReturn(identifier);
        when(vlmData.getVlmNr()).thenReturn(vlmNr);
        when(vlmData.getRscLayerObject()).thenReturn((StorageRscData) rscData);
        return vlmData;
    }

    @Test
    public void lvmThinCloneUsesSourceSnapshot() throws Exception
    {
        LvmThinData<Snapshot> srcData = init(mock(LvmThinData.class), srcRscData, SRC_LV + "_CF_" + TGT_RSC_NAME);
        LvmThinData<Resource> tgtData = init(mock(LvmThinData.class), tgtRscData, TGT_LV);
        when(tgtData.getVolumeGroup()).thenReturn(VG);

        try (
            MockedStatic<LvmUtils> lvmUtils = mockStatic(LvmUtils.class);
            MockedStatic<LvmCommands> lvmCommands = mockStatic(LvmCommands.class);
            // the strategy runs synchronously, the daemon would only report the result to the controller
            MockedConstruction<CloneDaemon> ignored = mockConstruction(CloneDaemon.class)
        )
        {
            lvmUtils.when(() -> LvmUtils.execWithRetry(any(), any(), any())).thenAnswer(
                inv -> ((ExceptionThrowingFunction<String, ?, ?>) inv.getArgument(2)).accept("cfg")
            );

            cloneService.startClone(srcData, tgtData, CloneStrategy.LVM_THIN_CLONE, null);

            lvmCommands.verify(
                () -> LvmCommands.restoreFromSnapshot(
                    any(),
                    eq(SRC_LV + "_CF_" + TGT_RSC_NAME),
                    eq(VG),
                    eq(TGT_LV),
                    eq("cfg")
                )
            );
            // never a thin snapshot of the live source volume
            lvmCommands.verify(
                () -> LvmCommands.restoreFromSnapshot(any(), eq(SRC_LV), anyString(), anyString(), anyString()),
                never()
            );
            lvmCommands.verify(() -> LvmCommands.activateVolume(any(), eq(VG), eq(TGT_LV), eq("cfg")));
        }
    }

    @Test
    public void zfsCloneUsesSourceSnapshot() throws Exception
    {
        ZfsData<Snapshot> srcData = init(mock(ZfsData.class), srcRscData, SRC_LV + "@CF_" + TGT_RSC_NAME);
        ZfsData<Resource> tgtData = init(mock(ZfsData.class), tgtRscData, TGT_LV);
        when(tgtData.getZPool()).thenReturn(ZPOOL);

        try (
            MockedStatic<ZfsCommands> zfsCommands = mockStatic(ZfsCommands.class);
            MockedConstruction<CloneDaemon> ignored = mockConstruction(CloneDaemon.class)
        )
        {
            cloneService.startClone(srcData, tgtData, CloneStrategy.ZFS_CLONE, null);

            zfsCommands.verify(
                () -> ZfsCommands.restoreSnapshot(any(), eq(ZPOOL), eq(SRC_LV + "@CF_" + TGT_RSC_NAME), eq(TGT_LV))
            );
        }
    }

    @Test
    public void unpreparedSourceSnapshotFails() throws Exception
    {
        // the storage identifier is set when the satellite prepared the snapshot
        ZfsData<Snapshot> srcData = init(mock(ZfsData.class), srcRscData, null);
        ZfsData<Resource> tgtData = init(mock(ZfsData.class), tgtRscData, TGT_LV);

        // ZFS_COPY needs open devices, i.e. the opened source snapshot
        ReadOnlySnapshotHandle srcSnapshotHandle = mock(ReadOnlySnapshotHandle.class);

        try (MockedConstruction<CloneDaemon> ignored = mockConstruction(CloneDaemon.class))
        {
            assertThatThrownBy(
                () -> cloneService.startClone(srcData, tgtData, CloneStrategy.ZFS_COPY, srcSnapshotHandle)
            )
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("storage identifier");
        }
    }
}

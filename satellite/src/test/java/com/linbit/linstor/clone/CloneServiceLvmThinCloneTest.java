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
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.utils.ExceptionThrowingFunction;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * The LVM_THIN_CLONE strategy has to clone from the clone snapshot that was taken while the source was suspended. The
 * target is only created after the source was resumed, so a snapshot of the live source volume would contain later
 * writes.
 */
@SuppressWarnings("unchecked")
public class CloneServiceLvmThinCloneTest
{
    private static final String VG = "scratch";
    private static final String SRC_LV = "base_00000";
    private static final String TGT_LV = "myclone_00000";
    private static final String TGT_RSC_NAME = "myclone";

    @Test
    public void lvmThinCloneUsesCloneSnapshot() throws Exception
    {
        VolumeNumber vlmNr = new VolumeNumber(0);

        StorageRscData<Resource> srcRscData = mock(StorageRscData.class);
        when(srcRscData.getResourceName()).thenReturn(new ResourceName("base"));
        when(srcRscData.getResourceNameSuffix()).thenReturn("");
        LvmThinData<Resource> srcData = mock(LvmThinData.class);
        when(srcData.getIdentifier()).thenReturn(SRC_LV);
        when(srcData.getVolumeGroup()).thenReturn(VG);
        when(srcData.getVlmNr()).thenReturn(vlmNr);
        when(srcData.getRscLayerObject()).thenReturn(srcRscData);

        StorageRscData<Resource> tgtRscData = mock(StorageRscData.class);
        when(tgtRscData.getResourceName()).thenReturn(new ResourceName(TGT_RSC_NAME));
        when(tgtRscData.getResourceNameSuffix()).thenReturn("");
        LvmThinData<Resource> tgtData = mock(LvmThinData.class);
        when(tgtData.getIdentifier()).thenReturn(TGT_LV);
        when(tgtData.getVolumeGroup()).thenReturn(VG);
        when(tgtData.getVlmNr()).thenReturn(vlmNr);
        when(tgtData.getRscLayerObject()).thenReturn(tgtRscData);

        CloneService cloneService = new CloneService(
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

            String cloneSnapshot = AbsStorageProvider.getLVMCloneSnapshotNameFull(SRC_LV, TGT_RSC_NAME, 0);
            lvmCommands.verify(
                () -> LvmCommands.restoreFromSnapshot(any(), eq(cloneSnapshot), eq(VG), eq(TGT_LV), eq("cfg"))
            );
            lvmCommands.verify(
                () -> LvmCommands.restoreFromSnapshot(any(), eq(SRC_LV), anyString(), anyString(), anyString()),
                never()
            );
            lvmCommands.verify(() -> LvmCommands.activateVolume(any(), eq(VG), eq(TGT_LV), eq("cfg")));
        }
    }
}

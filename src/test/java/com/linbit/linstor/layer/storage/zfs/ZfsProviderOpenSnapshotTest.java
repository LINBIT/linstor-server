package com.linbit.linstor.layer.storage.zfs;

import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.ReadOnlyOpener;
import com.linbit.linstor.layer.ReadOnlySnapshotHandle;
import com.linbit.linstor.layer.drbd.DrbdInvalidateUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider.AbsStorageProviderInit;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.utils.DeviceUtils;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.layer.storage.DeviceProvider;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link ZfsProvider#openSnapshotReadOnly}: the snapshot becomes a block device via {@code snapdev=visible} on its
 * zvol, also for snapshots of renamed zvols of deleted resources.
 */
public class ZfsProviderOpenSnapshotTest extends GenericDbBase
{
    private static final String NODE_NAME_STR = "node";
    private static final String RSC_NAME_STR = "rsc";
    private static final String SNAP_NAME_STR = "snap";
    private static final String SP_NAME_STR = "zfsSp";
    private static final String ZPOOL = "tank";
    private static final String RSC_ZVOL_ID = "rsc_00000";
    private static final long VLM_SIZE_IN_KIB = 4096L;

    private TestExtCmd extCmd;
    private ZfsProvider zfsProvider;
    private MockedStatic<DeviceUtils> deviceUtils;

    private Resource rsc;
    private Volume vlm;
    private Snapshot snap;
    private VlmProviderObject<Snapshot> snapVlmData;

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();

        extCmd = new TestExtCmd(errorReporter);

        ExtCmdFactoryStlt extCmdFactoryStlt = Mockito.mock(ExtCmdFactoryStlt.class);
        Mockito.when(extCmdFactoryStlt.create()).thenReturn(extCmd);

        StltConfigAccessor stltCfgAccessor = Mockito.mock(StltConfigAccessor.class);
        Mockito.when(stltCfgAccessor.getReadonlyProps()).thenReturn(ReadOnlyPropsImpl.emptyRoProps());

        NotificationListener notificationListener = Mockito.mock(NotificationListener.class);
        zfsProvider = new ZfsProvider(
            new AbsStorageProviderInit(
                errorReporter,
                extCmdFactoryStlt,
                stltCfgAccessor,
                Mockito.mock(WipeHandler.class),
                () -> notificationListener,
                transMgrProvider::get,
                Mockito.mock(StltExtToolsChecker.class),
                Mockito.mock(CloneService.class),
                Mockito.mock(BackupShippingMgr.class),
                Mockito.mock(FileSystemWatch.class),
                rscDfnMap,
                Mockito.mock(DrbdInvalidateUtils.class),
                remoteMap,
                Mockito.mock(ControllerPeerConnector.class)
            )
        );
        // the device nodes of the test do not exist
        deviceUtils = Mockito.mockStatic(DeviceUtils.class);

        createScenario();
    }

    @After
    public void closeDeviceUtilsMock()
    {
        deviceUtils.close();
    }

    private void createScenario() throws Exception
    {
        rsc = resourceTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR)
            .setLayerStack(Collections.singletonList(DeviceLayerKind.STORAGE))
            .build();
        StorPool storPool = storPoolTestFactory
            .builder(NODE_NAME_STR, SP_NAME_STR)
            .setDriverKind(DeviceProviderKind.ZFS)
            .build();
        storPool.getProps().setProp(
            StorageConstants.CONFIG_ZFS_POOL_KEY,
            ZPOOL,
            ApiConsts.NAMESPC_STORAGE_DRIVER
        );
        vlm = volumeTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR, 0)
            .setSize(VLM_SIZE_IN_KIB)
            .setStorPoolData(storPool)
            .build();

        snap = createSnapshot(SNAP_NAME_STR);
        snapVlmData = snap.getLayerData().getVlmProviderObject(new VolumeNumber(0));
    }

    private Snapshot createSnapshot(String snapName) throws Exception
    {
        SnapshotDefinition snapDfn = snapshotDefinitionFactory.create(
            rsc.getResourceDefinition(),
            new SnapshotName(snapName),
            new SnapshotDefinition.Flags[0]
        );
        SnapshotVolumeDefinition snapVlmDfn = snapshotVolumeDefinitionFactory.create(
            snapDfn,
            vlm.getVolumeDefinition(),
            VLM_SIZE_IN_KIB,
            new SnapshotVolumeDefinition.Flags[0]
        );
        Snapshot ret = snapshotFactory.create(rsc, snapDfn, new Snapshot.Flags[0]);
        snapshotVolumeFactory.create(rsc, ret, snapVlmDfn);
        return ret;
    }

    @Test
    public void openLinstorSnapshotMakesItVisible() throws Exception
    {
        expect(snapdevCommand(RSC_ZVOL_ID, "visible"));

        zfsProvider.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapVlmData, "_test"));

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        String devPath = "/dev/zvol/" + ZPOOL + "/" + RSC_ZVOL_ID + "@" + SNAP_NAME_STR;
        assertThat(snapVlmData.getCloneDevicePath()).isEqualTo(devPath);
        // udev creates the device asynchronously after snapdev became visible
        deviceUtils.verify(
            () -> DeviceUtils.waitUntilDeviceVisible(
                ArgumentMatchers.eq(devPath),
                ArgumentMatchers.anyLong(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
            )
        );
    }

    @Test
    public void closeLinstorSnapshotHidesIt() throws Exception
    {
        expect(snapdevCommand(RSC_ZVOL_ID, "visible"));
        ReadOnlySnapshotHandle handle = zfsProvider.openSnapshotReadOnly(
            ReadOnlyOpener.forSnapshot(snapVlmData, "_test")
        );

        expect(snapdevCommand(RSC_ZVOL_ID, "hidden"));
        handle.close();

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(snapVlmData.getCloneDevicePath()).isNull();
    }

    @Test
    public void snapshotsOfSameZvolAreHiddenOnlyByLastClose() throws Exception
    {
        // snapdev is a property of the zvol: all snapshots of the zvol share it
        VlmProviderObject<Snapshot> otherSnapVlmData = createSnapshot("other").getLayerData()
            .getVlmProviderObject(new VolumeNumber(0));
        expect(snapdevCommand(RSC_ZVOL_ID, "visible"));
        ReadOnlySnapshotHandle snapHandle = zfsProvider.openSnapshotReadOnly(
            ReadOnlyOpener.forSnapshot(snapVlmData, "_test")
        );
        ReadOnlySnapshotHandle otherHandle = zfsProvider.openSnapshotReadOnly(
            ReadOnlyOpener.forSnapshot(otherSnapVlmData, "_test")
        );
        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(otherSnapVlmData.getCloneDevicePath()).isEqualTo(
            "/dev/zvol/" + ZPOOL + "/" + RSC_ZVOL_ID + "@other"
        );

        // TestExtCmd fails on any unexpected command, i.e. also on a premature "snapdev=hidden"
        snapHandle.close();
        assertThat(otherSnapVlmData.getCloneDevicePath()).isNotNull();

        expect(snapdevCommand(RSC_ZVOL_ID, "hidden"));
        otherHandle.close();
        assertThat(extCmd.getUncalledCommands()).isEmpty();
    }

    @Test
    public void failedOpenHidesAgain() throws Exception
    {
        StorageException timeout = new StorageException("device did not show up");
        deviceUtils.when(
            () -> DeviceUtils.waitUntilDeviceVisible(
                ArgumentMatchers.any(),
                ArgumentMatchers.anyLong(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
            )
        ).thenThrow(timeout);
        expect(snapdevCommand(RSC_ZVOL_ID, "visible"));
        expect(snapdevCommand(RSC_ZVOL_ID, "hidden"));

        assertThatThrownBy(() -> zfsProvider.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapVlmData, "_test")))
            .isSameAs(timeout);
        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(snapVlmData.getCloneDevicePath()).isNull();
    }

    @Test
    public void openSnapshotOfRenamedZvol() throws Exception
    {
        // the resource was deleted while it still had snapshots: its zvol was renamed
        snap.getSnapProps().setProp(InternalApiConsts.KEY_ZFS_RENAME_SUFFIX, "x", DeviceProvider.STORAGE_NAMESPACE);
        String renamedZvol = "_deleted_" + RSC_ZVOL_ID + "_x";
        expect(snapdevCommand(renamedZvol, "visible"));

        zfsProvider.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapVlmData, "_test"));

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(snapVlmData.getCloneDevicePath()).isEqualTo(
            "/dev/zvol/" + ZPOOL + "/" + renamedZvol + "@" + SNAP_NAME_STR
        );
    }

    private void expect(String[] argv)
    {
        extCmd.setExpectedBehavior(new Command(argv), new TestOutputData(argv, "", "", 0));
    }

    private static String[] snapdevCommand(String zvol, String value)
    {
        return new String[] {"zfs", "set", "snapdev=" + value, ZPOOL + "/" + zvol};
    }
}

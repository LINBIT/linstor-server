package com.linbit.linstor.layer.storage.lvm;

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
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.AbsStorageProvider.AbsStorageProviderInit;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.utils.DeviceUtils;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.test.factories.StorPoolTestFactory;

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
 * Tests {@link LvmProvider#openSnapshotReadOnly} for LINSTOR snapshots (opened by their snapshot volume data) and
 * for clone snapshots (opened by the resource volume data plus the clone target), including restoring the activation
 * state of a LINSTOR snapshot when its last opener closes, and refusing to activate a LINSTOR snapshot that must not
 * be activated.
 */
public class LvmProviderOpenSnapshotTest extends GenericDbBase
{
    private static final String LVS_COLUMNS =
        "lv_name,lv_path,lv_size,vg_name,pool_lv,data_percent,lv_attr,metadata_percent,chunk_size,stripes,origin," +
            "kernel_discards";
    private static final String LV_ATTR_INACTIVE = "swi---s---";
    private static final String LV_ATTR_ACTIVE = "swi-a-s---";
    private static final String IGNORE_DRBD_CONFIG =
        "devices { ignore_suspended_devices=1 filter=[\"r|^/dev/drbd.*|\"] }";

    private static final String NODE_NAME_STR = "node";
    private static final String RSC_NAME_STR = "rsc";
    private static final String SNAP_NAME_STR = "snap";
    private static final String SP_NAME_STR = "lvmSp";
    private static final String SHARED_SPACE_NAME = "SharedSpace";
    private static final String RSC_LV_ID = "rsc_00000";
    private static final String SNAP_LV_ID = "rsc_00000_snap";
    private static final long VLM_SIZE_IN_KIB = 4096L;

    private TestExtCmd extCmd;
    private LvmProvider lvmProvider;
    private MockedStatic<DeviceUtils> deviceUtils;

    private Resource rsc;
    private Snapshot snap;
    private VlmProviderObject<Resource> rscVlmData;
    private VlmProviderObject<Snapshot> snapVlmData;
    /** Unique per test method since LvmUtils caches the lvm filter config statically per volume group set */
    private String vg;

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();

        extCmd = new TestExtCmd(errorReporter);
        LvmUtils.recacheNext();

        ExtCmdFactoryStlt extCmdFactoryStlt = Mockito.mock(ExtCmdFactoryStlt.class);
        Mockito.when(extCmdFactoryStlt.create()).thenReturn(extCmd);

        StltConfigAccessor stltCfgAccessor = Mockito.mock(StltConfigAccessor.class);
        Mockito.when(stltCfgAccessor.getReadonlyProps()).thenReturn(ReadOnlyPropsImpl.emptyRoProps());

        NotificationListener notificationListener = Mockito.mock(NotificationListener.class);
        lvmProvider = new LvmProvider(
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

        vg = "vg_" + testMethodName.getMethodName();
    }

    @After
    public void closeDeviceUtilsMock()
    {
        deviceUtils.close();
    }

    /**
     * Creates the {@link #RSC_NAME_STR} resource with one volume and a snapshot of it.
     *
     * @param sharedSp whether the backing storage pool belongs to a shared space
     */
    private void createScenario(boolean sharedSp) throws Exception
    {
        rsc = resourceTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR)
            .setLayerStack(Collections.singletonList(DeviceLayerKind.STORAGE))
            .build();
        StorPoolTestFactory.StorPoolBuilder storPoolBuilder = storPoolTestFactory
            .builder(NODE_NAME_STR, SP_NAME_STR)
            .setDriverKind(DeviceProviderKind.LVM);
        if (sharedSp)
        {
            storPoolBuilder.setFreeSpaceMgrName(SHARED_SPACE_NAME);
        }
        StorPool storPool = storPoolBuilder.build();
        storPool.getProps().setProp(
            StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY,
            vg,
            ApiConsts.NAMESPC_STORAGE_DRIVER
        );
        Volume vlm = volumeTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR, 0)
            .setSize(VLM_SIZE_IN_KIB)
            .setStorPoolData(storPool)
            .build();
        rscVlmData = rsc.getLayerData().getVlmProviderObject(new VolumeNumber(0));
        // set by prepare() on a real satellite
        ((LvmData<Resource>) rscVlmData).setVolumeGroup(vg);

        SnapshotDefinition snapDfn = snapshotDefinitionFactory.create(
            rsc.getResourceDefinition(),
            new SnapshotName(SNAP_NAME_STR),
            new SnapshotDefinition.Flags[0]
        );
        SnapshotVolumeDefinition snapVlmDfn = snapshotVolumeDefinitionFactory.create(
            snapDfn,
            vlm.getVolumeDefinition(),
            VLM_SIZE_IN_KIB,
            new SnapshotVolumeDefinition.Flags[0]
        );
        snap = snapshotFactory.create(rsc, snapDfn, new Snapshot.Flags[0]);
        snapshotVolumeFactory.create(rsc, snap, snapVlmDfn);

        snapVlmData = snap.getLayerData().getVlmProviderObject(new VolumeNumber(0));
    }

    @Test
    public void openInactiveSnapshotActivatesItAndDeactivatesOnClose() throws Exception
    {
        createScenario(false);
        // a LINSTOR snapshot might have been deactivated since its creation (reboot, activation skip)
        String lvmConfig = expectLvs(LV_ATTR_INACTIVE);
        expect(lvchangeActivateCommand(lvmConfig, SNAP_LV_ID), "");

        ReadOnlySnapshotHandle handle = lvmProvider.openSnapshotReadOnly(
            ReadOnlyOpener.forSnapshot(snapVlmData, "_test")
        );

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        String devPath = "/dev/" + vg + "/" + SNAP_LV_ID;
        assertThat(snapVlmData.getCloneDevicePath()).isEqualTo(devPath);
        deviceUtils.verify(
            () -> DeviceUtils.waitUntilDeviceVisible(
                ArgumentMatchers.eq(devPath),
                ArgumentMatchers.anyLong(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
            )
        );

        expect(lvchangeDeactivateCommand(lvmConfig, SNAP_LV_ID), "");
        handle.close();

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(snapVlmData.getCloneDevicePath()).isNull();
    }

    @Test
    public void openActiveSnapshotKeepsItActive() throws Exception
    {
        createScenario(false);
        expectLvs(LV_ATTR_ACTIVE);

        // TestExtCmd fails on any unexpected command, i.e. also on "lvchange -ay" and "lvchange -an"
        ReadOnlySnapshotHandle handle = lvmProvider.openSnapshotReadOnly(
            ReadOnlyOpener.forSnapshot(snapVlmData, "_test")
        );
        handle.close();

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(snapVlmData.getCloneDevicePath()).isNull();
    }

    @Test
    public void onlyLastCloseDeactivates() throws Exception
    {
        createScenario(false);
        String lvmConfig = expectLvs(LV_ATTR_INACTIVE);
        expect(lvchangeActivateCommand(lvmConfig, SNAP_LV_ID), "");

        ReadOnlySnapshotHandle first = lvmProvider.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapVlmData, "_a"));
        // the second opener neither queries nor activates the LV again
        ReadOnlySnapshotHandle second = lvmProvider.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapVlmData, "_b"));
        assertThat(extCmd.getUncalledCommands()).isEmpty();

        // still needed by the second opener
        first.close();

        expect(lvchangeDeactivateCommand(lvmConfig, SNAP_LV_ID), "");
        second.close();
        assertThat(extCmd.getUncalledCommands()).isEmpty();
    }

    @Test
    public void openCloneSnapshotUsesCloneSnapshotName() throws Exception
    {
        createScenario(false);
        // the clone snapshot was created active right before, no command is needed
        lvmProvider.openSnapshotReadOnly(ReadOnlyOpener.forClone(rscVlmData, "target"));

        assertThat(rscVlmData.getCloneDevicePath()).isEqualTo(
            "/dev/" + vg + "/" + RSC_LV_ID + "_" + AbsStorageProvider.getCloneSnapshotName("target", 0)
        );
    }

    @Test
    public void openSnapshotFlaggedForDeletionFails() throws Exception
    {
        createScenario(false);
        snap.getFlags().enableFlags(Snapshot.Flags.DELETE);

        // TestExtCmd fails on any unexpected command, i.e. also on "lvchange -ay"
        assertThatThrownBy(
            () -> lvmProvider.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapVlmData, "_test"))
        ).isInstanceOf(StorageException.class);

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(snapVlmData.getCloneDevicePath()).isNull();
    }

    @Test
    public void openSnapshotOfInactiveSharedSpCopyFails() throws Exception
    {
        // activating a thick snapshot also activates its origin, which the peer with the active copy is using
        createScenario(true);
        rsc.getStateFlags().enableFlags(Resource.Flags.INACTIVE);

        assertThatThrownBy(
            () -> lvmProvider.openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(snapVlmData, "_test"))
        ).isInstanceOf(StorageException.class);

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(snapVlmData.getCloneDevicePath()).isNull();
    }

    @Test
    public void openSnapshotOfActiveSharedSpCopy() throws Exception
    {
        createScenario(true);
        expectLvs(LV_ATTR_ACTIVE);

        ReadOnlySnapshotHandle handle = lvmProvider.openSnapshotReadOnly(
            ReadOnlyOpener.forSnapshot(snapVlmData, "_test")
        );
        handle.close();

        assertThat(extCmd.getUncalledCommands()).isEmpty();
    }

    /**
     * Expects the "lvs" call that reads the activation state of the snapshot LV and returns the lvm config.
     */
    private String expectLvs(String snapLvAttributes)
    {
        expect(pvDisplayCommand(), "  /dev/vdz1\n");
        String lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes("/dev/vdz1");
        expect(
            lvsCommand(lvmConfig),
            "  " + SNAP_LV_ID + ";/dev/" + vg + "/" + SNAP_LV_ID + ";" + VLM_SIZE_IN_KIB + ".00;" + vg + ";;;" +
                snapLvAttributes + ";;0;1;" + RSC_LV_ID + ";\n"
        );
        return lvmConfig;
    }

    private void expect(String[] argv, String stdOut)
    {
        extCmd.setExpectedBehavior(new Command(argv), new TestOutputData(argv, stdOut, "", 0));
    }

    private String[] pvDisplayCommand()
    {
        return new String[]
        {
            "pvdisplay",
            "--config", IGNORE_DRBD_CONFIG,
            "--columns",
            "-o", "pv_name",
            "-S", "vg_name=" + vg,
            "--noheadings",
            "--nosuffix"
        };
    }

    private String[] lvsCommand(String lvmConfig)
    {
        return new String[]
        {
            "lvs",
            "--config", lvmConfig,
            "-o", LVS_COLUMNS,
            "--separator", ";",
            "--noheadings",
            "--units", "k",
            "--nosuffix",
            vg
        };
    }

    private String[] lvchangeDeactivateCommand(String lvmConfig, String lvId)
    {
        return new String[]
        {
            "lvchange",
            "--config", lvmConfig,
            "-an",
            vg + "/" + lvId
        };
    }

    private String[] lvchangeActivateCommand(String lvmConfig, String lvId)
    {
        return new String[]
        {
            "lvchange",
            "--config", lvmConfig,
            "-ay",
            "-K",
            "-y",
            vg + "/" + lvId
        };
    }
}

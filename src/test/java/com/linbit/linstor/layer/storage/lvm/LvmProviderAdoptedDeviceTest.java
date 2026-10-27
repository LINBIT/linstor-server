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
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.drbd.DrbdInvalidateUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider.AbsStorageProviderInit;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.drbd.DrbdLayerTestAccess;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.test.factories.StorPoolTestFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A backing device that already exists under the expected name is adopted by {@link LvmProvider#prepare} without
 * being created. It must not be marked as created by the satellite, so DRBD does not skip the initial sync on it.
 */
public class LvmProviderAdoptedDeviceTest extends GenericDbBase
{
    private static final String LVS_COLUMNS =
        "lv_name,lv_path,lv_size,vg_name,pool_lv,data_percent,lv_attr,metadata_percent,chunk_size,stripes,origin," +
            "kernel_discards";
    private static final String VGS_THICK_COLUMNS = "vg_name,vg_extent_size,vg_size,vg_free";
    private static final String IGNORE_DRBD_CONFIG =
        "devices { ignore_suspended_devices=1 filter=[\"r|^/dev/drbd.*|\"] }";

    private static final String NODE_NAME_STR = "node";
    private static final String RSC_NAME_STR = "rsc";
    private static final String SP_NAME_STR = "lvmSp";
    private static final String SHARED_SPACE_NAME = "SharedSpace";
    private static final String RSC_LV_ID = "rsc_00000";
    private static final String LV_ATTR_INACTIVE = "-wi-------";
    private static final long VLM_SIZE_IN_KIB = 4096L;

    private TestExtCmd extCmd;
    private LvmProvider lvmProvider;

    private VlmProviderObject<Resource> rscVlmData;
    /** Unique per test method since LvmUtils caches the lvm filter config statically per volume group set */
    private String vg;
    private String lvmConfig;

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

        vg = "vg_" + testMethodName.getMethodName();
    }

    /**
     * Creates the {@link #RSC_NAME_STR} resource with one volume.
     *
     * @param sharedSp whether the backing storage pool belongs to a shared space
     * @param rscInactive whether the resource is flagged INACTIVE
     */
    private void createScenario(boolean sharedSp, boolean rscInactive) throws Exception
    {
        Resource rsc = resourceTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR)
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
        volumeTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR, 0)
            .setSize(VLM_SIZE_IN_KIB)
            .setStorPoolData(storPool)
            .build();
        if (rscInactive)
        {
            rsc.getStateFlags().enableFlags(Resource.Flags.INACTIVE);
        }

        rscVlmData = rsc.getLayerData().getVlmProviderObject(new VolumeNumber(0));
    }

    @Test
    public void preExistingLvIsAdoptedAndNotMarkedCreated() throws Exception
    {
        // e.g. an LV left behind by a failed delete, found under the name LINSTOR uses for the volume
        createScenario(false, false);
        expectPrepareCommands(lvsRow(RSC_LV_ID, LV_ATTR_INACTIVE));
        expect(lvchangeActivateCommand(RSC_LV_ID), "");

        lvmProvider.prepare(Collections.singletonList(rscVlmData), Collections.emptyList());

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(rscVlmData.exists()).isTrue();
        assertThat(((AbsStorageVlmData<?>) rscVlmData).isCreatedByStlt()).isFalse();
        assertThat(DrbdLayerTestAccess.isCreatedByStlt(Collections.singletonList(rscVlmData))).isFalse();
    }

    @Test
    public void newVolumeIsNotMarkedCreatedBeforeCreation() throws Exception
    {
        // no LV yet: prepare only finds out that it has to be created, the flag is set by createVolumes
        createScenario(false, false);
        expectPrepareCommands("");

        lvmProvider.prepare(Collections.singletonList(rscVlmData), Collections.emptyList());

        assertThat(rscVlmData.exists()).isFalse();
        assertThat(((AbsStorageVlmData<?>) rscVlmData).isCreatedByStlt()).isFalse();
    }

    private String lvsRow(String lvId, String attributes)
    {
        return "  " + lvId + ";/dev/" + vg + "/" + lvId + ";" + VLM_SIZE_IN_KIB + ".00;" + vg +
            ";;;" + attributes + ";;0;1;;\n";
    }

    private void expectPrepareCommands(String lvsOutput)
    {
        expect(pvDisplayCommand(), "  /dev/vdz1\n");
        lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes("/dev/vdz1");
        expect(lvsCommand(Collections.singleton(vg)), lvsOutput);
        expect(
            vgsCommand(Collections.singleton(vg)),
            "  " + vg + ";4096.00;104857600.00;104857600.00\n"
        );
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

    private String[] lvsCommand(Set<String> volumeGroups)
    {
        List<String> cmd = new ArrayList<>();
        cmd.add("lvs");
        cmd.add("--config");
        cmd.add(lvmConfig);
        Collections.addAll(
            cmd,
            "-o", LVS_COLUMNS,
            "--separator", ";",
            "--noheadings",
            "--units", "k",
            "--nosuffix"
        );
        cmd.addAll(volumeGroups);
        return cmd.toArray(new String[0]);
    }

    private String[] vgsCommand(Set<String> volumeGroups)
    {
        List<String> cmd = new ArrayList<>();
        cmd.add("vgs");
        cmd.add("--config");
        cmd.add(lvmConfig);
        Collections.addAll(
            cmd,
            "-o", VGS_THICK_COLUMNS,
            "--separator", ";",
            "--units", "k",
            "--noheadings",
            "--nosuffix"
        );
        cmd.addAll(volumeGroups);
        return cmd.toArray(new String[0]);
    }

    private String[] lvchangeActivateCommand(String lvId)
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

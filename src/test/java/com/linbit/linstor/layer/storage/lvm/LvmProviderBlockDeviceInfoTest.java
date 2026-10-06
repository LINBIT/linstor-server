package com.linbit.linstor.layer.storage.lvm;

import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.drbd.DrbdInvalidateUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider.AbsStorageProviderInit;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.nio.file.Path;
import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the block-device-property probing of empty storage pools ({@code updateBlockDeviceInfo}):
 * for thick LVM the properties have to be read from the volume group's PV instead of creating a
 * temporary probe LV. Creating an LV writes VG metadata, and the probe runs at satellite start
 * without the shared-space lock - concurrent probes of multiple satellites sharing the VG corrupted
 * its metadata area. Thin pools keep the LV based probe, since a thin volume's queue limits come
 * from the thin pool and not from the PV.
 *
 * Storage pools of peer nodes must not be probed at all: the probe would read the local VG and the
 * controller would apply the result to the local storage pool of the same name (GitHub issue #538).
 */
public class LvmProviderBlockDeviceInfoTest extends GenericDbBase
{
    private static final String IGNORE_DRBD_CONFIG =
        "devices { ignore_suspended_devices=1 filter=[\"r|^/dev/drbd.*|\"] }";

    private static final String NODE_NAME_STR = "node";
    private static final String PEER_NODE_NAME_STR = "peer";
    private static final String RSC_NAME_STR = "rsc";
    private static final String SP_NAME_STR = "lvmSp";
    private static final String THIN_POOL_NAME = "thinpool";
    private static final String PV_DEV = "/dev/vdz1";
    private static final String PROBE_VLM_SIZE = "1024k";
    private static final long VLM_SIZE_IN_KIB = 4096L;

    private TestExtCmd extCmd;
    private AbsStorageProviderInit providerInit;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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
        ControllerPeerConnector ctrlPeerConnector = Mockito.mock(ControllerPeerConnector.class);
        Mockito.when(ctrlPeerConnector.getLocalNode()).thenAnswer(ignored -> nodeTestFactory.get(NODE_NAME_STR, true));
        providerInit = new AbsStorageProviderInit(
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
            ctrlPeerConnector
        );

        vg = "vg_" + testMethodName.getMethodName();
    }

    @Test
    public void thickPoolWithoutVolumesProbesPhysicalVolumeReadOnly() throws Exception
    {
        LvmProvider lvmProvider = new LvmProvider(providerInit);
        StorPool storPool = createStorPool(DeviceProviderKind.LVM);
        expect(pvDisplayCommand(), "  " + PV_DEV + "\n");

        // TestExtCmd fails on any unexpected command, i.e. also on an "lvcreate" of a probe LV
        lvmProvider.updateBlockDeviceInfo(storPool, new LocalPropsChangePojo());

        assertThat(extCmd.getUncalledCommands()).isEmpty();
    }

    @Test
    public void thickPoolWithoutActiveVolumeProbesPhysicalVolumeReadOnly() throws Exception
    {
        LvmProvider lvmProvider = new LvmProvider(providerInit);
        StorPool storPool = createStorPool(DeviceProviderKind.LVM);
        // a volume without a device path, e.g. of an INACTIVE shared-SP copy or before the first
        // device-manager run: the PV still has to be probed instead of creating an LV
        resourceTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR)
            .setLayerStack(Collections.singletonList(DeviceLayerKind.STORAGE))
            .build();
        volumeTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR, 0)
            .setSize(VLM_SIZE_IN_KIB)
            .setStorPoolData(storPool)
            .build();
        expect(pvDisplayCommand(), "  " + PV_DEV + "\n");

        lvmProvider.updateBlockDeviceInfo(storPool, new LocalPropsChangePojo());

        assertThat(extCmd.getUncalledCommands()).isEmpty();
    }

    @Test
    public void thinPoolWithoutVolumesProbesWithTemporaryVolume() throws Exception
    {
        // TestExtCmd simulates lvcreate; use a real file for the resulting device path.
        Path probeDevice = temporaryFolder.newFile("probe").toPath();
        LvmThinProvider lvmThinProvider = new LvmThinProvider(providerInit)
        {
            @Override
            public String getDevicePath(String storageNameRef, String lvIdRef)
            {
                return probeDevice.toString();
            }
        };
        StorPool storPool = createStorPool(DeviceProviderKind.LVM_THIN);

        expect(pvDisplayCommand(), "  " + PV_DEV + "\n");
        String lvmConfig = LvmUtils.getLvmFilterByPhysicalVolumes(PV_DEV);
        expect(lvCreateThinProbeCommand(lvmConfig), "");
        expect(lvRemoveThinProbeCommand(lvmConfig), "");

        lvmThinProvider.updateBlockDeviceInfo(storPool, new LocalPropsChangePojo());

        assertThat(extCmd.getUncalledCommands()).isEmpty();
    }

    @Test
    public void thickPeerPoolIsNotProbed() throws Exception
    {
        assertPeerPoolIsNotProbed(new LvmProvider(providerInit), DeviceProviderKind.LVM);
    }

    @Test
    public void thinPeerPoolIsNotProbed() throws Exception
    {
        assertPeerPoolIsNotProbed(new LvmThinProvider(providerInit), DeviceProviderKind.LVM_THIN);
    }

    private void assertPeerPoolIsNotProbed(LvmProvider lvmProvider, DeviceProviderKind kind) throws Exception
    {
        // the local node with its own storage pool of the same name, which must not receive the peer's probe result
        createStorPool(NODE_NAME_STR, kind);
        StorPool peerStorPool = createStorPool(PEER_NODE_NAME_STR, kind);
        // a peer volume never has a local device path, which would make the probe fall back to the local PV
        resourceTestFactory.builder(PEER_NODE_NAME_STR, RSC_NAME_STR)
            .setLayerStack(Collections.singletonList(DeviceLayerKind.STORAGE))
            .build();
        volumeTestFactory.builder(PEER_NODE_NAME_STR, RSC_NAME_STR, 0)
            .setSize(VLM_SIZE_IN_KIB)
            .setStorPoolData(peerStorPool)
            .build();
        LocalPropsChangePojo propsChange = new LocalPropsChangePojo();

        // no expected commands: TestExtCmd fails on any "pvdisplay" or "lvcreate"
        lvmProvider.updateBlockDeviceInfo(peerStorPool, propsChange);

        assertThat(propsChange.changedStorPoolProps).isEmpty();
    }

    private StorPool createStorPool(DeviceProviderKind kind) throws Exception
    {
        return createStorPool(NODE_NAME_STR, kind);
    }

    private StorPool createStorPool(String nodeName, DeviceProviderKind kind) throws Exception
    {
        StorPool storPool = storPoolTestFactory.builder(nodeName, SP_NAME_STR)
            .setDriverKind(kind)
            .build();
        storPool.getProps().setProp(
            StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY,
            vg,
            ApiConsts.NAMESPC_STORAGE_DRIVER
        );
        if (kind == DeviceProviderKind.LVM_THIN)
        {
            storPool.getProps().setProp(
                StorageConstants.CONFIG_LVM_THIN_POOL_KEY,
                vg + "/" + THIN_POOL_NAME,
                ApiConsts.NAMESPC_STORAGE_DRIVER
            );
        }
        return storPool;
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

    private String[] lvCreateThinProbeCommand(String lvmConfig)
    {
        return new String[]
        {
            "lvcreate",
            "--config", lvmConfig,
            "--virtualsize", PROBE_VLM_SIZE,
            vg,
            "--thinpool", THIN_POOL_NAME,
            "--name", LvmThinProvider.PROBE_VLM_NAME_THIN
        };
    }

    private String[] lvRemoveThinProbeCommand(String lvmConfig)
    {
        return new String[]
        {
            "lvremove",
            "--config", lvmConfig,
            "-f",
            vg + "/" + LvmThinProvider.PROBE_VLM_NAME_THIN
        };
    }
}

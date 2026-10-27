package com.linbit.linstor.layer.storage.zfs;

import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.extproc.utils.TestExtCmd;
import com.linbit.extproc.utils.TestExtCmd.Command;
import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.drbd.DrbdInvalidateUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider.AbsStorageProviderInit;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A zvol that already exists under the name LINSTOR uses for a volume is adopted by {@link ZfsProvider#prepare}
 * without being created. Its volblocksize is kept, so the explicitly requested one from StorDriver/ZfscreateOptions
 * is recorded for the check before DRBD creates the metadata of the new replica.
 */
public class ZfsProviderAdoptedZvolTest extends GenericDbBase
{
    private static final String NODE_NAME_STR = "node";
    private static final String RSC_NAME_STR = "rsc";
    private static final String SP_NAME_STR = "zfsSp";
    private static final String ZPOOL = "tank";
    private static final String ZVOL = ZPOOL + "/" + RSC_NAME_STR + "_00000";
    private static final long VLM_SIZE_IN_KIB = 1_048_576L;

    private TestExtCmd extCmd;
    private ZfsProvider zfsProvider;
    private ZfsData<Resource> zfsData;
    private Volume vlm;

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
    }

    @SuppressWarnings("unchecked")
    private void createScenario(String zfsCreateOptions) throws Exception
    {
        Resource rsc = resourceTestFactory.builder(NODE_NAME_STR, RSC_NAME_STR)
            .setLayerStack(Collections.singletonList(DeviceLayerKind.STORAGE))
            .build();
        StorPool storPool = storPoolTestFactory.builder(NODE_NAME_STR, SP_NAME_STR)
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
        if (!zfsCreateOptions.isEmpty())
        {
            vlm.getVolumeDefinition().getProps().setProp(
                ApiConsts.KEY_STOR_POOL_ZFS_CREATE_OPTIONS,
                zfsCreateOptions,
                ApiConsts.NAMESPC_STORAGE_DRIVER
            );
        }
        zfsData = (ZfsData<Resource>) rsc.getLayerData().getVlmProviderObject(new VolumeNumber(0));
    }

    private void expectZfsList(String output)
    {
        String[] argv = {
            "zfs", "list", "-r", "-H", "-p",
            "-o", "name,refer,volsize,type,volblocksize,origin,clones",
            "-t", "volume,snapshot",
            ZVOL
        };
        extCmd.setExpectedBehavior(new Command(argv), new TestOutputData(argv, output, "", 0));
        String[] getMarkedArgv = {
            "zfs", "get", "-r", "-o", "name,value", "-H", "-p", "-s", "local", "linstor:marked_for_deletion", ZPOOL
        };
        extCmd.setExpectedBehavior(new Command(getMarkedArgv), new TestOutputData(getMarkedArgv, "", "", 0));
    }

    private String zvolRow(long volBlockSizeBytes)
    {
        return ZVOL + "\t8192\t" + VLM_SIZE_IN_KIB * 1024 + "\tvolume\t" + volBlockSizeBytes + "\t-\t-\n";
    }

    @Test
    public void adoptedZvolRecordsRequestedAndActualBlockSize() throws Exception
    {
        createScenario("-b 16K");
        expectZfsList(zvolRow(8_192));

        zfsProvider.prepare(Collections.singletonList(zfsData), Collections.emptyList());

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(zfsData.exists()).isTrue();
        assertThat(zfsData.isCreatedByStlt()).isFalse();
        assertThat(zfsData.getExtentSize()).isEqualTo(8L);
        assertThat(zfsData.getRequestedVolBlockSize()).isEqualTo(16_384L);
    }

    @Test
    public void noRequestedBlockSizeWithoutOption() throws Exception
    {
        createScenario("");
        expectZfsList(zvolRow(8_192));

        zfsProvider.prepare(Collections.singletonList(zfsData), Collections.emptyList());

        assertThat(extCmd.getUncalledCommands()).isEmpty();
        assertThat(zfsData.exists()).isTrue();
        assertThat(zfsData.getRequestedVolBlockSize()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void refusedZvolIsForgottenNotDestroyedOnDelete() throws Exception
    {
        // a zvol LINSTOR refused to adopt was never ours: deleting the volume must not run "zfs destroy" on it
        createScenario("-b 16K");
        expectZfsList(zvolRow(8_192));
        zfsProvider.prepare(Collections.singletonList(zfsData), Collections.emptyList());
        assertThat(zfsData.exists()).isTrue();

        zfsData.setAdoptionRefused(true);
        vlm.getFlags().enableFlags(Volume.Flags.DELETE);

        // any unexpected command (e.g. "zfs destroy") makes TestExtCmd fail the test
        zfsProvider.processVolumes(
            (List<com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject<Resource>>) (List<?>)
                Collections.singletonList(zfsData),
            new ApiCallRcImpl()
        );

        assertThat(zfsData.exists()).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void refusedVolumeIsNotCreatedWhileRscDfnIsDeleted() throws Exception
    {
        // the refused zvol disappeared while the resource definition is being deleted: no "zfs create" for a volume
        // that is about to be deleted anyway
        createScenario("-b 16K");
        expectZfsList("");
        zfsProvider.prepare(Collections.singletonList(zfsData), Collections.emptyList());
        assertThat(zfsData.exists()).isFalse();

        zfsData.setAdoptionRefused(true);
        vlm.getAbsResource().getResourceDefinition().getFlags().enableFlags(ResourceDefinition.Flags.DELETE);

        // any unexpected command (e.g. "zfs create") makes TestExtCmd fail the test
        zfsProvider.processVolumes(
            (List<com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject<Resource>>) (List<?>)
                Collections.singletonList(zfsData),
            new ApiCallRcImpl()
        );

        assertThat(zfsData.exists()).isFalse();
        assertThat(zfsData.isAdoptionRefused()).isTrue();
    }
}

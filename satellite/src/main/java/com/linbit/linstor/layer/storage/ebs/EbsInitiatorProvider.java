package com.linbit.linstor.layer.storage.ebs;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.storage.utils.LsBlkUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AttachVolumeRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.DetachVolumeRequest;
import com.amazonaws.services.ec2.model.Tag;

@Singleton
public class EbsInitiatorProvider extends AbsEbsProvider<LsBlkEntry>
{
    public static final String EC2_INSTANCE_ID_PATH = "/sys/devices/virtual/dmi/id/board_asset_tag";

    private static final int WAIT_NEW_DEV_APPEAR_MS = 500;
    private static final int WAIT_NEW_DEV_APPEAR_COUNT = 30_000 / WAIT_NEW_DEV_APPEAR_MS;
    private static final int WAIT_NEW_DEV_RECONNECT_COUNT = 5;

    private static final String EBS_VLM_STATE_ATTACHING = "attaching";
    private static final String EBS_VLM_STATE_IN_USE = "in-use";

    private static final int TOLERANCE_FACTOR = 3;
    private static final ArrayList<String> AVAILABLE_LETTERS_COMMON = new ArrayList<>();
    private static final ArrayList<String> AVAILABLE_LETTERS_HVM = new ArrayList<>();
    private static final ArrayList<String> AVAILABLE_LETTERS_PV = new ArrayList<>();

    /** Map<StorageName + "/" + LvId, Pair<EBS-vol-id, devicePath>> */
    private final Map<String, Pair<String, String>> lookupTable = new HashMap<>();

    private final @Nullable String ec2InstanceId;

    static
    {
        /*
         * common device letters
         */
        for (char chr = 'z'; chr >= 'b'; chr--)// sda is usually reserved for root
        {
            AVAILABLE_LETTERS_COMMON.add(String.valueOf(chr));
        }

        /*
         * HVM specific device letters
         */
        for (char firstCh = 'c'; firstCh >= 'b'; firstCh--)
        {
            for (char secondCh = 'z'; secondCh >= 'a'; secondCh--)
            {
                AVAILABLE_LETTERS_HVM.add(firstCh + "" + secondCh);
            }
        }

        /*
         * PV specific device letters
         */
        for (char firstCh = 'z'; firstCh >= 'a'; firstCh--)
        {
            for (int secondCh = 15; secondCh >= 1; secondCh--)
            {
                AVAILABLE_LETTERS_HVM.add(firstCh + "" + secondCh);
            }
        }
    }

    @Inject
    public EbsInitiatorProvider(AbsEbsProviderIniit superInitRef)
        throws StorageException
    {
        super(superInitRef, "EBS", DeviceProviderKind.EBS_INIT);

        ec2InstanceId = getEc2InstanceId(errorReporter, extCmdFactory);
    }

    public static @Nullable String getEc2InstanceId(ErrorReporter errorReporterRef, ExtCmdFactory extCmdFactoryRef)
    {
        @Nullable String ret;
        try
        {
            OutputData outputData = extCmdFactoryRef.create().exec("cat", EC2_INSTANCE_ID_PATH);
            if (outputData.exitCode != 0)
            {
                ret = null;
            }
            else
            {
                ret = new String(outputData.stdoutData).trim();
            }
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            errorReporterRef.reportError(exc);
            ret = null;
        }
        return ret;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.EBS_INIT;
    }

    @Override
    protected Map<String, LsBlkEntry> getInfoListImpl(
        List<EbsData<Resource>> vlmDataListRef,
        List<EbsData<Snapshot>> snapVlmsRef
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        return EbsProviderUtils.getEbsInfo(extCmdFactory.create());
    }

    @Override
    protected void updateStates(List<EbsData<Resource>> vlmDataListRef, List<EbsData<Snapshot>> snapVlmsRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        final List<EbsData<?>> combinedList = new ArrayList<>(vlmDataListRef);
        // no snapshots (for now)

        Map<String, com.amazonaws.services.ec2.model.Volume> amaVlmLut = getTargetInfoListImpl(
            vlmDataListRef,
            snapVlmsRef
        );

        for (EbsData<?> vlmData : combinedList)
        {
            final com.amazonaws.services.ec2.model.Volume amaVlm = amaVlmLut.get(getEbsVlmId(vlmData));
            final LsBlkEntry lsblkEntry;

            final String devPath;
            if (vlmData.getDevicePath() == null && amaVlm != null)
            {
                // ctrl got restarted but the amazonVlm is still be connected
                devPath = getFromTags(amaVlm.getTags(), TAG_KEY_LINSTOR_INIT_DEV);
            }
            else
            {
                devPath = vlmData.getDevicePath();
            }
            lsblkEntry = infoListCache.get(devPath);

            updateInfo(vlmData, lsblkEntry, amaVlm);

            if (lsblkEntry != null)
            {
                final long expectedSize = vlmData.getExpectedSize();
                final long actualSize = SizeConv.convert(lsblkEntry.getSize(), SizeUnit.UNIT_B, SizeUnit.UNIT_KiB);
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        vlmData.setSizeState(Size.TOO_SMALL);
                    }
                    else
                    {
                        Size sizeState = Size.TOO_LARGE;

                        final long toleratedSize = expectedSize + 4 * 1024 * TOLERANCE_FACTOR;
                        if (actualSize < toleratedSize)
                        {
                            sizeState = Size.TOO_LARGE_WITHIN_TOLERANCE;
                        }
                        vlmData.setSizeState(sizeState);
                    }
                }
                else
                {
                    vlmData.setSizeState(Size.AS_EXPECTED);
                }
            }
        }
    }

    private void updateInfo(
        EbsData<?> vlmDataRef,
        @Nullable LsBlkEntry lsblkEntryRef,
        @Nullable com.amazonaws.services.ec2.model.Volume amaVlmRef
    )
        throws DatabaseException, StorageException
    {

        if (vlmDataRef.getVolume() instanceof Volume)
        {
            @SuppressWarnings("unchecked")
            EbsData<Resource> vlmData = (EbsData<Resource>) vlmDataRef;
            vlmDataRef.setIdentifier(asLvIdentifier(vlmData));
        }
        else
        {
            @SuppressWarnings("unchecked")
            EbsData<Snapshot> vlmData = (EbsData<Snapshot>) vlmDataRef;
            vlmDataRef.setIdentifier(asSnapLvIdentifier(vlmData));
        }

        if (lsblkEntryRef == null)
        {
            vlmDataRef.setExists(false);
            vlmDataRef.setDevicePath(null);
            vlmDataRef.setAllocatedSize(-1);
            // vlmData.setUsableSize(-1);
        }
        else
        {
            if (amaVlmRef == null)
            {
                final String vlmDataLvId;
                if (vlmDataRef.getVolume() instanceof Volume)
                {
                    vlmDataLvId = asLvIdentifier((EbsData<Resource>) vlmDataRef);
                }
                else
                {
                    vlmDataLvId = asSnapLvIdentifier((EbsData<Snapshot>) vlmDataRef);
                }
                throw new StorageException("Target volume unexpectedly does not exist: " + vlmDataLvId);
            }
            vlmDataRef.setExists(true);
            vlmDataRef.setDevicePath(getFromTags(amaVlmRef.getTags(), TAG_KEY_LINSTOR_INIT_DEV));
            vlmDataRef.setAllocatedSize(SizeConv.convert(amaVlmRef.getSize(), SizeUnit.UNIT_GiB, SizeUnit.UNIT_KiB));
        }
    }

    @Override
    protected void createLvImpl(EbsData<Resource> vlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        connect(vlmDataRef, true);
    }

    private void connect(EbsData<Resource> initiatorVlmDataRef, boolean findDeviceRef)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        AmazonEC2 client = getClient(initiatorVlmDataRef.getStorPool());

        List<LsBlkEntry> lsblkPreConnect = LsBlkUtils.lsblk(extCmdFactory.create());
        String deviceLettersForAttach = findUnusedDevice(lsblkPreConnect);
        String ebsVlmId = getEbsVlmId(initiatorVlmDataRef);
        client.attachVolume(
            new AttachVolumeRequest(
                ebsVlmId,
                ec2InstanceId,
                "/dev/sd" + deviceLettersForAttach
            )
        );

        EbsProviderUtils.waitUntilVolumeHasState(
            client,
            ebsVlmId,
            EBS_VLM_STATE_IN_USE,
            EBS_VLM_STATE_ATTACHING
        );

        if (findDeviceRef)
        {
            String actualDevice = waitForDevice(lsblkPreConnect, deviceLettersForAttach, initiatorVlmDataRef);

            initiatorVlmDataRef.setDevicePath(actualDevice);
            lookupTable.put(
                getStorageName(initiatorVlmDataRef) + "/" + asLvIdentifier(initiatorVlmDataRef),
                new Pair<>(ebsVlmId, actualDevice)
            );

            client.createTags(
                new CreateTagsRequest()
                    .withResources(ebsVlmId)
                    .withTags(new Tag(TAG_KEY_LINSTOR_INIT_DEV, actualDevice))
            );

        }
    }

    private String waitForDevice(
        List<LsBlkEntry> lsblkPreConnect,
        String deviceLettersForAttach,
        EbsData<Resource> vlmDataRef
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        String actualDevice = null;
        int searchCount = WAIT_NEW_DEV_APPEAR_COUNT;
        int reconnectCount = WAIT_NEW_DEV_RECONNECT_COUNT;
        while (searchCount > 0)
        {
            List<LsBlkEntry> lsblkPostConnect = LsBlkUtils.lsblk(extCmdFactory.create());
            try
            {
                actualDevice = findAttachedDevice(
                    lsblkPreConnect,
                    lsblkPostConnect,
                    deviceLettersForAttach
                );
            }
            catch (TooManyDevicesException exc)
            {
                if (reconnectCount <= 0)
                {
                    throw new StorageException("Failed to find connected EBS device", exc);
                }
                else
                {
                    disconnect(vlmDataRef);
                    reconnectCount--;
                    connect(vlmDataRef, false);
                }
            }
            if (actualDevice == null)
            {
                searchCount--;
                try
                {
                    Thread.sleep(WAIT_NEW_DEV_APPEAR_MS);
                }
                catch (InterruptedException ignored)
                {
                }
            }
            else
            {
                searchCount = 0;
            }
        }
        if (actualDevice == null)
        {
            throw new StorageException("No new device created");
        }
        return actualDevice;
    }

    private String findUnusedDevice(List<LsBlkEntry> lsblkEntryList) throws StorageException
    {
        LinkedHashSet<String> availableLetters = new LinkedHashSet<>(AVAILABLE_LETTERS_COMMON);
        // for now we simply assume that we run on HVM
        availableLetters.addAll(AVAILABLE_LETTERS_HVM);

        for (LsBlkEntry entry : lsblkEntryList)
        {
            String kernelName = entry.getKernelName();
            // kernelName should be "/dev/<whatever>". id should now only be the "<whatever>" part
            String id = kernelName.substring(5); // 5 == "/dev/".length()

            // cut of the prefix "sd" or "xvd" so we only have the last letter(s) left
            if (id.startsWith("sd"))
            {
                id = id.substring(2);
            }
            else if (id.startsWith("xvd"))
            {
                id = id.substring(3);
            }

            availableLetters.remove(id);
        }
        if (availableLetters.isEmpty())
        {
            throw new StorageException("No availble device names left!");
        }
        return availableLetters.iterator().next();
    }

    /**
     * @return either the "/dev/..." path or null if no new devices were created.
     *
     * @throws StorageException if more than 1 new devices were created.
     * @throws TooManyDevicesException
     */
    private @Nullable String findAttachedDevice(
        List<LsBlkEntry> lsblkPreConnectRef,
        List<LsBlkEntry> lsblkPostConnectRef,
        String deviceForAttachRef
    )
        throws TooManyDevicesException
    {
        final String ret;
        final HashSet<String> devices = new HashSet<>();

        // collect devices existing AFTER the connect command
        for (LsBlkEntry entry : lsblkPostConnectRef)
        {
            devices.add(entry.getKernelName());
        }

        // remove devices that existed BEFORE the connect command
        for (LsBlkEntry entry : lsblkPreConnectRef)
        {
            devices.remove(entry.getKernelName());
        }

        if (devices.size() > 1)
        {
            throw new TooManyDevicesException();
        }

        if (devices.isEmpty())
        {
            ret = null;
        }
        else
        {
            ret = devices.iterator().next();
        }

        return ret;
    }

    protected PriorityProps getPrioProps(EbsData<Resource> vlmDataRef) throws AccessDeniedException
    {
        Volume vlm = (Volume) vlmDataRef.getVolume();
        Resource rsc = vlm.getAbsResource();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        return new PriorityProps(
            vlm.getProps(storDriverAccCtx),
            rsc.getProps(storDriverAccCtx),
            vlmDataRef.getStorPool().getProps(storDriverAccCtx),
            localNodeProps,
            vlmDfn.getProps(storDriverAccCtx),
            rscDfn.getProps(storDriverAccCtx),
            rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmDfn.getVolumeNumber()),
            rscGrp.getProps(storDriverAccCtx),
            stltConfigAccessor.getReadonlyProps()
        );
    }

    @Override
    protected void resizeLvImpl(EbsData<Resource> vlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        waitUntilResizeFinished(
            getClient(vlmDataRef.getStorPool()),
            getEbsVlmId(vlmDataRef),
            SizeConv.convert(vlmDataRef.getExpectedSize(), SizeUnit.UNIT_KiB, SizeUnit.UNIT_GiB)
        );
        // also wait until local device got resized
        final String devicePath = vlmDataRef.getDevicePath();
        int waitCount = WAIT_AFTER_RESIZE_COUNT;
        boolean resized = false;
        long entrySizeInKib = -1;
        while (waitCount > 0 && !resized)
        {
            try
            {
                Thread.sleep(WAIT_AFTER_RESIZE_TIMEOUT_IN_MS);
            }
            catch (InterruptedException ignored)
            {
            }
            List<LsBlkEntry> lsblkPostResize = LsBlkUtils.lsblk(extCmdFactory.create());
            for (LsBlkEntry entry : lsblkPostResize)
            {
                if (entry.getKernelName().equals(devicePath))
                {
                    entrySizeInKib = SizeConv.convert(
                        entry.getSize(),
                        SizeUnit.UNIT_B,
                        SizeUnit.UNIT_KiB
                    );
                    resized = entrySizeInKib == vlmDataRef.getExpectedSize();
                    break;
                }
            }
        }
        if (!resized)
        {
            throw new StorageException(
                "Device [" + devicePath + "] did not resize. Size: " + entrySizeInKib + "kib, expected: " +
                    vlmDataRef.getExpectedSize() + "kib"
            );
        }
    }

    @Override
    protected void deleteLvImpl(EbsData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        disconnect(vlmDataRef);
    }

    @Override
    protected void deactivateLvImpl(EbsData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        disconnect(vlmDataRef);
    }

    private void disconnect(EbsData<Resource> vlmDataRef)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        AmazonEC2 client = getClient(vlmDataRef.getStorPool());
        String ebsVlmId = getEbsVlmId(vlmDataRef);
        client.detachVolume(
            new DetachVolumeRequest(ebsVlmId)
        );
        client.deleteTags(
            new DeleteTagsRequest()
                .withResources(ebsVlmId)
                .withTags(new Tag(TAG_KEY_LINSTOR_INIT_DEV))
        );
        // volume is most likely in "detaching" state
        EbsProviderUtils.waitUntilVolumeHasState(client, ebsVlmId, "available", "in-use", "detaching");
        vlmDataRef.setExists(false);
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> freeSpaces = new HashMap<>();
        for (String changedSpName : changedStoragePoolStrings)
        {
            freeSpaces.put(changedSpName, ApiConsts.VAL_STOR_POOL_SPACE_ENOUGH);
        }
        return freeSpaces;
    }

    @Override
    public @Nullable LocalPropsChangePojo update(StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        return null;
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPoolRef)
        throws StorageException, AccessDeniedException
    {
        return null;
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return false;
    }

    @Override
    public @Nullable String getDevicePath(String storageNameRef, String lvIdRef)
    {
        Pair<String, String> pair = lookupTable.get(storageNameRef + "/" + lvIdRef);
        return pair == null ? null : pair.objB;
    }

    @Override
    protected void setDevicePath(EbsData<Resource> vlmDataRef, String devicePathRef) throws DatabaseException
    {
        vlmDataRef.setDevicePath(devicePathRef);
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        return fetchOrigAllocatedSizes(vlmDataListRef);
    }

    private static class TooManyDevicesException extends LinStorException
    {
        private static final long serialVersionUID = 8499402298289612696L;

        TooManyDevicesException()
        {
            super("Too many devices appeared");
        }
    }
}

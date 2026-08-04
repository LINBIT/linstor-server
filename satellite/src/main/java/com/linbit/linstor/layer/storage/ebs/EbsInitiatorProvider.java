package com.linbit.linstor.layer.storage.ebs;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
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
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AttachVolumeRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.DetachVolumeRequest;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.VolumeAttachment;

@Singleton
public class EbsInitiatorProvider extends AbsEbsProvider<LsBlkEntry>
{
    public static final String EC2_INSTANCE_ID_PATH = "/sys/devices/virtual/dmi/id/board_asset_tag";

    private static final Pattern DEVICES_FOR_REQUEST_PATTERN = Pattern.compile("/dev/(?:sd|xvd)(?<letter>.)");

    private static final int WAIT_NEW_DEV_APPEAR_MS = 500;
    private static final int WAIT_NEW_DEV_APPEAR_COUNT = 30_000 / WAIT_NEW_DEV_APPEAR_MS;

    private static final String EBS_VLM_STATE_ATTACHING = "attaching";
    private static final String EBS_VLM_STATE_IN_USE = "in-use";

    private static final int TOLERANCE_FACTOR = 3;

    /** {@code Map<StorageName + "/" + LvId, Pair<EBS-vol-id, devicePath>>} */
    private final Map<String, Pair<String, String>> lookupTable = new HashMap<>();

    private final @Nullable String ec2InstanceId;
    /** Unmodifiable list containing only {@code ec2InstanceId}'s content if {@code ec2InstanceId} is non-null.
     * Empty otherwise. */
    private final List<String> ec2InstanceIdAsList;

    @Inject
    public EbsInitiatorProvider(AbsEbsProviderIniit superInitRef)
        throws StorageException
    {
        super(superInitRef, "EBS", DeviceProviderKind.EBS_INIT);

        ec2InstanceId = getEc2InstanceId(errorReporter, extCmdFactory);
        ec2InstanceIdAsList = ec2InstanceId == null ?
            Collections.emptyList() :
            Collections.singletonList(ec2InstanceId);
    }

    /**
     * This method returns the instance id, read from {@code /sys/devices/virtual/dmi/id/board_asset_tag}. However this
     * file only exists on Nitro based EC2 machines, not on the old Xen instances. This is simply a limitation of
     * LINSTOR that it only works with Nitro based instances.
     *
     * <p>One difficulty with Xen based instances would also be the cumbersome finding of attached devices. Nitro
     * attaches the devices as nvme devices with a serial number that matches the EBS ID, which LINSTOR already have
     * and can precisely match instead of comparing before/after "lsblk" runs.</p>
     */
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
                ret = new String(outputData.stdoutData, StandardCharsets.UTF_8).trim();
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
        throws StorageException, DatabaseException
    {
        return EbsProviderUtils.getEbsInfo(extCmdFactory.create());
    }

    @Override
    protected void updateStates(List<EbsData<Resource>> vlmDataListRef, List<EbsData<Snapshot>> snapVlmsRef)
        throws StorageException, DatabaseException
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
        throws StorageException, DatabaseException
    {
        connect(vlmDataRef);
    }

    private void connect(EbsData<Resource> initiatorVlmDataRef)
        throws StorageException, DatabaseException
    {
        AmazonEC2 client = getClient(initiatorVlmDataRef.getStorPool());

        String devicePathForRequest = getDevicePathForRequest(client);

        String ebsVlmId = getEbsVlmId(initiatorVlmDataRef);
        client.attachVolume(
            new AttachVolumeRequest(
                ebsVlmId,
                ec2InstanceId,
                devicePathForRequest
            )
        );

        EbsProviderUtils.waitUntilVolumeHasState(
            errorReporter,
            client,
            ebsVlmId,
            EBS_VLM_STATE_IN_USE,
            EBS_VLM_STATE_ATTACHING
        );

        String actualDevice = waitForDevice(ebsVlmId);

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

    /**
     * Older versions tried to scan here for existing (aka local) {@code /dev/sd[a-z]} or {@code /dev/xvd...} to see
     * which device LINSTOR should include in the next attachVolume request, although AWS could receive a request like
     * {@code /dev/sdz} but (in old Xen versions, which are not supported by LINSTOR) attach a device that comes up as
     * {@code /dev/xvdz} or even with a different last letter. Nitro based EC2 instances on the other hand result in
     * {@code /dev/nvme[0..26]n1}. So the old approach (scan for local devices) does not work in Nitro setups, and only
     * in most cases worked in Xen setups (not always).
     *
     * <p>Instead of keeping track of the devices we already sent to AWS, we simply ask AWS directly and choose an
     * unused device based on the AWS response</p>
     */
    private String getDevicePathForRequest(AmazonEC2 clientRef)
        throws StorageException
    {
        if (ec2InstanceIdAsList.isEmpty())
        {
            throw new StorageException(
                "No EC2 instance ID found in " + EC2_INSTANCE_ID_PATH +
                    ". Only Nitro-based EC2 instances have this file / are supported."
            );
        }
        DescribeVolumesResult describeVolumesResult = clientRef.describeVolumes(
            new DescribeVolumesRequest().withFilters(new Filter("attachment.instance-id", ec2InstanceIdAsList))
        );
        Set<Character> previouslyRequestedDeviceLetters = new HashSet<>();
        for (com.amazonaws.services.ec2.model.Volume ec2Vlm : describeVolumesResult.getVolumes())
        {
            for (VolumeAttachment ec2VlmAttachment : ec2Vlm.getAttachments())
            {
                // the device of the original request, i.e. "/dev/sdz"
                @Nullable String device = ec2VlmAttachment.getDevice();
                if (device != null)
                {
                    // we want to strip "/dev/sd" or "/dev/xvd" and the trailing number (if exists)
                    // currently we simply do not request "/dev/nvme..." devices, so there is no need
                    // to check for that
                    Matcher matcher = DEVICES_FOR_REQUEST_PATTERN.matcher(device);
                    if (matcher.find())
                    {
                        previouslyRequestedDeviceLetters.add(matcher.group("letter").charAt(0));
                    }
                    else
                    {
                        errorReporter.logWarning("Ignoring unrecognized device of original request: %s", device);
                    }
                }
            }
        }
        char selectedLetter = selectLetter(previouslyRequestedDeviceLetters);
        return "/dev/sd" + selectedLetter;
    }

    /**
     * <p>https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html as of 2026 Aug. 04:
     *
     * > (Linux instances) Some custom kernels might have restrictions that limit use to /dev/sd[f-p] or
     * > /dev/sd[f-p][1-6]. If you're having trouble using /dev/sd[q-z] or /dev/sd[q-z][1-6], try switching to
     * > /dev/sd[f-p] or /dev/sd[f-p][1-6].
     * </p>
     *
     * <p>This means that LINSTOR first tries the letters [f-p], then [q-z] and [b-e] only as last.</p>
     *
     * @throws StorageException if all letters are exhausted
     */
    private char selectLetter(Set<Character> previouslyRequestedDeviceLettersRef) throws StorageException
    {
        // technically we could split here 'f-p' and then 'q-z', but since 'q' is the next char after 'p', we can also
        // merge the two searches.
        @Nullable Character ret = selectLetter(previouslyRequestedDeviceLettersRef, 'f', 'z');
        if (ret == null)
        {
            ret = selectLetter(previouslyRequestedDeviceLettersRef, 'b', 'e');
        }
        if (ret == null)
        {
            throw new StorageException("No available device names left!");
        }
        return ret;
    }

    private @Nullable Character selectLetter(
        Set<Character> previouslyRequestedDeviceLettersRef,
        char minLetterRef,
        char maxLetterRef
    )
    {
        @Nullable Character ret = null;
        if (minLetterRef > maxLetterRef)
        {
            throw new ImplementationError(
                "minLetter ('" + minLetterRef + "') must be smaller than maxLetter ('" + maxLetterRef + "')!"
            );
        }
        for (char ch = minLetterRef; ch <= maxLetterRef; ch++)
        {
            if (!previouslyRequestedDeviceLettersRef.contains(ch))
            {
                ret = ch;
                break;
            }
        }
        return ret;
    }

    /**
     * Finds the device by scanning NVMe devices for the given serial number.
     *
     * @return The device path, for example {@code "/dev/nvme1n1"}
     */
    private String waitForDevice(String ebsVlmId)
        throws StorageException
    {
        @Nullable String actualDevice = null;
        int searchCount = WAIT_NEW_DEV_APPEAR_COUNT;
        while (searchCount > 0)
        {
            List<LsBlkEntry> lsblk = LsBlkUtils.lsblk(extCmdFactory.create());
            actualDevice = findDeviceBySerial(ebsVlmId, lsblk);
            if (actualDevice == null)
            {
                searchCount--;
                try
                {
                    Thread.sleep(WAIT_NEW_DEV_APPEAR_MS);
                }
                catch (InterruptedException ignored)
                {
                    Thread.currentThread().interrupt();
                    break;
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

    /**
     * Scans "lsblk -o +SERIAL" for the given ebsVlmId
     */
    private @Nullable String findDeviceBySerial(String ebsVlmIdRef, Collection<LsBlkEntry> lsblkRef)
    {
        @Nullable String foundDevice = null;
        // ebsVlmId is something like "vol-[0-9a-f]+", but the SERIAL field from lsblk does not contain the "-"
        // just for completeness sake, we still check both.
        String strippedEbsVlmId = ebsVlmIdRef.replace("-", "");

        for (LsBlkEntry lsBlkEntry : lsblkRef)
        {
            @Nullable String serial = lsBlkEntry.getSerial();
            if (ebsVlmIdRef.equalsIgnoreCase(serial) || strippedEbsVlmId.equalsIgnoreCase(serial))
            {
                foundDevice = lsBlkEntry.getName();
                break;
            }
        }

        return foundDevice;
    }

    protected PriorityProps getPrioProps(EbsData<Resource> vlmDataRef)
    {
        Volume vlm = (Volume) vlmDataRef.getVolume();
        Resource rsc = vlm.getAbsResource();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        return new PriorityProps(
            vlm.getProps(),
            rsc.getProps(),
            vlmDataRef.getStorPool().getProps(),
            localNodeProps,
            vlmDfn.getProps(),
            rscDfn.getProps(),
            rscGrp.getVolumeGroupProps(vlmDfn.getVolumeNumber()),
            rscGrp.getProps(),
            stltConfigAccessor.getReadonlyProps()
        );
    }

    @Override
    protected void resizeLvImpl(EbsData<Resource> vlmDataRef)
        throws StorageException, DatabaseException
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
        throws StorageException, DatabaseException
    {
        disconnect(vlmDataRef);
    }

    @Override
    protected void deactivateLvImpl(EbsData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, DatabaseException
    {
        disconnect(vlmDataRef);
    }

    private void disconnect(EbsData<Resource> vlmDataRef)
        throws StorageException, DatabaseException
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
        EbsProviderUtils.waitUntilVolumeHasState(errorReporter, client, ebsVlmId, "available", "in-use", "detaching");
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
        throws DatabaseException, StorageException
    {
        return null;
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPoolRef)
        throws StorageException
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
}

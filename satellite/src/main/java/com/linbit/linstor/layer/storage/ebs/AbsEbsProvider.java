package com.linbit.linstor.layer.storage.ebs;

import com.linbit.ImplementationError;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Tag;

public abstract class AbsEbsProvider<INFO> extends AbsStorageProvider<INFO, EbsData<Resource>, EbsData<Snapshot>>
{
    @Singleton
    public static class AbsEbsProviderIniit
    {
        private final AbsStorageProviderInit superInit;
        private final CoreModule.RemoteMap remoteMap;
        private final DecryptionHelper decryptionHelper;
        private final StltSecurityObjects stltSecObjs;

        @Inject
        public AbsEbsProviderIniit(
            AbsStorageProviderInit superInitRef,
            RemoteMap remoteMapRef,
            DecryptionHelper decryptionHelperRef,
            StltSecurityObjects stltSecObjsRef
        )
        {
            superInit = superInitRef;
            remoteMap = remoteMapRef;
            decryptionHelper = decryptionHelperRef;
            stltSecObjs = stltSecObjsRef;
        }
    }

    /** <code>"${spName}/${rscName}${rscSuffix}_${vlmNr}"</code> */
    public static final String FORMAT_RSC_TO_LVM_ID = "%s/%s%s_%05d";
    /** <code>"${spName}/${rscName}${rscSuffix}_${vlmNr}_${snapName}"</code> */
    public static final String FORMAT_SNAP_TO_LVM_ID = FORMAT_RSC_TO_LVM_ID + "_%s";

    public static final String FORMAT_PATTERN_KEY_SP_NAME = "spName";
    public static final String FORMAT_PATTERN_KEY_RSC_NAME = "rscName";
    public static final String FORMAT_PATTERN_KEY_RSC_SUFFIX = "suffix";
    public static final String FORMAT_PATTERN_KEY_VLM_NR = "vlmNr";
    public static final String FORMAT_PATTERN_KEY_SNAP_NAME = "snapName";
    public static final Pattern FORMAT_PATTERN = Pattern.compile(
        "(?<" + FORMAT_PATTERN_KEY_SP_NAME + ">[^/]+)/" +
        "(?<" + FORMAT_PATTERN_KEY_RSC_NAME + ">[^_.]+)" +
        "(?<" + FORMAT_PATTERN_KEY_RSC_SUFFIX + ">.[^_]+)?_" +
        "(?<" + FORMAT_PATTERN_KEY_VLM_NR + ">[0-9]{5})" +
        "(?:_(?<" + FORMAT_PATTERN_KEY_SNAP_NAME + ">.*))?"
    );

    protected static final String EBS_VLM_STATE_AVAILABLE = "available";
    protected static final String EBS_VLM_STATE_CREATING = "creating";

    protected static final HashSet<String> LINSTOR_TAGS = new HashSet<>();
    protected static final String TAG_KEY_LINSTOR_ID = "LinstorID";
    protected static final String TAG_KEY_LINSTOR_INIT_DEV = "LinstorInitDevice";

    protected static final int WAIT_AFTER_RESIZE_COUNT = 300;
    protected static final long WAIT_AFTER_RESIZE_TIMEOUT_IN_MS = 100;

    protected static final SpaceInfo ENOUGH_SPACE_INFO = new SpaceInfo(
        ApiConsts.VAL_STOR_POOL_SPACE_ENOUGH,
        ApiConsts.VAL_STOR_POOL_SPACE_ENOUGH
    );
    protected static final int TOLERANCE_FACTOR = 3;

    static
    {
        LINSTOR_TAGS.add(TAG_KEY_LINSTOR_ID);
        LINSTOR_TAGS.add(TAG_KEY_LINSTOR_INIT_DEV);
    }

    private final Map<EbsRemote, AmazonEC2> amazonEc2ClientLUT;
    protected final RemoteMap remoteMap;
    private final DecryptionHelper decHelper;
    private final StltSecurityObjects stltSecObj;

    AbsEbsProvider(
        AbsEbsProviderIniit initRef,
        String typeDescrRef,
        DeviceProviderKind kindRef
    )
    {
        super(initRef.superInit, typeDescrRef, kindRef);
        remoteMap = initRef.remoteMap;
        decHelper = initRef.decryptionHelper;
        stltSecObj = initRef.stltSecObjs;

        amazonEc2ClientLUT = new HashMap<>();
    }

    protected AmazonEC2 getClient(StorPool storPoolRef) throws StorageException
    {
        return getClient(getEbsRemote(storPoolRef));
    }

    protected AmazonEC2 getClient(EbsRemote remoteRef) throws StorageException
    {
        AmazonEC2 client = amazonEc2ClientLUT.get(remoteRef); // to avoid double-locking problem
        if (client == null)
        {
            synchronized (amazonEc2ClientLUT)
            {
                client = amazonEc2ClientLUT.get(remoteRef); // update, just to make sure
                if (client == null)
                {
                    byte[] masterKey = stltSecObj.getCryptKey();
                    if (masterKey == null)
                    {
                        throw new StorageException(
                            "Cannot operate on EBS volumes/Snapshots without having linstor's encryption enabled" +
                                " (enter passphrase)"
                        );
                    }
                    try
                    {
                        EndpointConfiguration endpointConfiguration = new EndpointConfiguration(
                            remoteRef.getUrl(storDriverAccCtx).toString(),
                            remoteRef.getRegion(storDriverAccCtx)
                        );
                        client = AmazonEC2ClientBuilder.standard()
                            .withEndpointConfiguration(endpointConfiguration)
                            .withCredentials(
                                new AWSStaticCredentialsProvider(
                                    new BasicAWSCredentials(
                                        new String(
                                            decHelper.decrypt(
                                                masterKey,
                                                remoteRef.getEncryptedAccessKey(storDriverAccCtx)
                                            )
                                        ),
                                        new String(
                                            decHelper.decrypt(
                                                masterKey,
                                                remoteRef.getEncryptedSecretKey(storDriverAccCtx)
                                            )
                                        )
                                    )
                                )
                            ).build();
                    }
                    catch (AccessDeniedException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    catch (LinStorException exc)
                    {
                        String errMsg = "Failed to decrypt access / secret key.";
                        if (masterKey == null || masterKey.length == 0)
                        {
                            errMsg += " MasterKey is missing.";
                        }
                        throw new StorageException(errMsg, exc);
                    }

                    amazonEc2ClientLUT.put(remoteRef, client);
                }
            }
        }
        return client;
    }

    public void recacheAmazonClient(EbsRemote remote)
    {
        synchronized (amazonEc2ClientLUT)
        {
            amazonEc2ClientLUT.remove(remote);
        }
    }

    protected Map<String, com.amazonaws.services.ec2.model.Volume> getTargetInfoListImpl(
        List<EbsData<Resource>> vlmDataListRef,
        List<EbsData<Snapshot>> snapVlmsRef
    )
        throws AccessDeniedException, StorageException
    {
        final Map<String, com.amazonaws.services.ec2.model.Volume> ret = new HashMap<>();

        final Set<StorPool> storPools = new HashSet<>();
        {
            List<EbsData<?>> combinedList = new ArrayList<>(vlmDataListRef);
            combinedList.addAll(snapVlmsRef);
            for (EbsData<?> data : combinedList)
            {
                storPools.add(data.getStorPool());
            }
        }

        for (StorPool storPool : storPools)
        {
            EbsRemote ebsRemote = getEbsRemote(storPool);
            AmazonEC2 client = getClient(ebsRemote);
            DescribeVolumesResult volumesResult = client.describeVolumes(
                new DescribeVolumesRequest().withFilters(
                    new Filter("availability-zone", Arrays.asList(ebsRemote.getAvailabilityZone(storDriverAccCtx)))
                )
            );
            for (com.amazonaws.services.ec2.model.Volume amazonVolume : volumesResult.getVolumes())
            {
                ret.put(amazonVolume.getVolumeId(), amazonVolume);
            }
        }
        return ret;
    }

    protected @Nullable String getFromTags(List<Tag> tagList, String key)
    {
        String ret = null;
        for (Tag tag : tagList)
        {
            if (tag.getKey().equals(key))
            {
                ret = tag.getValue();
                break;
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    protected String asGenericLvIdentifier(EbsData<?> vlmData)
    {
        String vlmDataLvId;
        if (vlmData.getVolume() instanceof Volume)
        {
            vlmDataLvId = asLvIdentifier((EbsData<Resource>) vlmData);
        }
        else
        {
            vlmDataLvId = asSnapLvIdentifier((EbsData<Snapshot>) vlmData);
        }
        return vlmDataLvId;
    }

    @Override
    protected String asLvIdentifier(
        StorPoolName spName,
        ResourceName resourceNameRef,
        String rscNameSuffixRef,
        VolumeNumber volumeNumberRef
    )
    {
        // this lvIdentifier does not correspond to the actual /dev/<whatever> device, but we can come up with an
        // arbitrary ID here as long as we keep track of the mapping of ID -> Pair<EBS-vol-id, "/dev/<whatever>">
        return String.format(
            FORMAT_RSC_TO_LVM_ID,
            spName.displayValue,
            resourceNameRef.displayValue,
            rscNameSuffixRef,
            volumeNumberRef.value
        );
    }

    @Override
    protected String asSnapLvIdentifier(EbsData<Snapshot> snapVlmDataRef)
    {
        StorageRscData<Snapshot> snapData = snapVlmDataRef.getRscLayerObject();
        return String.format(
            FORMAT_SNAP_TO_LVM_ID,
            snapVlmDataRef.getStorPool().getName().displayValue,
            snapData.getResourceName().displayValue,
            snapData.getResourceNameSuffix(),
            snapVlmDataRef.getVlmNr().value,
            snapData.getAbsResource().getSnapshotName().displayValue
        );
    }

    @Override
    protected void setAllocatedSize(EbsData<Resource> vlmDataRef, long sizeRef) throws DatabaseException
    {
        vlmDataRef.setAllocatedSize(sizeRef);
    }

    @Override
    protected void setUsableSize(EbsData<Resource> vlmDataRef, long sizeRef) throws DatabaseException
    {
        vlmDataRef.setUsableSize(sizeRef);
    }

    @Override
    protected void setExpectedUsableSize(EbsData<Resource> vlmDataRef, long sizeRef)
        throws DatabaseException, StorageException
    {
        vlmDataRef.setExpectedSize(sizeRef);
    }

    @Override
    protected boolean updateDmStats()
    {
        return false;
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPoolRef) throws StorageException, AccessDeniedException
    {
        return ENOUGH_SPACE_INFO;
    }

    @Override
    protected String getStorageName(EbsData<Resource> vlmDataRef)
        throws DatabaseException, AccessDeniedException, StorageException
    {
        return getStorageName(vlmDataRef.getStorPool());
    }

    @Override
    protected String getStorageName(StorPool storPoolRef) throws AccessDeniedException, StorageException
    {
        String poolName;
        try
        {
            poolName = DeviceLayerUtils.getNamespaceStorDriver(
                storPoolRef.getProps(storDriverAccCtx)
            )
                .getProp(ApiConsts.KEY_STOR_POOL_NAME);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return poolName;
    }

    protected void setEbsVlmId(EbsData<Resource> vlmDataRef, String ebsVlmIdRef)
        throws AccessDeniedException, DatabaseException
    {
        try
        {
            ((Volume) vlmDataRef.getVolume()).getProps(storDriverAccCtx)
                .setProp(
                    InternalApiConsts.KEY_EBS_VLM_ID + vlmDataRef.getRscLayerObject().getResourceNameSuffix(),
                    ebsVlmIdRef,
                    ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
                );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    protected String getEbsVlmId(EbsData<?> vlmDataRef)
    {
        try
        {
            return EbsUtils.getEbsVlmId(storDriverAccCtx, vlmDataRef);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    protected void setEbsSnapId(EbsData<Snapshot> snapVlmDataRef, String ebsSnapIdRef)
        throws AccessDeniedException, DatabaseException
    {
        try
        {
            ((SnapshotVolume) snapVlmDataRef.getVolume()).getSnapVlmProps(storDriverAccCtx)
                .setProp(
                    EbsUtils.getEbsSnapIdKey(snapVlmDataRef),
                    ebsSnapIdRef
                );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    protected String getEbsSnapId(EbsData<Snapshot> snapVlmDataRef)
    {
        try
        {
            return EbsUtils.getEbsSnapId(storDriverAccCtx, snapVlmDataRef);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    protected EbsRemote getEbsRemote(StorPool storPoolRef) {
        return EbsUtils.getEbsRemote(
            storDriverAccCtx,
            remoteMap,
            storPoolRef,
            stltConfigAccessor.getReadonlyProps()
        );
    }

    protected void waitUntilResizeFinished(AmazonEC2 client, String ebsVlmId, long expectedNewSizeInGib)
        throws StorageException
    {
        DescribeVolumesRequest describeVolumesRequest = new DescribeVolumesRequest()
            .withVolumeIds(ebsVlmId);

        boolean hasExpectedSize = false;
        int waitCount = WAIT_AFTER_RESIZE_COUNT;
        while (!hasExpectedSize && waitCount-- > 0)
        {
            DescribeVolumesResult describeVolumes = client.describeVolumes(describeVolumesRequest);
            if (describeVolumes.getVolumes().size() != 1)
            {
                throw new StorageException(
                    "Unexpected volume count for id: " + ebsVlmId + ", count: " + describeVolumes.getVolumes().size()
                );
            }
            hasExpectedSize = describeVolumes.getVolumes().get(0).getSize() == expectedNewSizeInGib;
            try
            {
                Thread.sleep(WAIT_AFTER_RESIZE_TIMEOUT_IN_MS);
            }
            catch (InterruptedException exc)
            {
                // ignored
            }
        }
        if (!hasExpectedSize)
        {
            throw new StorageException(
                "EBS Volume " + ebsVlmId + " unexpectedly did not resize within " +
                    (WAIT_AFTER_RESIZE_COUNT * WAIT_AFTER_RESIZE_TIMEOUT_IN_MS) + "ms"
            );
        }
    }

    @Override
    protected long getExtentSize(AbsStorageVlmData<?> vlmDataRef)
    {
        return SizeConv.convert(1, SizeUnit.UNIT_GiB, SizeUnit.UNIT_KiB);
    }
}

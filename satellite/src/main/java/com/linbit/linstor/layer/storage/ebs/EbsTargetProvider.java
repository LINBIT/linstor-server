package com.linbit.linstor.layer.storage.ebs;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.Remote;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeResult;
import com.amazonaws.services.ec2.model.DeleteVolumeRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.ModifyVolumeRequest;
import com.amazonaws.services.ec2.model.ResourceType;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;

@Singleton
public class EbsTargetProvider extends AbsEbsProvider<com.amazonaws.services.ec2.model.Volume>
{
    @Inject
    public EbsTargetProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
        CloneService cloneServiceRef,
        BackupShippingMgr backupShipMgrRef,
        RemoteMap remoteMapRef,
        DecryptionHelper decHelperRef,
        StltSecurityObjects stltSecObjRef
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            transMgrProvider,
            "EBS-Target",
            DeviceProviderKind.EBS_TARGET,
            snapShipMrgRef,
            extToolsCheckerRef,
            cloneServiceRef,
            backupShipMgrRef,
            remoteMapRef,
            decHelperRef,
            stltSecObjRef
        );

        isDevPathExpectedToBeNull = true;
    }

    // @Override
    // public void clearCache() throws StorageException
    // {
    // super.clearCache();
    // }

    @Override
    protected Map<String, com.amazonaws.services.ec2.model.Volume> getInfoListImpl(
        List<EbsData<Resource>> vlmDataListRef,
        List<EbsData<Snapshot>> snapVlmsRef
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        return getTargetInfoListImpl(vlmDataListRef, snapVlmsRef);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void updateStates(List<EbsData<Resource>> vlmDataListRef, List<EbsData<Snapshot>> snapVlmsRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        final List<EbsData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataListRef);
        // no snapshots (for now)

        for (EbsData<?> vlmData : combinedList)
        {
            final com.amazonaws.services.ec2.model.Volume amazonVlm = infoListCache.get(getEbsVlmId(vlmData));

            updateInfo(vlmData, amazonVlm);

            if (amazonVlm != null)
            {
                final long expectedSize = vlmData.getExpectedSize();
                final long actualSize = SizeConv.convert(amazonVlm.getSize(), SizeUnit.UNIT_GiB, SizeUnit.UNIT_KiB);
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

    private void updateInfo(EbsData<?> vlmDataRef, com.amazonaws.services.ec2.model.Volume amazonVlmRef)
        throws DatabaseException, AccessDeniedException
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

        if (amazonVlmRef == null)
        {
            vlmDataRef.setExists(false);
            vlmDataRef.setDevicePath(null);
            vlmDataRef.setAllocatedSize(-1);
            // vlmData.setUsableSize(-1);
        }
        else
        {
            if (!vlmDataRef.exists())
            {
                // we might have lost the volume and just found it again. make sure the property is set
                try
                {
                    vlmDataRef.getVolume().getProps(storDriverAccCtx).setProp(
                        InternalApiConsts.KEY_EBS_VLM_ID + vlmDataRef.getRscLayerObject().getResourceNameSuffix(),
                        amazonVlmRef.getVolumeId(),
                        ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
                    );
                }
                catch (InvalidKeyException | InvalidValueException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
            vlmDataRef.setAllocatedSize(SizeConv.convert(amazonVlmRef.getSize(), SizeUnit.UNIT_GiB, SizeUnit.UNIT_KiB));

            vlmDataRef.setExists(true);
        }
    }

    @Override
    protected void createLvImpl(EbsData<Resource> vlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        EbsRemote remote = getEbsRemote(vlmDataRef.getStorPool());
        AmazonEC2 client = getClient(remote);
        CreateVolumeResult createVolumeResult = client.createVolume(
            new CreateVolumeRequest(
                (int) SizeConv.convertRoundUp(vlmDataRef.getExpectedSize(), SizeUnit.UNIT_KiB, SizeUnit.UNIT_GiB),
                remote.getAvailabilityZone(storDriverAccCtx)
            )
                .withTagSpecifications(
                    new TagSpecification()
                        .withResourceType(ResourceType.Volume)
                        .withTags(
                            new Tag(TAG_KEY_LINSTOR_ID, asLvIdentifier(vlmDataRef))
                        )
                )
        );

        String ebsVlmId = createVolumeResult.getVolume().getVolumeId();
        setEbsVlmId(vlmDataRef, ebsVlmId);
        EbsProviderUtils.waitUntilVolumeHasState(client, ebsVlmId, EBS_VLM_STATE_AVAILABLE, EBS_VLM_STATE_CREATING);

        long allocatedSize = getAllocatedSize(vlmDataRef); // queries online
        vlmDataRef.setAllocatedSize(allocatedSize);
        vlmDataRef.setUsableSize(allocatedSize);
    }

    @Override
    protected EbsRemote getEbsRemote(StorPool storPoolRef)
    {
        Remote remote;
        try
        {
            remote = remoteMap.get(
                new RemoteName(
                    getPrioProps(storPoolRef).getProp(
                        ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.NAMESPC_EBS + "/" + ApiConsts.KEY_REMOTE
                    ),
                    true
                )
            );
        }
        catch (InvalidKeyException | InvalidNameException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        if (!(remote instanceof EbsRemote))
        {
            throw new ImplementationError(
                "Unexpected remote type: " + (remote == null ? "null" : remote.getClass().getSimpleName())
            );
        }
        return (EbsRemote) remote;
    }

    private PriorityProps getPrioProps(StorPool spRef) throws AccessDeniedException
    {
        return new PriorityProps(
            spRef.getProps(storDriverAccCtx),
            localNodeProps,
            stltConfigAccessor.getReadonlyProps()
        );
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
        AmazonEC2 client = getClient(vlmDataRef.getStorPool());

        String ebsVlmId = vlmDataRef.getVolume().getProps(storDriverAccCtx).getProp(
            InternalApiConsts.KEY_EBS_VLM_ID,
            ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
            );
        long newSizeInGib = SizeConv.convert(vlmDataRef.getExpectedSize(), SizeUnit.UNIT_KiB, SizeUnit.UNIT_GiB);
        if (newSizeInGib > Integer.MAX_VALUE)
        {
            throw new StorageException(
                "Can only grow to max " + Integer.MAX_VALUE + "GiB, but " + newSizeInGib + " was given"
            );
        }

        client.modifyVolume(
            new ModifyVolumeRequest()
                .withSize((int) newSizeInGib)
                .withVolumeId(ebsVlmId)
        );

        // wait until amazon also reports the correct size as otherwise the next
        // DevMgrRun would read the old size and will try another resize which will
        // fail since the amazon volume will be still in optimizing state (which might
        // take up to a few hours. See requirements for resizing (growing):
        // https://docs.amazonaws.cn/en_us/AWSEC2/latest/UserGuide/modify-volume-requirements.html

        waitUntilResizeFinished(client, ebsVlmId, newSizeInGib);

        vlmDataRef.setAllocatedSize(newSizeInGib);
        vlmDataRef.setUsableSize(newSizeInGib);
    }

    @Override
    protected void deleteLvImpl(EbsData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        AmazonEC2 client = getClient(vlmDataRef.getStorPool());

        String ebsVlmId = vlmDataRef.getVolume().getProps(storDriverAccCtx).getProp(
            InternalApiConsts.KEY_EBS_VLM_ID,
            ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
        );
        errorReporter.logTrace("Deleting EBS volumd ID: %s", ebsVlmId);
        client.deleteVolume(new DeleteVolumeRequest(ebsVlmId));

        vlmDataRef.setExists(false);
    }

    @Override
    protected void deactivateLvImpl(EbsData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // noop
    }

    @Override
    protected long getAllocatedSize(EbsData<Resource> vlmDataRef) throws StorageException
    {
        long ret = -1;
        String ebsVlmId = getEbsVlmId(vlmDataRef);
        if (ebsVlmId != null)
        {
            AmazonEC2 client = getClient(vlmDataRef.getStorPool());
            DescribeVolumesResult volumesResult = client.describeVolumes(
                new DescribeVolumesRequest().withVolumeIds(ebsVlmId)
            );
            if (volumesResult.getVolumes().size() > 1)
            {
                throw new StorageException(
                    "Unexected count of volumes for EBS vol-id: " + ebsVlmId + ", count: " +
                        volumesResult.getVolumes().size()
                );
            }
            if (!volumesResult.getVolumes().isEmpty())
            {
                ret = SizeConv.convert(
                    volumesResult.getVolumes().get(0).getSize(),
                    SizeUnit.UNIT_GiB,
                    SizeUnit.UNIT_KiB
                );
            }
        }
        return ret;
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
    public void update(StorPool storPoolRef) throws AccessDeniedException, DatabaseException, StorageException
    {
        // noop
    }

    @Override
    public LocalPropsChangePojo checkConfig(StorPool storPoolRef) throws StorageException, AccessDeniedException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return false;
    }

    @Override
    public String getDevicePath(String storageNameRef, String lvIdRef)
    {
        return null;
    }

    @Override
    protected void setDevicePath(EbsData<Resource> vlmDataRef, String devicePathRef) throws DatabaseException
    {
        // noop
    }
}

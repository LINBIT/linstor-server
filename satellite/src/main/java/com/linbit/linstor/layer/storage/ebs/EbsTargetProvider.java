package com.linbit.linstor.layer.storage.ebs;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.ValueOutOfRangeException;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CreateSnapshotRequest;
import com.amazonaws.services.ec2.model.CreateSnapshotResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeResult;
import com.amazonaws.services.ec2.model.DeleteSnapshotRequest;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.DeleteVolumeRequest;
import com.amazonaws.services.ec2.model.DescribeSnapshotsRequest;
import com.amazonaws.services.ec2.model.DescribeSnapshotsResult;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.ModifyVolumeRequest;
import com.amazonaws.services.ec2.model.ResourceType;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;

@Singleton
public class EbsTargetProvider extends AbsEbsProvider<com.amazonaws.services.ec2.model.Volume>
{
    private final ResourceDefinitionMap rscDfnMap;

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
        StltSecurityObjects stltSecObjRef,
        ResourceDefinitionMap rscDfnMapRef,
        FileSystemWatch fileSystemWatchRef
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
            stltSecObjRef,
            fileSystemWatchRef
        );
        rscDfnMap = rscDfnMapRef;

        isDevPathExpectedToBeNull = true;
    }

    // @Override
    // public void clearCache() throws StorageException
    // {
    // super.clearCache();
    // }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.EBS_TARGET;
    }

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

                updateTags(vlmData, amazonVlm);
                updateVolumeType(vlmData, amazonVlm);
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
                    AbsVolume<?> absVlm = vlmDataRef.getVolume();
                    Props vlmProps = absVlm instanceof Volume ?
                        ((Volume) absVlm).getProps(storDriverAccCtx) :
                        ((SnapshotVolume) absVlm).getSnapVlmProps(storDriverAccCtx);
                    vlmProps.setProp(
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

    private void updateTags(EbsData<?> vlmDataRef, com.amazonaws.services.ec2.model.Volume amazonVlmRef)
        throws AccessDeniedException, StorageException
    {
        Map<String, String> missingTags = getEbsTags(vlmDataRef);
        List<Tag> tagsToDelete = new ArrayList<>();
        for (Tag tag : amazonVlmRef.getTags())
        {
            String amaKey = tag.getKey();
            // dont touch linstor internal tags
            if (!LINSTOR_TAGS.contains(amaKey))
            {
                String linstorValue = missingTags.get(amaKey);
                if (linstorValue == null)
                {
                    tagsToDelete.add(tag);
                }
                else if (linstorValue.equals(tag.getValue()))
                {
                    missingTags.remove(amaKey);
                }
            }
        }

        if (!missingTags.isEmpty() || !tagsToDelete.isEmpty())
        {
            List<String> vlmIdAsList = Arrays.asList(amazonVlmRef.getVolumeId());

            AmazonEC2 client = getClient(vlmDataRef.getStorPool());
            if (!missingTags.isEmpty())
            {
                client.createTags(new CreateTagsRequest(vlmIdAsList, asAmazonTagList(missingTags)));
            }
            if (!tagsToDelete.isEmpty())
            {
                client.deleteTags(new DeleteTagsRequest(vlmIdAsList).withTags(tagsToDelete));
            }
        }
    }

    private Map<String, String> getEbsTags(EbsData<?> vlmDataRef) throws AccessDeniedException
    {
        PriorityProps prioProps;
        AbsVolume<?> absVlm = vlmDataRef.getVolume();
        if (absVlm instanceof Volume)
        {
            VolumeDefinition vlmDfn = absVlm.getVolumeDefinition();
            ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();
            ResourceGroup rscGrp = rscDfn.getResourceGroup();
            prioProps = new PriorityProps(
                vlmDfn.getProps(storDriverAccCtx),
                rscDfn.getProps(storDriverAccCtx),
                rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmDfn.getVolumeNumber()),
                rscGrp.getProps(storDriverAccCtx),
                localNodeProps,
                stltConfigAccessor.getReadonlyProps()
            );
        }
        else
        {
            SnapshotVolume snapVlm = (SnapshotVolume) absVlm;
            SnapshotVolumeDefinition snapVlmDfn = snapVlm.getSnapshotVolumeDefinition();
            SnapshotDefinition snapDfn = snapVlm.getSnapshotDefinition();
            prioProps = new PriorityProps(
                snapVlmDfn.getSnapVlmDfnProps(storDriverAccCtx),
                snapVlmDfn.getVlmDfnProps(storDriverAccCtx),
                snapDfn.getSnapDfnProps(storDriverAccCtx),
                snapDfn.getRscDfnProps(storDriverAccCtx),
                localNodeProps,
                stltConfigAccessor.getReadonlyProps()
            );
        }
        return prioProps.renderRelativeMap(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS);
    }

    private ArrayList<Tag> asAmazonTagList(Map<String, String> missingTags)
    {
        ArrayList<Tag> tagListToAdd = new ArrayList<>();
        for (Map.Entry<String, String> entry : missingTags.entrySet())
        {
            tagListToAdd.add(new Tag(entry.getKey(), entry.getValue()));
        }
        return tagListToAdd;
    }

    @SuppressWarnings("unchecked")
    private void updateVolumeType(EbsData<?> vlmDataRef, com.amazonaws.services.ec2.model.Volume amazonVlmRef)
        throws StorageException, AccessDeniedException
    {
        if (vlmDataRef.getVolume() instanceof Volume)
        {
            String linstorVlmType = getVolumeType((EbsData<Resource>) vlmDataRef);
            String amaVlmType = amazonVlmRef.getVolumeType();
            if (linstorVlmType != null && !linstorVlmType.equals(amaVlmType))
            {
                getClient(vlmDataRef.getStorPool()).modifyVolume(
                    new ModifyVolumeRequest()
                        .withVolumeId(amazonVlmRef.getVolumeId())
                        .withVolumeType(linstorVlmType)
                );
            }
        }
        // else: we do not touch snapshots :)
    }

    private @Nullable String getVolumeType(EbsData<Resource> vlmDataRef) throws AccessDeniedException
    {
        return getPrioProps(vlmDataRef).getProp(ApiConsts.KEY_EBS_VOLUME_TYPE);
    }

    @Override
    protected void createLvImpl(EbsData<Resource> vlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        createEbsVolume(vlmDataRef, null);
    }

    private void createEbsVolume(EbsData<Resource> vlmDataRef, @Nullable String restoreFromSnapEbsId)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        EbsRemote remote = getEbsRemote(vlmDataRef.getStorPool());
        AmazonEC2 client = getClient(remote);

        ArrayList<Tag> tags = asAmazonTagList(getEbsTags(vlmDataRef));
        tags.add(new Tag(TAG_KEY_LINSTOR_ID, asLvIdentifier(vlmDataRef)));


        CreateVolumeRequest createVlmRequest = new CreateVolumeRequest()
            .withAvailabilityZone(remote.getAvailabilityZone(storDriverAccCtx))
            .withSize(
                (int) SizeConv.convertRoundUp(vlmDataRef.getExpectedSize(), SizeUnit.UNIT_KiB, SizeUnit.UNIT_GiB)
            )
            .withTagSpecifications(
                new TagSpecification()
                    .withResourceType(ResourceType.Volume)
                    .withTags(tags)
            );
        if (restoreFromSnapEbsId != null)
        {
            createVlmRequest.withSnapshotId(restoreFromSnapEbsId);
        }
        String vlmType = getVolumeType(vlmDataRef);
        if (vlmType != null)
        {
            createVlmRequest.withVolumeType(vlmType);
        }
        CreateVolumeResult createVolumeResult = client.createVolume(createVlmRequest);

        String ebsVlmId = createVolumeResult.getVolume().getVolumeId();
        setEbsVlmId(vlmDataRef, ebsVlmId);
        EbsProviderUtils.waitUntilVolumeHasState(client, ebsVlmId, EBS_VLM_STATE_AVAILABLE, EBS_VLM_STATE_CREATING);

        long allocatedSize = getAllocatedSize(vlmDataRef); // queries online
        vlmDataRef.setAllocatedSize(allocatedSize);
        vlmDataRef.setUsableSize(allocatedSize);
    }

    @Override
    protected boolean snapshotExists(EbsData<Snapshot> snapVlmRef, boolean ignoredForTakeSnapshorRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        boolean snapshotExists = false;

        EbsRemote remote = getEbsRemote(snapVlmRef.getStorPool());
        AmazonEC2 client = getClient(remote);
        String ebsSnapId = getEbsSnapId(snapVlmRef);
        if (ebsSnapId != null)
        {
            // no need to check if we have no ebsSnapId
            DescribeSnapshotsResult describeSnapshots = client.describeSnapshots(
                new DescribeSnapshotsRequest()
                .withSnapshotIds(ebsSnapId)
            );
            String linstorSnapId = asSnapLvIdentifier(snapVlmRef);
            for (com.amazonaws.services.ec2.model.Snapshot amaSnap : describeSnapshots.getSnapshots())
            {
                String amaTagLinstorId = getFromTags(amaSnap.getTags(), TAG_KEY_LINSTOR_ID);
                if (linstorSnapId.equals(amaTagLinstorId))
                {
                    snapshotExists = true;
                    break;
                }
            }
        }
        return snapshotExists;
    }

    @Override
    protected void createSnapshot(EbsData<Resource> vlmDataRef, EbsData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        EbsRemote remote = getEbsRemote(vlmDataRef.getStorPool());
        AmazonEC2 client = getClient(remote);

        String snapLvIdentifier = asSnapLvIdentifier(snapVlmRef);

        ArrayList<Tag> tags = asAmazonTagList(getEbsTags(vlmDataRef));
        tags.add(new Tag(TAG_KEY_LINSTOR_ID, snapLvIdentifier));

        CreateSnapshotResult createSnapshotResult = client.createSnapshot(
            new CreateSnapshotRequest()
                .withVolumeId(getEbsVlmId(vlmDataRef))
                .withDescription(snapLvIdentifier)
                .withTagSpecifications(
                    new TagSpecification()
                        .withResourceType(ResourceType.Snapshot)
                        .withTags(tags)
                )
        );
        String snapshotId = createSnapshotResult.getSnapshot().getSnapshotId();

        EbsProviderUtils.waitUntilSnapshotCreated(client, snapshotId);

        errorReporter.logTrace("EBS Snapshot created. EBS Snapshot ID: %s", snapshotId);
        setEbsSnapId(snapVlmRef, snapshotId);
        snapVlmRef.setExists(true);
    }

    @Override
    protected void restoreSnapshot(EbsData<Snapshot> sourceSnapVlmDataRef, EbsData<Resource> vlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        createEbsVolume(vlmDataRef, getEbsSnapId(sourceSnapVlmDataRef));
    }

    @Override
    protected void rollbackImpl(EbsData<Resource> vlmDataRef, EbsData<Snapshot> rollbackToSnapVlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // we will need to delete the old volume if the rollback/restore worked
        String oldEbsVlmId = getEbsVlmId(vlmDataRef);

        // will override the EbsVlmId property
        createEbsVolume(vlmDataRef, getEbsSnapId(rollbackToSnapVlmDataRef));

        AmazonEC2 client = getClient(vlmDataRef.getStorPool());

        errorReporter.logTrace("Deleting old EBS volumd ID: %s", oldEbsVlmId);
        client.deleteVolume(new DeleteVolumeRequest(oldEbsVlmId));
    }

    private EbsData<Snapshot> findSourceEbsData(
        String sourceLvIdRef,
        String sourceSnapNameRef,
        NodeName localNodeNameRef
    )
    {
        EbsData<Snapshot> ret;

        Matcher matcher = FORMAT_PATTERN.matcher(sourceLvIdRef);
        if (!matcher.find())
        {
            throw new ImplementationError("Unknown source LV ID format: " + sourceLvIdRef);
        }
        String srcRscName = matcher.group(FORMAT_PATTERN_KEY_RSC_NAME);
        String srcRscNameSuffix = matcher.group(FORMAT_PATTERN_KEY_RSC_SUFFIX);
        if (srcRscNameSuffix == null)
        {
            // regex will return null instead of "". we correct this so we can use .equals later in this method
            srcRscNameSuffix = RscLayerSuffixes.SUFFIX_DATA;
        }
        String srcVlmNrStr = matcher.group(FORMAT_PATTERN_KEY_VLM_NR);

        try
        {
            ret = findSourceEbsData(
                localNodeNameRef,
                srcRscName,
                srcRscNameSuffix,
                new VolumeNumber(Integer.parseInt(srcVlmNrStr)),
                sourceSnapNameRef
            );
        }
        catch (NumberFormatException | ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    private EbsData<Snapshot> findSourceEbsData(
        NodeName localNodeNameRef,
        String srcRscName,
        String rscNameSuffix,
        VolumeNumber srcVlmNr,
        String srcSnapNameRef
    )
    {
        EbsData<Snapshot> srcEbsData = null;
        try
        {
            ResourceDefinition srcRscDfn = rscDfnMap.get(new ResourceName(srcRscName));
            if (srcRscDfn == null)
            {
                throw new ImplementationError(
                    String.format(
                        "Unknown source resource definition [%s]",
                        srcRscName
                    )
                );
            }
            SnapshotDefinition srcSnapDfn = srcRscDfn.getSnapshotDfn(
                storDriverAccCtx,
                new SnapshotName(srcSnapNameRef)
            );
            if (srcSnapDfn == null)
            {
                throw new ImplementationError(
                    String.format(
                        "Unknown snapshot definition [%s] of resource definition [%s]",
                        srcSnapNameRef,
                        srcRscName
                    )
                );
            }
            Snapshot srcSnap = srcSnapDfn.getSnapshot(
                storDriverAccCtx,
                localNodeNameRef
            );
            if (srcSnap == null)
            {
                throw new ImplementationError(
                    String.format(
                        "Unknown snapshot [%s] of resource [%s] on node [%s]",
                        srcSnapNameRef,
                        srcRscName,
                        localNodeNameRef.displayValue
                    )
                );
            }
            Set<AbsRscLayerObject<Snapshot>> srcStorSnapDataSet = LayerRscUtils.getRscDataByLayer(
                srcSnap.getLayerData(storDriverAccCtx),
                DeviceLayerKind.STORAGE
            );

            for (AbsRscLayerObject<Snapshot> srcStorSnapData : srcStorSnapDataSet)
            {
                if (srcStorSnapData.getResourceNameSuffix().equals(rscNameSuffix))
                {
                    VlmProviderObject<Snapshot> srcVlmData = srcStorSnapData.getVlmProviderObject(srcVlmNr);
                    if (!(srcVlmData instanceof EbsData))
                    {
                        throw new ImplementationError(
                            String.format(
                                "Source volume data is of instance %s instead of EbsData!",
                                srcVlmData == null ? "null " : srcVlmData.getClass().getSimpleName()
                            )
                        );
                    }
                    srcEbsData = (EbsData<Snapshot>) srcVlmData;
                    break;
                }
            }
        }
        catch (AccessDeniedException | NumberFormatException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return srcEbsData;
    }

    @Override
    protected void deleteSnapshotImpl(EbsData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        EbsRemote remote = getEbsRemote(snapVlmRef.getStorPool());
        AmazonEC2 client = getClient(remote);

        String ebsSnapId = getEbsSnapId(snapVlmRef);
        errorReporter.logTrace("Deleting EBS snapshot. EBS Snapshot ID: %s", ebsSnapId);
        client.deleteSnapshot(new DeleteSnapshotRequest(ebsSnapId));
        snapVlmRef.setExists(false);
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

        String ebsVlmId = ((Volume) vlmDataRef.getVolume()).getProps(storDriverAccCtx)
            .getProp(
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

        String ebsVlmId = getEbsVlmId(vlmDataRef);
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
    public String getDevicePath(String storageNameRef, String lvIdRef)
    {
        return null;
    }

    @Override
    protected void setDevicePath(EbsData<Resource> vlmDataRef, String devicePathRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        return fetchOrigAllocatedSizes(vlmDataListRef);
    }
}

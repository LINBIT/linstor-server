package com.linbit.linstor.layer.storage.ebs;

import com.linbit.ImplementationError;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
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
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.ModifyVolumeRequest;
import com.amazonaws.services.ec2.model.ResourceType;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import org.slf4j.event.Level;

@Singleton
public class EbsTargetProvider extends AbsEbsProvider<com.amazonaws.services.ec2.model.Volume>
{
    /** AWS accepts at most 200 filter values in one Describe* request. */
    private static final int MAX_FILTER_VALUES_PER_REQUEST = 200;

    private final Set<String> reportedLeftBehindSnapshots;
    private final Set<String> reportedDeletedSnapshots;

    @Inject
    public EbsTargetProvider(AbsEbsProviderIniit superInitRef)
    {
        super(superInitRef, "EBS-Target", DeviceProviderKind.EBS_TARGET);
        isDevPathExpectedToBeNull = true;
        reportedLeftBehindSnapshots = new HashSet<>();
        reportedDeletedSnapshots = new HashSet<>();
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
        throws StorageException, DatabaseException
    {
        return getTargetInfoListImpl(vlmDataListRef, snapVlmsRef);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void updateStates(List<EbsData<Resource>> vlmDataListRef, List<EbsData<Snapshot>> snapVlmsRef)
        throws StorageException, DatabaseException
    {
        final List<EbsData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataListRef);

        refreshUnknownSnapshots(snapVlmsRef);

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

    private void updateInfo(EbsData<?> vlmDataRef, @Nullable com.amazonaws.services.ec2.model.Volume amazonVlmRef)
        throws DatabaseException
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
                    Props vlmProps = absVlm instanceof Volume volume ?
                        volume.getProps() :
                        ((SnapshotVolume) absVlm).getSnapVlmProps();
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
        throws StorageException
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

    private Map<String, String> getEbsTags(EbsData<?> vlmDataRef)
    {
        PriorityProps prioProps;
        AbsVolume<?> absVlm = vlmDataRef.getVolume();
        if (absVlm instanceof Volume)
        {
            VolumeDefinition vlmDfn = absVlm.getVolumeDefinition();
            ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();
            ResourceGroup rscGrp = rscDfn.getResourceGroup();
            prioProps = new PriorityProps(
                vlmDfn.getProps(),
                rscDfn.getProps(),
                rscGrp.getVolumeGroupProps(vlmDfn.getVolumeNumber()),
                rscGrp.getProps(),
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
                snapVlmDfn.getSnapVlmDfnProps(),
                snapVlmDfn.getVlmDfnProps(),
                snapDfn.getSnapDfnProps(),
                snapDfn.getRscDfnProps(),
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
        throws StorageException
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

    private @Nullable String getVolumeType(EbsData<Resource> vlmDataRef)
    {
        return getPrioProps(vlmDataRef).getProp(ApiConsts.KEY_EBS_VOLUME_TYPE);
    }

    @Override
    protected void createLvImpl(EbsData<Resource> vlmDataRef)
        throws StorageException, DatabaseException
    {
        createEbsVolume(vlmDataRef, null);
    }

    private void createEbsVolume(EbsData<Resource> vlmDataRef, @Nullable String restoreFromSnapEbsId)
        throws StorageException, DatabaseException
    {
        EbsRemote remote = getEbsRemote(vlmDataRef.getStorPool());
        AmazonEC2 client = getClient(remote);

        ArrayList<Tag> tags = asAmazonTagList(getEbsTags(vlmDataRef));
        tags.add(new Tag(TAG_KEY_LINSTOR_ID, asLvIdentifier(vlmDataRef)));


        CreateVolumeRequest createVlmRequest = new CreateVolumeRequest()
            .withAvailabilityZone(remote.getAvailabilityZone())
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
        EbsProviderUtils.waitUntilVolumeHasState(
            errorReporter, client, ebsVlmId, EBS_VLM_STATE_AVAILABLE, EBS_VLM_STATE_CREATING);

        long allocatedSize = getAllocatedSize(vlmDataRef); // queries online
        vlmDataRef.setAllocatedSize(allocatedSize);
        vlmDataRef.setUsableSize(allocatedSize);
    }

    @Override
    protected boolean snapshotExists(EbsData<Snapshot> snapVlmRef, boolean ignoredForTakeSnapshorRef)
        throws StorageException, DatabaseException
    {
        AmazonEC2 client = getClient(getEbsRemote(snapVlmRef.getStorPool()));
        String linstorSnapId = asSnapLvIdentifier(snapVlmRef);

        @Nullable com.amazonaws.services.ec2.model.Snapshot amaSnap;
        @Nullable String ebsSnapId = getEbsSnapId(snapVlmRef);
        if (ebsSnapId != null)
        {
            // a filter instead of withSnapshotIds: the latter throws InvalidSnapshot.NotFound if the snapshot was
            // deleted in the meantime, the filter simply matches nothing.
            // The stored id is authoritative, so the UUID tag is not required here (snapshots created before the tag
            // existed have none)
            amaSnap = findLinstorSnapshot(
                client,
                linstorSnapId,
                null,
                new Filter("snapshot-id").withValues(ebsSnapId)
            );
        }
        else
        {
            /*
             * No id stored yet. Besides "not created yet" this is also the state after an attempt that did create
             * the AWS snapshot but died before the id was stored (timeout while waiting for "pending", satellite
             * restart, ...). Creating another snapshot in that case leaks the first one and runs into AWS' per-volume
             * CreateSnapshot rate limit (GitHub issue #507). Every snapshot LINSTOR creates carries its tags from the
             * very first moment (they are part of the CreateSnapshot request), so look it up by them and adopt it.
             * The UUID tag is mandatory for adoption: the LinstorID is built from names, and a snapshot deleted and
             * re-created under the same name must not pick up an orphaned AWS snapshot of its predecessor.
             */
            String uuid = snapVlmUuid(snapVlmRef);
            List<Filter> filters = new ArrayList<>();
            filters.add(new Filter("tag:" + TAG_KEY_LINSTOR_SNAP_VLM_UUID).withValues(uuid));
            filters.add(new Filter("tag:" + TAG_KEY_LINSTOR_ID).withValues(linstorSnapId));
            // the snapshot volume's props are a copy of the source volume's props, including the EBS volume id
            @Nullable String srcEbsVlmId = getEbsVlmId(snapVlmRef);
            if (srcEbsVlmId != null)
            {
                filters.add(new Filter("volume-id").withValues(srcEbsVlmId));
            }
            amaSnap = findLinstorSnapshot(client, linstorSnapId, uuid, filters.toArray(new Filter[0]));
            if (amaSnap != null)
            {
                adopt(snapVlmRef, amaSnap, linstorSnapId);
            }
        }
        return amaSnap != null;
    }

    private static String snapVlmUuid(EbsData<Snapshot> snapVlmRef)
    {
        return snapVlmRef.getVolume().getUuid().toString();
    }

    /**
     * Refreshes the {@code exists} flag of the given snapshot volumes that are currently marked as not existing.
     *
     * <p>Snapshots have no local device to probe and the flag lives in memory only, so after a satellite restart every
     * snapshot is "unknown" and its deletion would be skipped, leaving the AWS snapshot behind (GitHub issue #507).
     * Snapshots already known to exist are not re-checked: {@link #createSnapshot} and {@link #deleteSnapshotImpl}
     * keep the flag up to date, so in steady state this method sends no request at all. The unknown ones are
     * resolved with at most two paged DescribeSnapshots requests per remote (one by stored id, one by the
     * snapshot-volume UUID tag), so the cost after a restart does not grow with the number of snapshots.</p>
     *
     * <p>Snapshots that have the {@link Snapshot#getTakeSnapshot()} set are skipped by this method since we know that
     * either we have not yet created that snapshot or have just created it. In both situations the current state of
     * exists is expected, so we can skip querying AWS for this snapshot</p>
     */
    private void refreshUnknownSnapshots(List<EbsData<Snapshot>> snapVlmsRef)
        throws StorageException, DatabaseException
    {
        Map<EbsRemote, List<EbsData<Snapshot>>> unknownByRemote = new HashMap<>();
        for (EbsData<Snapshot> snapVlm : snapVlmsRef)
        {
            // we also exclude snapshots with set "takeSnapshot" boolean since we know that snapshot does not exist on
            // AWS - we can skip asking AWS for that
            if (!snapVlm.exists() && !snapVlm.getVolume().getAbsResource().getTakeSnapshot())
            {
                unknownByRemote.computeIfAbsent(getEbsRemote(snapVlm.getStorPool()), ignored -> new ArrayList<>())
                    .add(snapVlm);
            }
        }

        for (Map.Entry<EbsRemote, List<EbsData<Snapshot>>> entry : unknownByRemote.entrySet())
        {
            AmazonEC2 client = getClient(entry.getKey());

            Map<String, EbsData<Snapshot>> byEbsSnapId = new HashMap<>();
            Map<String, EbsData<Snapshot>> byUuid = new HashMap<>();
            for (EbsData<Snapshot> snapVlm : entry.getValue())
            {
                @Nullable String ebsSnapId = getEbsSnapId(snapVlm);
                if (ebsSnapId != null)
                {
                    byEbsSnapId.put(ebsSnapId, snapVlm);
                }
                else
                {
                    byUuid.put(snapVlmUuid(snapVlm), snapVlm);
                }
            }

            for (List<String> ids : chunk(byEbsSnapId.keySet()))
            {
                List<com.amazonaws.services.ec2.model.Snapshot> amaSnaps = describeOwnSnapshots(
                    client,
                    new Filter("snapshot-id").withValues(ids)
                );
                for (String ebsSnapId : ids)
                {
                    EbsData<Snapshot> snapVlm = byEbsSnapId.get(ebsSnapId);
                    snapVlm.setExists(pickLinstorSnapshot(amaSnaps, asSnapLvIdentifier(snapVlm), null) != null);
                }
            }
            for (List<String> uuids : chunk(byUuid.keySet()))
            {
                List<com.amazonaws.services.ec2.model.Snapshot> amaSnaps = describeOwnSnapshots(
                    client,
                    new Filter("tag:" + TAG_KEY_LINSTOR_SNAP_VLM_UUID).withValues(uuids)
                );
                for (String uuid : uuids)
                {
                    EbsData<Snapshot> snapVlm = byUuid.get(uuid);
                    String linstorSnapId = asSnapLvIdentifier(snapVlm);
                    @Nullable com.amazonaws.services.ec2.model.Snapshot amaSnap = pickLinstorSnapshot(
                        amaSnaps,
                        linstorSnapId,
                        uuid
                    );
                    if (amaSnap != null)
                    {
                        adopt(snapVlm, amaSnap, linstorSnapId);
                    }
                    snapVlm.setExists(amaSnap != null);
                }
            }
        }
    }

    private void adopt(
        EbsData<Snapshot> snapVlmRef,
        com.amazonaws.services.ec2.model.Snapshot amaSnapRef,
        String linstorSnapId
    )
        throws DatabaseException
    {
        errorReporter.logInfo(
            "Adopting EBS snapshot %s for %s, which was created by an earlier attempt",
            amaSnapRef.getSnapshotId(),
            linstorSnapId
        );
        setEbsSnapId(snapVlmRef, amaSnapRef.getSnapshotId());
    }

    /**
     * Splits the given values into lists of at most {@link #MAX_FILTER_VALUES_PER_REQUEST} entries, the maximum
     * number of filter values AWS accepts in one Describe* request.
     */
    private static List<List<String>> chunk(Collection<String> valuesRef)
    {
        List<List<String>> ret = new ArrayList<>();
        List<String> current = new ArrayList<>();
        for (String value : valuesRef)
        {
            if (current.size() == MAX_FILTER_VALUES_PER_REQUEST)
            {
                ret.add(current);
                current = new ArrayList<>();
            }
            current.add(value);
        }
        if (!current.isEmpty())
        {
            ret.add(current);
        }
        return ret;
    }

    /**
     * Describes the snapshots matching the given filters and picks ours, see {@link #pickLinstorSnapshot}.
     */
    private @Nullable com.amazonaws.services.ec2.model.Snapshot findLinstorSnapshot(
        AmazonEC2 client,
        String linstorSnapId,
        @Nullable String requiredSnapVlmUuid,
        Filter... filters
    )
    {
        return pickLinstorSnapshot(describeOwnSnapshots(client, filters), linstorSnapId, requiredSnapVlmUuid);
    }

    /**
     * Returns the snapshot that carries the given LinstorID tag (and, if {@code requiredSnapVlmUuid} is given, also
     * the matching snapshot-volume UUID tag) and is not in "error" state, or {@code null} if there is none among the
     * given snapshots. Further matches are left untouched but reported: with equal tags they are duplicates from
     * earlier attempts, with a different or missing UUID they belong to a deleted predecessor of the same name.
     *
     * @param requiredSnapVlmUuid {@code null} when the caller already identified the snapshot by its stored id;
     *     required whenever a snapshot is looked up by name (adoption)
     */
    private @Nullable com.amazonaws.services.ec2.model.Snapshot pickLinstorSnapshot(
        List<com.amazonaws.services.ec2.model.Snapshot> amaSnapsRef,
        String linstorSnapId,
        @Nullable String requiredSnapVlmUuid
    )
    {
        @Nullable com.amazonaws.services.ec2.model.Snapshot ret = null;
        List<String> duplicates = new ArrayList<>();
        List<String> predecessors = new ArrayList<>();
        for (com.amazonaws.services.ec2.model.Snapshot amaSnap : amaSnapsRef)
        {
            if (linstorSnapId.equals(getFromTags(amaSnap.getTags(), TAG_KEY_LINSTOR_ID)))
            {
                String description = amaSnap.getSnapshotId() + " (" + amaSnap.getState() + ")";
                @Nullable String amaUuid = getFromTags(amaSnap.getTags(), TAG_KEY_LINSTOR_SNAP_VLM_UUID);
                if (requiredSnapVlmUuid != null && !requiredSnapVlmUuid.equals(amaUuid))
                {
                    predecessors.add(description);
                }
                else
                {
                    boolean errorState = EbsUtils.EBS_SNAP_STATE_ERROR.equalsIgnoreCase(amaSnap.getState());
                    if (ret == null && !errorState)
                    {
                        ret = amaSnap;
                    }
                    else
                    {
                        duplicates.add(description);
                    }
                }
            }
        }
        if (!duplicates.isEmpty())
        {
            if (reportedLeftBehindSnapshots.add(linstorSnapId))
            {
                // report the problem once per stlt restart. visible via "err list"
                errorReporter.reportProblem(
                    Level.WARN,
                    new StorageException(
                        String.format(
                            "Ignoring EBS snapshot(s) %s tagged %s=%s.",
                            duplicates,
                            TAG_KEY_LINSTOR_ID,
                            linstorSnapId
                        ),
                        null,
                        "The EBS snapshot(s) were left behind by earlier attempts.",
                        "Since LINSTOR tracks a different EBS snapshot, the above mentioned EBS snapshots can/should " +
                            "be deleted manually.",
                        null
                    ),
                    null,
                    null
                );
            }
        }
        if (!predecessors.isEmpty())
        {
            if (reportedDeletedSnapshots.add(linstorSnapId))
            {
                // report the problem once per stlt restart. visible via "err list"
                errorReporter.reportProblem(
                    Level.WARN,
                    new StorageException(
                        String.format(
                            "Ignoring EBS snapshot(s) %s tagged %s=%s.",
                            predecessors,
                            TAG_KEY_LINSTOR_ID,
                            linstorSnapId
                        ),
                        null,
                        String.format(
                            "The ignored snapshot belong to a different (deleted) LINSTOR snapshot of the " +
                                "same name (tag %s does not match %s)",
                            TAG_KEY_LINSTOR_SNAP_VLM_UUID,
                            requiredSnapVlmUuid
                        ),
                        "Since this LINSTOR snapshot has no corresponding EBS snapshot, the LINSTOR " +
                            "snapshot can be deleted safely.",
                        null
                    ),
                    null,
                    null
                );
            }
        }
        return ret;
    }

    /**
     * Describes all snapshots owned by this account that match the given filters, following the pagination.
     */
    private List<com.amazonaws.services.ec2.model.Snapshot> describeOwnSnapshots(AmazonEC2 client, Filter... filters)
    {
        List<com.amazonaws.services.ec2.model.Snapshot> ret = new ArrayList<>();
        @Nullable String nextToken = null;
        do
        {
            DescribeSnapshotsResult describeSnapshots = client.describeSnapshots(
                new DescribeSnapshotsRequest()
                    .withOwnerIds("self")
                    .withFilters(filters)
                    .withNextToken(nextToken)
            );
            nextToken = describeSnapshots.getNextToken();
            ret.addAll(describeSnapshots.getSnapshots());
        }
        while (nextToken != null && !nextToken.isEmpty());
        return ret;
    }

    @Override
    protected void createSnapshot(EbsData<Resource> vlmDataRef, EbsData<Snapshot> snapVlmRef, boolean readOnly)
        throws StorageException, DatabaseException
    {
        EbsRemote remote = getEbsRemote(vlmDataRef.getStorPool());
        AmazonEC2 client = getClient(remote);

        String snapLvIdentifier = asSnapLvIdentifier(snapVlmRef);

        ArrayList<Tag> tags = asAmazonTagList(getEbsTags(vlmDataRef));
        tags.add(new Tag(TAG_KEY_LINSTOR_ID, snapLvIdentifier));
        tags.add(new Tag(TAG_KEY_LINSTOR_SNAP_VLM_UUID, snapVlmUuid(snapVlmRef)));

        CreateSnapshotResult createSnapshotResult = client.createSnapshot(
            new CreateSnapshotRequest()
                .withVolumeId(getEbsVlmIdNonNull(vlmDataRef))
                .withDescription(snapLvIdentifier)
                .withTagSpecifications(
                    new TagSpecification()
                        .withResourceType(ResourceType.Snapshot)
                        .withTags(tags)
                )
        );
        String snapshotId = createSnapshotResult.getSnapshot().getSnapshotId();
        // store the id before waiting: should the wait fail or the satellite die, the next attempt has to find this
        // snapshot (see snapshotExists) instead of creating a second one
        setEbsSnapId(snapVlmRef, snapshotId);

        EbsProviderUtils.waitUntilSnapshotCreatedOrPending(errorReporter, client, snapshotId);

        errorReporter.logTrace("EBS Snapshot created. EBS Snapshot ID: %s", snapshotId);
        snapVlmRef.setExists(true);
    }

    @Override
    protected void restoreSnapshot(EbsData<Snapshot> sourceSnapVlmDataRef, EbsData<Resource> vlmDataRef)
        throws StorageException, DatabaseException
    {
        createEbsVolume(vlmDataRef, getEbsSnapId(sourceSnapVlmDataRef));
    }

    @Override
    protected void rollbackImpl(EbsData<Resource> vlmDataRef, EbsData<Snapshot> rollbackToSnapVlmDataRef)
        throws StorageException, DatabaseException
    {
        // we will need to delete the old volume if the rollback/restore worked
        String oldEbsVlmId = getEbsVlmIdNonNull(vlmDataRef);

        // will override the EbsVlmId property
        createEbsVolume(vlmDataRef, getEbsSnapId(rollbackToSnapVlmDataRef));

        AmazonEC2 client = getClient(vlmDataRef.getStorPool());

        errorReporter.logTrace("Deleting old EBS volumd ID: %s", oldEbsVlmId);
        client.deleteVolume(new DeleteVolumeRequest(oldEbsVlmId));
    }

    @Override
    protected void deleteSnapshotImpl(EbsData<Snapshot> snapVlmRef)
        throws StorageException, DatabaseException
    {
        EbsRemote remote = getEbsRemote(snapVlmRef.getStorPool());
        AmazonEC2 client = getClient(remote);

        @Nullable String ebsSnapId = getEbsSnapId(snapVlmRef);
        if (ebsSnapId == null)
        {
            // snapshotExists() adopts snapshots by their LinstorID tag, so without an id AWS has no such snapshot
            errorReporter.logTrace(
                "No EBS snapshot id stored for %s, nothing to delete in AWS",
                asSnapLvIdentifier(snapVlmRef)
            );
        }
        else
        {
            errorReporter.logTrace("Deleting EBS snapshot. EBS Snapshot ID: %s", ebsSnapId);
            client.deleteSnapshot(new DeleteSnapshotRequest(ebsSnapId));
        }
        snapVlmRef.setExists(false);
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
        AmazonEC2 client = getClient(vlmDataRef.getStorPool());

        String ebsVlmId = ((Volume) vlmDataRef.getVolume()).getProps()
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
        throws StorageException, DatabaseException
    {
        AmazonEC2 client = getClient(vlmDataRef.getStorPool());
        String ebsVlmId = getEbsVlmIdNonNull(vlmDataRef);
        errorReporter.logTrace("Deleting EBS volumd ID: %s", ebsVlmId);
        client.deleteVolume(new DeleteVolumeRequest(ebsVlmId));
        vlmDataRef.setExists(false);
    }

    @Override
    protected void deactivateLvImpl(EbsData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, DatabaseException
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

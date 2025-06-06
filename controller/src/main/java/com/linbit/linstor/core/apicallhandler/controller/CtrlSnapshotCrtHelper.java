package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotControllerFactory;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionControllerFactory;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotVlmDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.linstor.utils.layer.LayerVlmUtils.getStorPoolMap;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class CtrlSnapshotCrtHelper
{
    private final AccessContext apiCtx;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SnapshotDefinitionControllerFactory snapshotDefinitionFactory;
    private final SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactory;
    private final SnapshotControllerFactory snapshotFactory;
    private final SnapshotVolumeControllerFactory snapshotVolumeControllerFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlPropsHelper ctrlPropsHelper;

    @Inject
    public CtrlSnapshotCrtHelper(
        @ApiContext AccessContext apiCtxRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SnapshotDefinitionControllerFactory snapshotDefinitionFactoryRef,
        SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactoryRef,
        SnapshotControllerFactory snapshotFactoryRef,
        SnapshotVolumeControllerFactory snapshotVolumeControllerFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlPropsHelper ctrlPropsHelperRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapshotDefinitionFactory = snapshotDefinitionFactoryRef;
        snapshotVolumeDefinitionControllerFactory = snapshotVolumeDefinitionControllerFactoryRef;
        snapshotFactory = snapshotFactoryRef;
        snapshotVolumeControllerFactory = snapshotVolumeControllerFactoryRef;
        peerAccCtx = peerAccCtxRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
    }

    public SnapshotDefinition createSnapshots(
        Collection<String> nodeNameStrs,
        ResourceName rscName,
        SnapshotName snapshotName,
        ApiCallRcImpl responses
    )
    {
        final ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);

        SnapshotDefinition snapshotDfn = createSnapshotDfnData(
            rscDfn,
            snapshotName,
            new SnapshotDefinition.Flags[] {}
        );
        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(rscDfn),
            ctrlPropsHelper.getProps(snapshotDfn, true)
        );

        ensureSnapshotsViable(rscDfn);

        setInCreation(snapshotDfn);

        Iterator<VolumeDefinition> vlmDfnIterator = iterateVolumeDfn(rscDfn);
        List<SnapshotVolumeDefinition> snapshotVolumeDefinitions = new ArrayList<>();
        while (vlmDfnIterator.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIterator.next();

            SnapshotVolumeDefinition snapshotVlmDfn = createSnapshotVlmDfnData(snapshotDfn, vlmDfn);
            snapshotVolumeDefinitions.add(snapshotVlmDfn);

            ctrlPropsHelper.copy(
                ctrlPropsHelper.getProps(vlmDfn),
                ctrlPropsHelper.getProps(snapshotVlmDfn, true)
            );
        }

        boolean resourceFound = false;
        if (nodeNameStrs.isEmpty())
        {
            Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();

                if (!isDisklessPrivileged(rsc))
                {
                    if (isEvacuatingPrivileged(rsc))
                    {
                        warnNodeEvacuating(rscName.displayValue, responses, rsc.getNode().getName().displayValue);
                    }
                    else if (!isNodeOnline(rsc))
                    {
                        warnNodeOffline(rscName.displayValue, responses, rsc.getNode().getName().displayValue);
                    }
                    else
                    {
                        Snapshot snap = createSnapshotOnNode(snapshotDfn, snapshotVolumeDefinitions, rsc);
                        setNodeIds(rsc, snap);
                        resourceFound = true;
                    }
                }
            }
        }
        else
        {
            for (String nodeNameStr : nodeNameStrs)
            {
                Resource rsc = ctrlApiDataLoader.loadRsc(rscDfn, nodeNameStr, true);

                if (isDisklessPrivileged(rsc))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                            "Cannot create snapshot from diskless resource on node '" + nodeNameStr + "'"
                        )
                    );
                }
                if (isEvacuatingPrivileged(rsc))
                {
                    warnNodeEvacuating(rscName.displayValue, responses, nodeNameStr);
                }
                else
                {
                    Snapshot snap = createSnapshotOnNode(snapshotDfn, snapshotVolumeDefinitions, rsc);
                    setNodeIds(rsc, snap);
                    resourceFound = true;
                }
            }
        }

        if (!resourceFound)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_RSC,
                    "No resources found for snapshotting"
                )
            );
        }

        Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIterator.hasNext())
        {
            Resource rsc = rscIterator.next();
            setSuspend(rsc, true);
        }

        return snapshotDfn;
    }

    private void warnNodeOffline(String rscNameStr, ApiCallRcImpl responses, String nodeNameStr)
    {
        responses.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_WARN,
                "Snapshot for resource '" + rscNameStr + "' will not be created on node '" + nodeNameStr +
                    "' because that node is currently offline."
            )
        );
    }

    private void warnNodeEvacuating(String rscNameStr, ApiCallRcImpl responses, String nodeNameStr)
    {
        responses.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_WARN,
                "Snapshot for resource '" + rscNameStr + "' will not be created on node '" + nodeNameStr +
                    "' because that node is currently evacuating."
            )
        );
    }

    private void setNodeIds(Resource rsc, Snapshot snap)
    {
        List<Integer> nodeIds = new ArrayList<>();
        try
        {
            Set<AbsRscLayerObject<Resource>> drbdLayers = LayerRscUtils
                .getRscDataByLayer(rsc.getLayerData(apiCtx), DeviceLayerKind.DRBD);

            if (drbdLayers.size() > 1)
            {
                throw new ImplementationError("Only one instance of DRBD-layer supported");
            }

            for (AbsRscLayerObject<Resource> layer : drbdLayers)
            {
                DrbdRscData<Resource> drbdLayer = (DrbdRscData<Resource>) layer;
                boolean intMeta = false;
                for (DrbdVlmData<Resource> drbdVlm : drbdLayer.getVlmLayerObjects().values())
                {
                    if (!drbdVlm.isUsingExternalMetaData())
                    {
                        intMeta = true;
                    }
                }
                if (intMeta)
                {
                    for (DrbdRscData<Resource> rscData : drbdLayer.getRscDfnLayerObject().getDrbdRscDataList())
                    {
                        if (!rscData.isDiskless(apiCtx))
                        {
                            /*
                             * diskless nodes do reserve a node-id for themselves, but the peer-slot is not used in the
                             * metadata of diskfull peers
                             */
                            nodeIds.add(rscData.getNodeId().value);
                        }
                    }
                }
            }
            snap.getSnapshotDefinition()
                .getSnapDfnProps(apiCtx)
                .setProp(
                    InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
                    StringUtils.join(nodeIds, InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR),
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "create snapshot", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Snapshot createSnapshotOnNode(
        SnapshotDefinition snapshotDfn,
        Collection<SnapshotVolumeDefinition> snapshotVolumeDefinitions,
        Resource rsc
    )
    {
        Snapshot snapshot = createSnapshot(snapshotDfn, rsc);
        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(rsc),
            ctrlPropsHelper.getProps(snapshot, true)
        );

        setSuspend(snapshot);

        for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitions)
        {
            SnapshotVolume snapVlm = createSnapshotVolume(rsc, snapshot, snapshotVolumeDefinition);

            ctrlPropsHelper.copy(
                ctrlPropsHelper.getProps(rsc.getVolume(snapshotVolumeDefinition.getVolumeNumber())),
                ctrlPropsHelper.getProps(snapVlm, true)
            );
        }
        return snapshot;
    }

    private void ensureSnapshotsViable(ResourceDefinition rscDfn)
    {
        Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
        // ctrl, node, sp, rg, rd, r
        int diskFullConnected = 0;
        while (rscIterator.hasNext())
        {
            Resource currentRsc = rscIterator.next();
            try
            {
                Set<AbsRscLayerObject<Resource>> drbdLayerDataSet = LayerRscUtils.getRscDataByLayer(
                    currentRsc.getLayerData(apiCtx),
                    DeviceLayerKind.DRBD
                );
                ReadOnlyProps stltProps = ctrlPropsHelper.getStltPropsForView();
                for (AbsRscLayerObject<Resource> drbdLayerData : drbdLayerDataSet)
                {
                    DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) drbdLayerData;
                    if (drbdRscData.isSkipDiskEnabled(apiCtx, stltProps))
                    {
                        throw new ApiRcException(ApiCallRcImpl
                            .entryBuilder(
                                ApiConsts.FAIL_SNAPSHOT_NOT_UPTODATE,
                                "SkipDisk is enabled for resource " + rscDfn.getName()
                            )
                            .setDetails(
                                "Snapshots are not allowed while SkipDisk is enabled, " +
                                    "because upToDate-state cannot be ensured."
                            )
                            .build()
                        );
                    }
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            ensureDriversSupportSnapshots(currentRsc);
            if (!isDisklessPrivileged(currentRsc) && ctrlSnapshotHelper.satelliteConnected(currentRsc))
            {
                diskFullConnected++;
            }
        }

        if (diskFullConnected == 0)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_CONNECTED,
                    "No diskful connected satellite for snapshot or no resources."
                )
                .setDetails("Snapshots need at least one diskful online satellite.")
                .build()
            );
        }
    }

    private void ensureDriversSupportSnapshots(Resource rsc)
    {
        try
        {
            if (!isDisklessPrivileged(rsc))
            {
                Iterator<Volume> vlmIterator = rsc.iterateVolumes();
                while (vlmIterator.hasNext())
                {
                    Volume vlm = vlmIterator.next();
                    Map<String, StorPool> storPoolMap = getStorPoolMap(
                        vlm,
                        apiCtx
                    );

                    for (StorPool storPool : storPoolMap.values())
                    {
                        DeviceProviderKind providerKind = storPool.getDeviceProviderKind();
                        boolean supportsSnapshot = storPool.isSnapshotSupported(apiCtx);

                        if (!supportsSnapshot)
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.entryBuilder(
                                    ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                                    "Storage driver '" + providerKind + "' " + "does not support snapshots."
                                ).setDetails(
                                    "Used for storage pool '" + storPool.getName() + "'" +
                                        " on '" + rsc.getNode().getName() + "'."
                                ).build()
                            );
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private boolean isDisklessPrivileged(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            StateFlags<Flags> stateFlags = rsc.getStateFlags();
            isDiskless = stateFlags.isSomeSet(
                apiCtx,
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            );
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    private boolean isNodeOnline(Resource rsc)
    {
        return ctrlSnapshotHelper.satelliteConnected(rsc);
    }

    private boolean isEvacuatingPrivileged(Resource rsc)
    {
        boolean isEvacuating;
        try
        {
            isEvacuating = rsc.getNode().getFlags().isSet(apiCtx, Node.Flags.EVACUATE);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return isEvacuating;
    }

    public SnapshotDefinition createSnapshotDfnData(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDefinition.Flags[] snapshotDfnInitFlags
    )
    {
        SnapshotDefinition snapshotDfn;
        try
        {
            snapshotDfn = snapshotDefinitionFactory.create(
                peerAccCtx.get(),
                rscDfn,
                snapshotName,
                snapshotDfnInitFlags
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotDfnDescriptionInline(rscDfn.getName().displayValue, snapshotName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                    String.format(
                        "A snapshot definition with the name '%s' already exists in resource definition '%s'.",
                        snapshotName,
                        rscDfn.getName().displayValue
                    ),
                    true
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapshotDfn;
    }

    public SnapshotVolumeDefinition createSnapshotVlmDfnData(SnapshotDefinition snapshotDfn, VolumeDefinition vlmDfn)
    {
        String descriptionInline = getSnapshotVlmDfnDescriptionInline(
            snapshotDfn.getResourceName().displayValue,
            snapshotDfn.getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
        long volumeSize = getVolumeSize(vlmDfn);

        SnapshotVolumeDefinition snapshotVlmDfn;
        try
        {
            snapshotVlmDfn = snapshotVolumeDefinitionControllerFactory.create(
                peerAccCtx.get(),
                snapshotDfn,
                vlmDfn,
                volumeSize,
                new SnapshotVolumeDefinition.Flags[] {}
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + descriptionInline,
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                    String.format(
                        "Volume %d of snapshot definition with the name '%s' already exists in " +
                            "resource definition '%s'.",
                        vlmDfn.getVolumeNumber().value,
                        snapshotDfn.getName().displayValue,
                        snapshotDfn.getResourceName().displayValue
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (MdException mdExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_SIZE,
                    String.format(
                        "The %s has an invalid size of '%d'. Valid sizes range from %d to %d.",
                        descriptionInline,
                        volumeSize,
                        MetaData.DRBD_MIN_NET_kiB,
                        MetaData.DRBD_MAX_kiB
                    )
                ),
                mdExc
            );
        }
        return snapshotVlmDfn;
    }

    private Snapshot createSnapshot(SnapshotDefinition snapshotDfn, Resource rsc)
    {
        String snapshotNameStr = snapshotDfn.getName().displayValue;
        String rscNameStr = rsc.getResourceDefinition().getName().displayValue;
        String nodeNameStr = rsc.getNode().getName().displayValue;

        Snapshot snapshot;
        try
        {
            snapshot = snapshotFactory.create(
                peerAccCtx.get(),
                rsc,
                snapshotDfn,
                new Snapshot.Flags[0]
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotDescriptionInline(
                    Collections.singletonList(nodeNameStr),
                    rscNameStr,
                    snapshotNameStr
                ),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                    String.format(
                        "A snapshot with the name '%s' of the resource '%s' on '%s' already exists.",
                        snapshotNameStr,
                        rscNameStr,
                        nodeNameStr
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapshot;
    }

    public Snapshot restoreSnapshot(
        SnapshotDefinition snapshotDfn,
        Node node,
        RscLayerDataApi layerData,
        Map<String, String> renameStorPoolsMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        String snapshotNameStr = snapshotDfn.getName().displayValue;
        String rscNameStr = snapshotDfn.getResourceName().displayValue;
        String nodeName = node.getName().displayValue;

        Snapshot snapshot;
        try
        {
            snapshot = snapshotFactory.restore(
                apiCtx,
                layerData,
                node,
                snapshotDfn,
                new Snapshot.Flags[0],
                renameStorPoolsMap,
                apiCallRc
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotDescriptionInline(
                    Collections.singletonList(nodeName),
                    rscNameStr,
                    snapshotNameStr
                ),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                    String.format(
                        "A snapshot with the name '%s' of the resource '%s' on '%s' already exists.",
                        snapshotNameStr,
                        rscNameStr,
                        nodeName
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapshot;
    }

    private SnapshotVolume createSnapshotVolume(
        Resource rsc,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
    {
        SnapshotVolume snapVlm;
        try
        {
            snapVlm = snapshotVolumeControllerFactory.create(
                peerAccCtx.get(),
                rsc,
                snapshot,
                snapshotVolumeDefinition
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotVlmDescriptionInline(
                    snapshot.getNodeName(),
                    snapshot.getResourceName(),
                    snapshot.getSnapshotName(),
                    snapshotVolumeDefinition.getVolumeNumber()
                ),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                    String.format(
                        "Volume %d of snapshot '%s' of the resource '%s' on '%s' already exists.",
                        snapshotVolumeDefinition.getVolumeNumber().value,
                        snapshot.getSnapshotName(),
                        snapshot.getResourceName(),
                        snapshot.getNodeName()
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapVlm;
    }

    public SnapshotVolume restoreSnapshotVolume(
        RscLayerDataApi layerData,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        Map<String, String> renameStorPoolsMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        SnapshotVolume snapVlm;
        try
        {
            snapVlm = snapshotVolumeControllerFactory.restore(
                apiCtx,
                layerData,
                snapshot,
                snapshotVolumeDefinition,
                renameStorPoolsMap,
                apiCallRc
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotVlmDescriptionInline(
                    snapshot.getNodeName(),
                    snapshot.getResourceName(),
                    snapshot.getSnapshotName(),
                    snapshotVolumeDefinition.getVolumeNumber()
                ),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                    String.format(
                        "Volume %d of snapshot '%s' of the resource '%s' on '%s' already exists.",
                        snapshotVolumeDefinition.getVolumeNumber().value,
                        snapshot.getSnapshotName(),
                        snapshot.getResourceName(),
                        snapshot.getNodeName()
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapVlm;
    }

    @Deprecated
    private void setSuspend(Snapshot snapshot)
    {
        try
        {
            snapshot.setSuspendResource(peerAccCtx.get(), true);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set resource suspension for " + getSnapshotDescriptionInline(snapshot),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
    }

    private void setSuspend(Resource rsc, boolean suspend)
    {
        try
        {
            rsc.getLayerData(peerAccCtx.get()).setShouldSuspendIo(suspend);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set resource suspension for " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private long getVolumeSize(VolumeDefinition vlmDfn)
    {
        long volumeSize;
        try
        {
            volumeSize = vlmDfn.getVolumeSize(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get size of " + getVlmDfnDescriptionInline(vlmDfn),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return volumeSize;
    }

    private Iterator<VolumeDefinition> iterateVolumeDfn(ResourceDefinition rscDfn)
    {
        Iterator<VolumeDefinition> vlmDfnIter;
        try
        {
            vlmDfnIter = rscDfn.iterateVolumeDfn(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "iterate the volume definitions of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return vlmDfnIter;
    }

    private void setInCreation(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.setInCreation(peerAccCtx.get(), true);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getSnapshotDfnDescriptionInline(snapshotDfn) + " in creation",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

}

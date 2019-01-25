package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.pojo.VlmUpdatePojo;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RscInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peer;

    private final NodeRepository nodeRepository;
    private final ResourceDefinitionRepository resourceDefinitionRepository;

    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    @Inject
    public RscInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        nodeRepository = nodeRepositoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peer = peerRef;

        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
    }

    public void handleResourceRequest(
        String nodeNameStr,
        UUID rscUuid,
        String rscNameStr
    )
    {
        try (
            LockGuard ls = LockGuard.createLocked(
                nodesMapLock.readLock(),
                rscDfnMapLock.readLock(),
                storPoolDfnMapLock.readLock(),
                peer.get().getSerializerLock().readLock()
            )
        )
        {
            NodeName nodeName = new NodeName(nodeNameStr);

            Node node = nodeRepository.get(apiCtx, nodeName);

            if (node != null)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                Resource rsc = !node.isDeleted() ? node.getResource(apiCtx, rscName) : null;

                long fullSyncTimestamp = peer.get().getFullSyncId();
                long updateId = peer.get().getNextSerializerId();
                // TODO: check if the localResource has the same uuid as rscUuid
                if (rsc != null && !rsc.isDeleted())
                {
                    peer.get().sendMessage(
                        ctrlStltSerializer
                            .onewayBuilder(InternalApiConsts.API_APPLY_RSC)
                            .resourceData(rsc, fullSyncTimestamp, updateId)
                            .build()
                    );
                }
                else
                {
                    peer.get().sendMessage(
                        ctrlStltSerializer
                            .onewayBuilder(InternalApiConsts.API_APPLY_RSC_DELETED)
                            .deletedResourceData(rscNameStr, fullSyncTimestamp, updateId)
                            .build()
                    );
                }
            }
            else
            {
                errorReporter.reportError(
                    new ImplementationError(
                        "Satellite requested resource '" + rscNameStr + "' on node '" + nodeNameStr + "' " +
                            "but that node does not exist.",
                        null
                    )
                );
                peer.get().closeConnection();
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Satellite requested data for invalid name (node or rsc name).",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested resource data.",
                    accDeniedExc
                )
            );
        }
    }

    public void updateVolumeData(
        String resourceName,
        List<VlmUpdatePojo> vlmUpdates,
        List<CapacityInfoPojo> capacityInfos
    )
    {
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.readLock(), rscDfnMapLock.writeLock()))
        {
            NodeName nodeName = peer.get().getNode().getName();
            Map<StorPoolName, CapacityInfoPojo> storPoolToCapacityInfoMap = capacityInfos.stream().collect(
                Collectors.toMap(
                    freeSpacePojo -> LinstorParsingUtils.asStorPoolName(freeSpacePojo.getStorPoolName()),
                    Function.identity()
                )
            );
            ResourceDefinition rscDfn = resourceDefinitionRepository.get(apiCtx, new ResourceName(resourceName));
            Resource rsc = rscDfn.getResource(apiCtx, nodeName);

            for (VlmUpdatePojo vlmUpd : vlmUpdates)
            {
                try
                {
                    Volume vlm = rsc.getVolume(new VolumeNumber(vlmUpd.getVolumeNumber()));
                    if (vlm != null)
                    {
                        vlm.setBackingDiskPath(apiCtx, vlmUpd.getBlockDevicePath());
                        vlm.setMetaDiskPath(apiCtx, vlmUpd.getMetaDiskPath());
                        vlm.setDevicePath(apiCtx, vlmUpd.getDevicePath());
                        vlm.setUsableSize(apiCtx, vlmUpd.getUsableSize());
                        vlm.setAllocatedSize(apiCtx, vlmUpd.getAllocatedSize());

                        Props vlmDfnProps = vlm.getVolumeDefinition().getProps(apiCtx);
                        vlmDfnProps.map().putAll(vlmUpd.getVlmDfnPropsMap());
                        vlmDfnProps.keySet().retainAll(vlmUpd.getVlmDfnPropsMap().keySet());

                        StorPool storPool = vlm.getStorPool(apiCtx);
                        CapacityInfoPojo capacityInfo =
                            storPoolToCapacityInfoMap.get(storPool.getName());

                        storPool.getFreeSpaceTracker().vlmCreationFinished(
                            apiCtx,
                            vlm,
                            capacityInfo == null ? null : capacityInfo.getFreeCapacity(),
                            capacityInfo == null ? null : capacityInfo.getTotalCapacity()
                        );

                        if (capacityInfo == null && !storPool.getDriverKind().usesThinProvisioning())
                        {
                            errorReporter.logWarning(
                                String.format(
                                    "No freespace info for storage pool '%s' on node: %s",
                                    storPool.getName().value,
                                    nodeName.displayValue
                                )
                            );
                        }

                    }
                    else
                    {
                        errorReporter.logWarning(
                            String.format(
                                "Tried to update a non existing volume. Node: %s, Resource: %s, VolumeNr: %d",
                                nodeName.displayValue,
                                rscDfn.getName().displayValue,
                                vlmUpd.getVolumeNumber()
                            )
                        );
                    }
                }
                catch (ValueOutOfRangeException ignored)
                {
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (InvalidNameException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }
}

package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerHelperUtils;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.StorageVlmPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.exos.ExosData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.ExosMappingManager;
import com.linbit.linstor.storage.utils.LayerDataFactory;
import com.linbit.linstor.utils.NameShortener;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class RscStorageLayerHelper extends
    AbsRscLayerHelper<
    StorageRscData<Resource>, VlmProviderObject<Resource>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    @Deprecated(forRemoval = true)
    private final NameShortener exosNameShortener;
    private final ExosMappingManager exosMapMgr;
    private final CtrlSecurityObjects secObjs;
    private final RemoteMap remoteMap;

    @Inject
    RscStorageLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL)  DynamicNumberPool layerRscIdPoolRef,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        @Named(NameShortener.EXOS) NameShortener exosNameShortenerRef,
        ExosMappingManager exosMapMgrRef,
        CtrlSecurityObjects secObjsRef,
        RemoteMap remoteMapRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            // StorageRscData.class cannot directly be casted to Class<StorageRscData<Resource>>. because java.
            // its type is Class<StorageRscData> (without nested types), but that is not enough as the super constructor
            // wants a Class<RSC_PO>, where RSC_PO is StorageRscData<Resource>.
            (Class<StorageRscData<Resource>>) ((Object) StorageRscData.class),
            DeviceLayerKind.STORAGE,
            rscLayerDataFactory
        );
        storPoolResolveHelper = storPoolResolveHelperRef;
        exosNameShortener = exosNameShortenerRef;
        exosMapMgr = exosMapMgrRef;
        secObjs = secObjsRef;
        remoteMap = remoteMapRef;
    }

    @Override
    protected @Nullable RscDfnLayerObject createRscDfnData(
        ResourceDefinition rscDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // StorageLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected void mergeRscDfnData(RscDfnLayerObject rscDfnRef, LayerPayload payloadRef)
    {
        // no Storage specific resource-definition, nothing to merge
    }

    @Override
    protected @Nullable VlmDfnLayerObject createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // StorageLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected void mergeVlmDfnData(VlmDfnLayerObject vlmDfnDataRef, LayerPayload payloadRef)
    {
        // no Storage specific volume-definition, nothing to merge
    }

    @Override
    protected StorageRscData<Resource> createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        return layerDataFactory.createStorageRscData(
            layerRscIdPool.autoAllocate(),
            parentObjectRef,
            rscRef,
            rscNameSuffixRef
        );
    }

    @Override
    protected List<ChildResourceData> getChildRsc(
        StorageRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidKeyException
    {
        return Collections.emptyList(); // no children.
    }

    @Override
    protected void mergeRscData(StorageRscData<Resource> rscDataRef, LayerPayload payloadRef)
    {
        // nothing to merge
    }

    @Override
    protected boolean needsChildVlm(AbsRscLayerObject<Resource> childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
    {
        throw new ImplementationError("Storage layer should not have child volumes to be asked for");
    }

    @Override
    protected Set<StorPool> getNeededStoragePools(
        Resource rsc,
        VolumeDefinition vlmDfn,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidNameException
    {
        Set<StorPool> neededStorPools = new HashSet<>();

        boolean resolveSp = false;

        AbsRscLayerObject<Resource> rscData = rsc.getLayerData(apiCtx);
        /*
         * If we are creating a (diskless?) resource, we might need to resolve storage pool or not.
         * If we are toggling disk we *must* resolve storage pools
         *
         * If we are creating a (diskless?) resource, we did not create StorRscData yet. (boolean will be false)
         * If we are toggling disk, we already have StorRscData from a previous run (boolean will be true)
         */
        boolean rscToggleDiskOrCreation = rscData != null &&
            !LayerRscUtils.getRscDataByLayer(rscData, DeviceLayerKind.STORAGE).isEmpty();

        for (StorageVlmPayload storageVlmPayload : payloadRef.storagePayload.values())
        {
            StorPool storPool = storageVlmPayload.storPool;
            if (storPool != null)
            {
                neededStorPools.add(storPool);
            }
            else
            {
                resolveSp = true;
            }
        }

        if (rscToggleDiskOrCreation || resolveSp)
        {
            CtrlRscLayerDataFactory ctrlRscLayerDataFactory = layerDataHelperProvider.get();
            StorPool resolvedStorPool = storPoolResolveHelper.resolveStorPool(
                apiCtx,
                rsc,
                vlmDfn,
                ctrlRscLayerDataFactory.isDiskless(rsc) && !ctrlRscLayerDataFactory.isDiskAddRequested(rsc),
                ctrlRscLayerDataFactory.isDiskRemoving(rsc),
                false
            ).extractApiCallRc(new ApiCallRcImpl());
            if (resolvedStorPool != null)
            {
                neededStorPools.add(resolvedStorPool);
            }
        }

        return neededStorPools;
    }

    @Override
    protected VlmProviderObject<Resource> createVlmLayerData(
        StorageRscData<Resource> rscData,
        Volume vlm,
        LayerPayload payload,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException, InvalidKeyException, InvalidNameException
    {
        StorPool storPool = layerDataHelperProvider.get().getStorPool(vlm, rscData, payload);

        DeviceProviderKind kind = storPool.getDeviceProviderKind();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VlmProviderObject<Resource> vlmData = rscData.getVlmProviderObject(
            vlmDfn.getVolumeNumber()
        );
        if (vlmData == null)
        {
            switch (kind)
            {
                case DISKLESS:
                    vlmData = layerDataFactory.createDisklessData(
                        vlm,
                        vlmDfn.getVolumeSize(apiCtx),
                        rscData,
                        storPool
                    );
                    break;
                case LVM:
                    vlmData = layerDataFactory.createLvmData(vlm, rscData, storPool);
                    break;
                case LVM_THIN:
                    vlmData = layerDataFactory.createLvmThinData(vlm, rscData, storPool);
                    break;
                case STORAGE_SPACES: // fall-through
                case STORAGE_SPACES_THIN:
                    vlmData = layerDataFactory.createStorageSpacesData(vlm, rscData, kind, storPool);
                    break;
                case ZFS: // fall-through
                case ZFS_THIN:
                    vlmData = layerDataFactory.createZfsData(vlm, rscData, kind, storPool);
                    break;
                case FILE: // fall-through
                case FILE_THIN:
                    vlmData = layerDataFactory.createFileData(vlm, rscData, kind, storPool);
                    break;
                case SPDK:
                case REMOTE_SPDK:
                    if (rscData.getParent() == null || !rscData.getParent().getLayerKind().equals(DeviceLayerKind.NVME))
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_LAYER_STACK,
                                "SPDK storage requires NVME layer directly above"
                            )
                        );
                    }
                    vlmData = layerDataFactory.createSpdkData(vlm, rscData, kind, storPool);
                    break;
                case EXOS:
                    exosNameShortener.shorten(
                        vlmDfn,
                        storPool.getSharedStorPoolName().displayValue,
                        rscData.getResourceNameSuffix(),
                        false
                    );
                    try
                    {
                        exosMapMgr.findFreeExosPortAndLun(storPool, vlm);
                    }
                    catch (InvalidKeyException | InvalidValueException exc)
                    {
                        throw new ImplementationError(exc);
                    }

                    ExosData<Resource> exosData = layerDataFactory.createExosData(vlm, rscData, storPool);
                    exosData.updateShortName(apiCtx);
                    vlmData = exosData;
                    break;
                case EBS_TARGET:
                    vlmData = layerDataFactory.createEbsData(
                        vlm,
                        rscData,
                        storPool
                    );
                    break;
                case EBS_INIT:
                    PairNonNull<String, Resource> unusedTargetEbsPair = findUnusedTargetEbsPair(
                        apiCtx,
                        remoteMap,
                        rscData,
                        vlm,
                        storPool
                    );

                    String unusedTargetEbsVlmId = unusedTargetEbsPair.objA;
                    Resource targetRsc = unusedTargetEbsPair.objB;
                    vlmData = layerDataFactory.createEbsData(
                        vlm,
                        rscData,
                        storPool
                    );
                    try
                    {
                        vlm.getProps(apiCtx).setProp(
                            InternalApiConsts.KEY_EBS_VLM_ID + rscData.getResourceNameSuffix(),
                            unusedTargetEbsVlmId,
                            ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
                        );
                        Props rscProp = targetRsc.getProps(apiCtx);
                        // double check, just to be sure
                        String connectedInitNodeName = rscProp.getProp(
                            InternalApiConsts.KEY_EBS_CONNECTED_INIT_NODE_NAME,
                            ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
                        );
                        String currentNodeName = vlm.getAbsResource().getNode().getName().displayValue;
                        if (connectedInitNodeName == null)
                        {
                            rscProp.setProp(
                                InternalApiConsts.KEY_EBS_CONNECTED_INIT_NODE_NAME,
                                currentNodeName,
                                ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
                            );
                        }
                        else if (!currentNodeName.equals(connectedInitNodeName))
                        {
                            throw new StorageException(
                                "Cannot connect to target resource '" + targetRsc +
                                    "' since it has already a connected EBS initiator"
                            );
                        }
                    }
                    catch (InvalidKeyException | InvalidValueException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    break;
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
                default:
                    throw new ImplementationError("Unexpected kind: " + kind);
            }
            storPool.putVolume(apiCtx, vlmData);
        }
        return vlmData;
    }

    public static @Nullable String getEbsVlmId(
        AccessContext accCtxRef,
        StorageRscData<Resource> rscDataRef,
        Volume vlmRef
    )
        throws AccessDeniedException
    {
        return getEbsVlmId(accCtxRef, rscDataRef.getResourceNameSuffix(), vlmRef);
    }

    private static @Nullable String getEbsVlmId(
        AccessContext accCtxRef,
        String rscNameSuffix,
        Volume vlmRef
    )
        throws AccessDeniedException
    {
        return vlmRef.getProps(accCtxRef).getProp(
            InternalApiConsts.KEY_EBS_VLM_ID + rscNameSuffix,
            ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
        );
    }

    /**
     * If a target resource has an initiator, the resource itself should have a property containing the node name of the
     * initiator. This method searches for a resource in the given resource definition, that is an EBS target resource
     * in the given availabilityZone and has either the "connectedInitiator" property not set or set to the optional
     * nodeName parameter.
     *
     * @param accCtx
     * @param remoteMap
     * @param rscDfn
     * @param availabilityZone
     * @param nodeName
     *
     * @return A target resource if one exists
     *
     * @throws AccessDeniedException
     */
    public static @Nullable Resource findTargetEbsResource(
        AccessContext accCtx,
        RemoteMap remoteMap,
        ResourceDefinition rscDfn,
        String availabilityZone,
        @Nullable String nodeName
    )
        throws AccessDeniedException
    {
        Resource ret = null;
        Iterator<Resource> rscIt = rscDfn.iterateResource(accCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();

            Node targetNode = rsc.getNode();
            if (targetNode.getNodeType(accCtx).equals(Node.Type.EBS_TARGET))
            {
                if (targetNode.getStorPoolCount() != 1)
                {
                    throw new ImplementationError(
                        "EBS target node has unexpectedly many storage pools: " + targetNode.getStorPoolCount()
                    );
                }
                StorPool ebsStorPool = targetNode.iterateStorPools(accCtx).next();
                String ebsStorPoolAZ = getAvailabilityZone(accCtx, remoteMap, ebsStorPool);

                if (ebsStorPoolAZ.equals(availabilityZone))
                {
                    ReadOnlyProps rscProp = rsc.getProps(accCtx);
                    String connectedInitiator = rscProp.getProp(
                        InternalApiConsts.KEY_EBS_CONNECTED_INIT_NODE_NAME,
                        ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
                    );
                    if (connectedInitiator == null || connectedInitiator.equals(nodeName))
                    {
                        ret = rsc;
                        break;
                    }
                }

            }
        }
        return ret;
    }

    public static String getAvailabilityZone(AccessContext accCtx, RemoteMap remoteMap, StorPool ebsStorPool)
        throws AccessDeniedException, ImplementationError
    {
        AbsRemote remote;
        try
        {
            remote = remoteMap.get(
                new RemoteName(
                    ebsStorPool.getProps(accCtx).getProp(
                        ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.NAMESPC_EBS + "/" +
                            ApiConsts.KEY_REMOTE
                    ),
                    true
                )
            );
        }
        catch (InvalidKeyException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        if (!(remote instanceof EbsRemote))
        {
            throw new ImplementationError(
                "Remote was unexpectedly not an EBS remote, but: " + (remote == null ?
                    "null" :
                    remote.getClass().getSimpleName())
            );
        }
        return ((EbsRemote) remote).getAvailabilityZone(accCtx);
    }

    private static PairNonNull<String, Resource> findUnusedTargetEbsPair(
        AccessContext accCtxRef,
        RemoteMap remoteMap,
        StorageRscData<Resource> rscDataRef,
        Volume vlmRef,
        StorPool storPool
    )
        throws AccessDeniedException, DatabaseException
    {
        String storedEbsVlmId = getEbsVlmId(accCtxRef, rscDataRef, vlmRef);
        PairNonNull<String, Resource> ret;

        if (storedEbsVlmId != null)
        {
            ret = new PairNonNull<>(storedEbsVlmId, vlmRef.getAbsResource());
        }
        else
        {
            Resource targetRsc = findTargetEbsResource(
                accCtxRef,
                remoteMap,
                vlmRef.getResourceDefinition(),
                getAvailabilityZone(accCtxRef, remoteMap, storPool),
                vlmRef.getAbsResource().getNode().getName().displayValue
            );

            if (targetRsc == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_MISSING_EBS_TARGET,
                        "No unused EBS target resource available"
                    )
                );
            }

            Volume targetVlm = targetRsc.getVolume(vlmRef.getVolumeNumber());
            String targetEbsVlmId = targetVlm.getProps(accCtxRef).getProp(
                InternalApiConsts.KEY_EBS_VLM_ID + rscDataRef.getResourceNameSuffix(),
                ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
            );

            if (targetEbsVlmId == null)
            {
                throw new ImplementationError("Target volume '" + targetVlm + "' does not have an EBSVlmId");
            }

            ret = new PairNonNull<>(targetEbsVlmId, targetRsc);
        }
        return ret;
    }

    @Override
    protected void mergeVlmData(
        VlmProviderObject<Resource> vlmDataRef,
        Volume vlmRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws InvalidKeyException, InvalidNameException, DatabaseException, ValueOutOfRangeException,
            ExhaustedPoolException, ValueInUseException, LinStorException
    {
        // if storage pool changed (i.e. because of a toggle disk) we need to update that

        StorPool currentStorPool = vlmDataRef.getStorPool();

        StorageRscData<Resource> storageRscData = (StorageRscData<Resource>) vlmDataRef
            .getRscLayerObject();
        StorPool newStorPool = layerDataHelperProvider.get().getStorPool(
            vlmRef,
            storageRscData,
            payloadRef
        );

        if (newStorPool != null && !newStorPool.equals(currentStorPool))
        {
            VlmProviderObject<Resource> vlmData = vlmDataRef;
            if (!currentStorPool.getDeviceProviderKind().equals(newStorPool.getDeviceProviderKind()))
            {
                VolumeNumber vlmNr = vlmData.getVlmNr();
                // Remove the old data, which also ensures that createVlmLayerData doesn't just return the existing
                // object.
                storageRscData.remove(apiCtx, vlmNr);
                // if the kind changes, we basically need a new vlmData
                vlmData = createVlmLayerData(
                    storageRscData,
                    vlmRef,
                    payloadRef,
                    layerListRef
                );
                storageRscData.getVlmLayerObjects().put(vlmNr, vlmData);
            }
            else
            {
                vlmDataRef.setStorPool(apiCtx, newStorPool);
            }
        }
    }

    @Override
    protected void resetStoragePools(AbsRscLayerObject<Resource> rscDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // changing storage pools allows other DeviceProviders than before. Therefore we simply delete
        // all storage volumes as they will be re-created soon

        HashSet<VolumeNumber> vlmNrs = new HashSet<>(rscDataRef.getVlmLayerObjects().keySet());
        for (VolumeNumber vlmNr : vlmNrs)
        {
            rscDataRef.remove(apiCtx, vlmNr);
        }
    }

    private boolean ignoreLockedSEDResources(StorageRscData<Resource> rscData)
        throws AccessDeniedException, DatabaseException
    {
        boolean ret = false;
        for (VlmProviderObject<Resource> vlmData : rscData.getVlmLayerObjects().values())
        {
            ReadOnlyProps storPoolProps = vlmData.getStorPool().getProps(apiCtx);
            if (storPoolProps.getNamespace(ApiConsts.NAMESPC_SED) != null && !secObjs.areAllSet())
            {
                CtrlRscLayerDataFactory ctrlRscLayerDataFactory = layerDataHelperProvider.get();
                ctrlRscLayerDataFactory.getLayerHelperByKind(DeviceLayerKind.STORAGE)
                    .addIgnoreReason(rscData, LayerIgnoreReason.SED_MISSING_KEY, true, false, false);
                ret = true;
                break;
            }
        }
        return ret;
    }

    @Override
    protected boolean recalculateVolatilePropertiesImpl(
        StorageRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, DatabaseException
    {
        Resource rsc = rscDataRef.getAbsResource();

        Collection<VlmProviderObject<Resource>> vlmLayerObjects = rscDataRef.getVlmLayerObjects().values();

        // ignore SED drives if master-passphrase not entered
        boolean changed = ignoreLockedSEDResources(rscDataRef);

        // first run some checks depending on the storageProvider.
        if (!changed)
        {
            Set<DeviceProviderKind> providerKindSet = new HashSet<>();
            for (VlmProviderObject<Resource> vlmData : vlmLayerObjects)
            {
                providerKindSet.add(vlmData.getProviderKind());
            }

            @Nullable LayerIgnoreReason reason = null;
            if (providerKindSet.contains(DeviceProviderKind.EBS_TARGET))
            {
                reason = LayerIgnoreReason.EBS_TARGET;
            }
            else if (providerKindSet.contains(DeviceProviderKind.SPDK) ||
                providerKindSet.contains(DeviceProviderKind.REMOTE_SPDK))
            {
                reason = LayerIgnoreReason.SPDK_TARGET;
            }

            if (reason != null) // IGNORE_REASON_NONE == null
            {
                changed |= addIgnoreReason(rscDataRef, reason, true, false, true);
            }
        }

        StateFlags<Flags> rscFlags = rsc.getStateFlags();
        if (rscFlags.isSet(apiCtx, Resource.Flags.INACTIVE) && rscFlags.isUnset(apiCtx, Resource.Flags.INACTIVATING))
        {
            // do not propagate the reason while we are still inactivating the resource.
            changed |= addIgnoreReason(rscDataRef, LayerIgnoreReason.RSC_INACTIVE, true, false, true);
        }
        if (rsc.streamVolumes().anyMatch(
            vlm -> isAnyVolumeFlagSetPrivileged(vlm, Volume.Flags.CLONING, Volume.Flags.CLONING_START) &&
                !areAllVolumeFlagsSetPrivileged(vlm, Volume.Flags.CLONING_FINISHED)
        ))
        {
            changed |= addIgnoreReason(
                rscDataRef,
                LayerIgnoreReason.RSC_CLONING,
                true,
                false, // does not matter, it should not be possible that anything exists below storage layer
                rscData ->
                /*
                 * set ignore reason for all non-STORAGE rscData as well as for STORAGE data that have a rscSuffix
                 * that should not be cloned
                 */
                !rscData.getLayerKind().equals(DeviceLayerKind.STORAGE) ||
                    !RscLayerSuffixes.shouldSuffixBeCloned(rscData.getResourceNameSuffix())
            );
        }

        if (!secObjs.areAllSet())
        {
            for (VlmProviderObject<Resource> vlmData : vlmLayerObjects)
            {
                DeviceProviderKind devProviderKind = vlmData.getStorPool().getDeviceProviderKind();
                if (devProviderKind.equals(DeviceProviderKind.EBS_INIT) ||
                    devProviderKind.equals(DeviceProviderKind.EBS_TARGET))
                {
                    changed |= addIgnoreReason(rscDataRef, LayerIgnoreReason.EBS_MISSING_KEY, false, false, false);
                    break;
                }
            }
        }
        return changed;
    }

    private boolean areAllVolumeFlagsSetPrivileged(Volume vlm, Volume.Flags... flags)
    {
        boolean isSet = false;
        try
        {
            isSet = vlm.getFlags().isSet(apiCtx, flags);
        }
        catch (AccessDeniedException ignored)
        {
        }
        return isSet;
    }

    private boolean isAnyVolumeFlagSetPrivileged(Volume vlm, Volume.Flags... flags)
    {
        boolean isSet = false;
        try
        {
            isSet = vlm.getFlags().isSomeSet(apiCtx, flags);
        }
        catch (AccessDeniedException ignored)
        {
        }
        return isSet;
    }

    @Override
    protected boolean isExpectedToProvideDevice(StorageRscData<Resource> storageRscData) throws AccessDeniedException
    {
        return !storageRscData.hasAnyPreventExecutionIgnoreReason();
    }

    @Override
    protected <RSC extends AbsResource<RSC>> @Nullable RscDfnLayerObject restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<RSC> fromSnapDataRef
    )
    {
        // StorageLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> @Nullable VlmDfnLayerObject restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<RSC> fromSnapVlmDataRef
    )
    {
        // StorageLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> StorageRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createStorageRscData(
            layerRscIdPool.autoAllocate(),
            rscParentRef,
            rscRef,
            fromAbsRscDataRef.getResourceNameSuffix()
        );
    }

    @Override
    protected <RSC extends AbsResource<RSC>> VlmProviderObject<Resource> restoreVlmData(
        Volume vlmRef,
        StorageRscData<Resource> storRscData,
        VlmProviderObject<RSC> snapVlmData,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws DatabaseException, AccessDeniedException, LinStorException, InvalidNameException
    {
        VlmProviderObject<Resource> vlmData;

        DeviceProviderKind providerKind = snapVlmData.getProviderKind();
        StorPool storPool = AbsLayerHelperUtils.getStorPool(
            apiCtx,
            vlmRef,
            storRscData,
            snapVlmData.getStorPool(),
            storpoolRenameMap,
            apiCallRc
        );
        switch (providerKind)
        {
            case DISKLESS:
                vlmData = layerDataFactory.createDisklessData(
                    vlmRef,
                    snapVlmData.getUsableSize(),
                    storRscData,
                    storPool
                );
                break;
            case FILE:
            case FILE_THIN:
                vlmData = layerDataFactory.createFileData(vlmRef, storRscData, providerKind, storPool);
                break;
            case LVM:
                vlmData = layerDataFactory.createLvmData(vlmRef, storRscData, storPool);
                break;
            case LVM_THIN:
                vlmData = layerDataFactory.createLvmThinData(vlmRef, storRscData, storPool);
                break;
            case STORAGE_SPACES:    // fall-through
            case STORAGE_SPACES_THIN:
                vlmData = layerDataFactory.createStorageSpacesData(vlmRef, storRscData, providerKind, storPool);
                break;
            case ZFS:
            case ZFS_THIN:
                vlmData = layerDataFactory.createZfsData(vlmRef, storRscData, providerKind, storPool);
                break;
            case SPDK:
            case REMOTE_SPDK:
                vlmData = layerDataFactory.createSpdkData(vlmRef, storRscData, providerKind, storPool);
                break;
            case EXOS:
                exosNameShortener.shorten(
                    vlmRef.getVolumeDefinition(),
                    storPool.getSharedStorPoolName().displayValue,
                    storRscData.getResourceNameSuffix(),
                    false
                );
                ExosData<Resource> exosData = layerDataFactory.createExosData(vlmRef, storRscData, storPool);
                exosData.updateShortName(apiCtx);
                vlmData = exosData;
                break;
            case EBS_INIT:
            case EBS_TARGET:
                vlmData = layerDataFactory.createEbsData(vlmRef, storRscData, storPool);
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unexpected kind: " + kind);
        }
        storPool.putVolume(apiCtx, vlmData);
        return vlmData;
    }
}

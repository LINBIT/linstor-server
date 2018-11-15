package com.linbit.linstor.core.devmgr.helper;

import com.linbit.utils.AccessUtils;
import com.linbit.utils.RemoveAfterDevMgrRework;

import static com.linbit.utils.AccessUtils.execPrivileged;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.ResourceType;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.LayerDataFactory;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.VolumeUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@RemoveAfterDevMgrRework
@Singleton
public class LayeredResourcesHelper
{
    private final AccessContext sysCtx;
    private final ResourceDataFactory rscFactory;
    private final VolumeDataFactory vlmFactory;
    private final LayerDataFactory layerDataFactory;
    private final ControllerPeerConnector controllerPeerConnector;
    private final ResourceDefinitionMap rscDfnMap;
    private final ErrorReporter errorReporter;

    @Inject
    public LayeredResourcesHelper(
        @SystemContext AccessContext sysCtxRef,
        ResourceDataFactory rscFactoryRef,
        VolumeDataFactory vlmFactoryRef,
        LayerDataFactory layerDataFactoryRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        ErrorReporter errorReporterRef
    )
    {
        sysCtx = sysCtxRef;
        rscFactory = rscFactoryRef;
        vlmFactory = vlmFactoryRef;
        layerDataFactory = layerDataFactoryRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        rscDfnMap = rscDfnMapRef;
        errorReporter = errorReporterRef;
    }


    @RemoveAfterDevMgrRework
    public List<Resource> getResourcesToDelete(
        Collection<Resource> origResources,
        Map<Resource, StorageException> exceptions
    )
    {
        List<Resource> resourcesToDelete = new ArrayList<>();
        /*
         *  At this point, all layers are expected to delete their resource and notify the controller.
         *  The only resources that are not deleted in this process are the DEFAULT resource.
         *  That means we basically only have to perform a check if there are DEFAULT resources marked
         *  as DELETE that have (typed) children. If so, that is an impl-error as one layer did not clean up properly.
         *  Otherwise, remove the resource.
         */
        for (Resource rsc : origResources)
        {
            try
            {
                if (rsc.getStateFlags().isSet(sysCtx, RscFlags.DELETE))
                {
                    if (
                        rsc.getType().equals(ResourceType.DEFAULT) &&
                        !rsc.getChildResources(sysCtx).isEmpty() &&
                        !exceptions.containsKey(rsc)
                    )
                    {
                        throw new ImplementationError("resource not properly cleaned up: " + rsc.getKey());
                    }
                    resourcesToDelete.add(rsc);
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError(accDeniedExc);
            }
        }
        //  (default) remote resources are already deleted by the API layer.
        //  So we have to find orphaned resources and also delete those

        resourcesToDelete.addAll(
            rscDfnMap.values().stream()
                .flatMap(rscDfn -> AccessUtils.execPrivileged(() -> rscDfn.streamResource(sysCtx)))
                .filter(rsc -> isOrphaned(rsc, exceptions))
                .collect(Collectors.toList())
        );
        return resourcesToDelete;
    }

    public Set<Volume> getVolumesToDelete(
        Collection<Resource> origResources,
        Map<Resource, StorageException> exceptions
    )
    {
        Set<Volume> volumesToDelete = new HashSet<>();
        /*
         *  At this point, all layers are expected to delete their volumes and notify the controller.
         *  The only volumes that are not deleted in this process are the DEFAULT resource.
         *  That means we basically only have to perform a check if there are DEFAULT volumes marked
         *  as DELETE that have (typed) children. If so, that is an impl-error as one layer did not clean up properly.
         *  Otherwise, remove the volume.
         */
        for (Resource rsc : origResources)
        {
            if (!exceptions.containsKey(rsc) && rsc.getType().equals(ResourceType.DEFAULT))
            {
                try
                {
                    for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                    {
                        if (vlm.getFlags().isSet(sysCtx, VlmFlags.DELETE))
                        {
                            Volume backingVolume = VolumeUtils.getBackingVolume(sysCtx, vlm);
                            if (backingVolume != null && !backingVolume.isDeleted())
                            {
                                throw new ImplementationError("resource not properly cleaned up: " + rsc.getKey());
                            }
                            volumesToDelete.add(vlm);
                        }
                    }
                }
                catch (StorageException storExc)
                {
                    exceptions.put(rsc, storExc);
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    throw new ImplementationError(accDeniedExc);
                }
            }
        }

        //  (default) remote volumes are already deleted by the API layer.
        //  So we have to find orphaned volumes and also delete those
        volumesToDelete.addAll(
            rscDfnMap.values().stream()
                .flatMap(rscDfn -> AccessUtils.execPrivileged(() -> rscDfn.streamResource(sysCtx)))
                .flatMap(Resource::streamVolumes)
                .filter(
                    vlm ->
                    {
                        boolean ret;
                        try
                        {
                            ret =
                                vlm.getFlags().isSet(sysCtx, VlmFlags.DELETE) &&
                                VolumeUtils.getBackingVolume(sysCtx, vlm) == null;
                        }
                        catch (AccessDeniedException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                        catch (StorageException exc)
                        {
                            exceptions.put(vlm.getResource(), exc);
                            ret = false;
                        }
                        return ret;
                    }
                )
                .collect(Collectors.toList())
        );

        return volumesToDelete;
    }

    @RemoveAfterDevMgrRework
    public List<Resource> extractLayers(Collection<Resource> origResources)
    {
        List<Resource> resourcesToConvert = new ArrayList<>();
        {
            List<Resource> orphanedResources = new ArrayList<>();
            origResources.stream().map(Resource::getDefinition)
                .flatMap(rscDfn -> AccessUtils.execPrivileged(() -> rscDfn.streamResource(sysCtx)))
                .forEach(rsc ->
                    {
                        if (rsc.getType().equals(ResourceType.DEFAULT))
                        {
                            if (!rsc.isDeleted())
                            {
                                resourcesToConvert.add(rsc);
                            }
                        }
                        else
                        if (isOrphaned(rsc, Collections.emptyMap()))
                        {
                            orphanedResources.add(rsc);
                        }
                    }
            );
            orphanedResources.forEach(rsc ->
                {
                    try
                    {
                        rsc.delete(sysCtx);
                    }
                    catch (AccessDeniedException | SQLException exc)
                    {
                        throw new ImplementationError(exc);
                    }

                }
            );
        }



        List<Resource> layeredResources = new ArrayList<>();
        try
        {
            /*
             * although we iterate ALL resources (including peer resources), we only add our local resources to the
             * layeredResources list as the device manager should only process those.
             */
            for (Resource origRsc : resourcesToConvert)
            {
                /* currently a resource can have 3 layers at maximum
                *
                *  DRBD (not on swordfish)
                *  Crypt (not on swordfish)
                *  {,Thin}{Lvm,Zfs}, Swordfish{Target,Initiator}
                *
                *  The upper two layers stay the same (and might get extended in the future)
                *
                *  The lowest layer will be grouped into a "StorgeLayer" which
                *  basically only switches the input for the layers from resource-based
                *  to volume-based. As this Layer groups the current {,Thin}{Lvm,Zfs} and
                *  Swordfish{Target,Initiator} drivers, it will also decide which volume should be created
                *  using which driver.
                *  This is needed as otherwise we could not compose a resource with volumes using different
                *  backing-drivers.
                */

                /*
                 * The stacked resource will keep the origRsc as the lowest resource and will create
                 * parent-resources as needed.
                 */

                errorReporter.logTrace("Creating typed resources for %s", origRsc.toString());

                ResourceDefinition rscDfn = origRsc.getDefinition();
                Resource currentRsc = null;

                // for the lowest resource, we only need volume-based data
                // additionally, we do not need extra layer-specific data.

                // however, we do have to set the ResourceType, thus we need to create a new resource
                if (!origRsc.getStateFlags().isSet(sysCtx, RscFlags.DISKLESS))
                {
                    currentRsc = createStorageRsc(layeredResources, origRsc);
                }

                if (needsCrypt(origRsc))
                {
                    currentRsc = nextRsc(layeredResources, origRsc, currentRsc, ResourceType.CRYPT);
                    currentRsc.setLayerData(
                        sysCtx,
                        layerDataFactory.createCryptSetupData(
                            getDrbdResourceName(rscDfn),
                            currentRsc,
                            getCryptPw(origRsc)
                        )
                    );
                }
                if (needsDrbd(origRsc))
                {
                    currentRsc = nextRsc(layeredResources, origRsc, currentRsc, ResourceType.DRBD);
                    // TODO: update volume definition sizes of child resources (meta-data)
                    currentRsc.setLayerData(
                        sysCtx,
                        layerDataFactory.createDrbdRscData(
                            currentRsc,
                            getDrbdResourceName(rscDfn),
                            currentRsc.getNodeId(),
                            origRsc.isDiskless(sysCtx),
                            origRsc.disklessForPeers(sysCtx)
                        )
                    );
                    rscDfn.setLayerData(
                        sysCtx,
                        layerDataFactory.createDrbdRscDfnData(
                            rscDfn,
                            getDrbdResourceName(rscDfn),
                            rscDfn.getPort(sysCtx),
                            rscDfn.getTransportType(sysCtx),
                            rscDfn.getSecret(sysCtx)
                        )
                    );

                    String peerSlotsProp = origRsc.getProps(sysCtx).getProp(ApiConsts.KEY_PEER_SLOTS);
                    // Property is checked when the API sets it; if it still throws for whatever reason, it is logged
                    // as an unexpected exception in dispatchResource()
                    short peerSlots = peerSlotsProp == null ?
                        InternalApiConsts.DEFAULT_PEER_SLOTS : Short.parseShort(peerSlotsProp);

                    rscDfn.streamVolumeDfn(sysCtx).forEach(
                        vlmDfn ->
                            AccessUtils.execPrivileged(
                                () ->
                                    vlmDfn.setLayerData(
                                        sysCtx,
                                        layerDataFactory.createDrbdVlmDfnData(
                                            vlmDfn,
                                            execPrivileged(() -> vlmDfn.getMinorNr(sysCtx)),
                                            peerSlots
                                        )
                                    )
                            )
                    );
                    currentRsc.streamVolumes().forEach(this::initializeDrbdVlmData);
                }
            }
            /*
             * remote resource get deleted in the API layer. As we are splitting default resources into the actual
             * typed resources here (within the devicemanager), we need to check if the API layer has already removed
             * a default resource. If so, we also have to delete all of it's (typed) children
             */
//            List<Resource> typedResourcesToDelete = rscDfnMap.values().stream()
//                .flatMap(rscDfn -> AccessUtils.execPrivileged(() -> rscDfn.streamResource(sysCtx)))
//                .filter(
//                    rsc ->
//                        !rsc.getType().equals(ResourceType.DEFAULT) &&
//                        AccessUtils.execPrivileged(() -> rsc.getParentResource(sysCtx).isDeleted())
//                    )
//                .collect(Collectors.toList());
//            typedResourcesToDelete.forEach(
//                    rsc ->
//                    {
//                        try
//                        {
//                            rsc.delete(sysCtx);
//                        }
//                        catch (AccessDeniedException | SQLException exc)
//                        {
//                            throw new ImplementationError(exc);
//                        }
//                    }
//            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError("Privileged context has not enough privileges", accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ImplementationError(sqlExc);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return layeredResources;
    }

    private Resource createStorageRsc(List<Resource> layeredResources, Resource origRsc)
        throws AccessDeniedException, SQLException
    {
        Resource typedRsc;
        ResourceDefinition rscDfn = origRsc.getDefinition();
        UUID rscUuid = UUID.randomUUID();
        RscFlags[] origFlags = FlagsHelper.toFlagsArray(RscFlags.class, origRsc.getStateFlags(), sysCtx);
        typedRsc = rscFactory.getTypedInstanceSatellite(
            sysCtx,
            rscUuid,
            origRsc.getAssignedNode(),
            rscDfn,
            origRsc.getNodeId(),
            origFlags,
            ResourceType.STORAGE,
            null
        );

        typedRsc.getStateFlags().disableAllFlags(sysCtx);
        typedRsc.getStateFlags().enableFlags(sysCtx, origFlags);
        Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(sysCtx);
        while (vlmDfnIt.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIt.next();
            Volume origVlm = origRsc.getVolume(vlmDfn.getVolumeNumber());
            VlmFlags[] origVlmFlags =
                FlagsHelper.toFlagsArray(Volume.VlmFlags.class, origVlm.getFlags(), sysCtx);
            Volume typedVlm;
            if (origVlm.getFlags().isSet(sysCtx, VlmFlags.DELETE))
            {
                typedVlm = typedRsc.getVolume(vlmDfn.getVolumeNumber());
            }
            else
            {
                typedVlm = vlmFactory.getInstanceSatellite(
                    sysCtx,
                    UUID.randomUUID(),
                    typedRsc,
                    vlmDfn,
                    origVlm.getStorPool(sysCtx),
                    null,
                    null,
                    origVlmFlags
                );
            }

            if (typedVlm != null)
            {
                typedVlm.getFlags().disableAllFlags(sysCtx);
                typedVlm.getFlags().enableFlags(sysCtx, origVlmFlags);
            }
        }
        errorReporter.logTrace(
            "%s STORAGE resource: %s",
            typedRsc.getUuid().equals(rscUuid) ? "Created new " : "Loaded",
            typedRsc
        );
        typedRsc.setParentResource(sysCtx, origRsc, true); // just to keep the reference
        add(layeredResources, typedRsc);
        return typedRsc;
    }


    private Resource nextRsc(
        List<Resource> layeredResources,
        Resource origRsc,
        Resource typedChild,
        ResourceType type
    )
        throws AccessDeniedException, SQLException
    {
        Resource typedResource;
        UUID uuid = UUID.randomUUID();

        ResourceDefinition origRscDfn = origRsc.getDefinition();
        RscFlags[] origFlags = FlagsHelper.toFlagsArray(RscFlags.class, origRsc.getStateFlags(), sysCtx);
        typedResource = rscFactory.getTypedInstanceSatellite(
            sysCtx,
            uuid,
            origRsc.getAssignedNode(),
            origRscDfn,
            origRsc.getNodeId(),
            origFlags,
            type,
            null
        );

        typedResource.getStateFlags().disableAllFlags(sysCtx);
        typedResource.getStateFlags().enableFlags(sysCtx, origFlags);

        if (origRsc.isCreatePrimary())
        {
            ((ResourceData) typedResource).setCreatePrimary();
        }
        // TODO: copy resource connections - maybe drbd level?
        try
        {
            Iterator<VolumeDefinition> vlmDfnIt = origRscDfn.iterateVolumeDfn(sysCtx);
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                Volume origVlm = origRsc.getVolume(vlmDfn.getVolumeNumber());
                Volume typedVlm;
                if (origVlm.getFlags().isSet(sysCtx, VlmFlags.DELETE))
                {
                    typedVlm = typedResource.getVolume(vlmDfn.getVolumeNumber());
                }
                else
                {
                    typedVlm = vlmFactory.getInstanceSatellite(
                        sysCtx,
                        UUID.randomUUID(),
                        typedResource,
                        vlmDfn,
                        origVlm.getStorPool(sysCtx),
                        null,
                        null,
                        FlagsHelper.toFlagsArray(Volume.VlmFlags.class, origVlm.getFlags(), sysCtx)
                    );
                }
                if (typedVlm != null)
                {
                    typedVlm.setAllocatedSize(sysCtx, origVlm.getAllocatedSize(sysCtx));
                    typedVlm.setBackingDiskPath(sysCtx, origVlm.getBackingDiskPath(sysCtx));
                    typedVlm.setMetaDiskPath(sysCtx, origVlm.getMetaDiskPath(sysCtx));
                    if (origVlm.isUsableSizeSet(sysCtx))
                    {
                        typedVlm.setUsableSize(sysCtx, origVlm.getUsableSize(sysCtx));
                    }
                    // TODO: copy volume connection - maybe drbd level?
                    // TODO: copy device path? drbd?
                }
            }
            errorReporter.logTrace(
                "%s %s resource: %s",
                typedResource.getUuid().equals(uuid) ? "Created new " : "Loaded",
                type,
                typedResource
            );
            if (typedChild != null) // could be == null in case of DISKLESS drbd
            {
                typedChild.setParentResource(sysCtx, typedResource, true);
            }
            typedResource.setParentResource(sysCtx, origRsc, true); // just to keep the reference
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(exc);
        }

        add(layeredResources, typedResource);

        return typedResource;
    }

    private void add(List<Resource> returnedResources, Resource rsc)
    {
        if (rsc.getAssignedNode().getName().equals(controllerPeerConnector.getLocalNodeName()))
        {
            returnedResources.add(rsc);
        }
    }

    private String getDrbdResourceName(ResourceDefinition rscDfn)
    {
        return rscDfn.getName().displayValue; // TODO: find a better naming for this
    }

    private boolean needsDrbd(Resource rsc)
    {
        boolean ret;
        // for now: if one volume needs drbd, whole resource gets drbd
        ret = rsc.streamVolumes()
            .anyMatch(
                vlm ->
                    execPrivileged(() -> vlm.getStorPool(sysCtx))
                        .getDriverKind()
                        .supportsDrbd()
        );

        return ret;
    }

    private boolean needsCrypt(Resource rsc)
    {
        boolean ret;
        // known limitation: if one volume definition wants to crypt, all volume definitions get crypt
        ret = rsc.streamVolumes()
            .anyMatch(
                vlm -> execPrivileged(() -> vlm.getVolumeDefinition().getFlags().isSet(sysCtx, VlmDfnFlags.ENCRYPTED))
        );
        return ret;
    }

    private char[] getCryptPw(Resource rsc) throws AccessDeniedException
    {
        // we use the same password for all volumes of this resource
        VolumeDefinition firstVlmDfn = rsc.getDefinition().streamVolumeDfn(sysCtx).findFirst().orElse(null);
        char[] passwd = null;
        if (firstVlmDfn != null)
        {
            passwd = firstVlmDfn.getCryptKey(sysCtx).toCharArray();
        }

        return passwd;
    }

    private void initializeDrbdVlmData(Volume vlm)
    {
        try
        {
            vlm.setLayerData(
                sysCtx,
                layerDataFactory.createDrbdVlmData()
            );
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void printCurrentResourcesWithVolumes()
    {
        printCurrentResourcesWithVolumes(rscDfnMap, sysCtx);
    }

    public static void printCurrentResourcesWithVolumes(
        ResourceDefinitionMap rscDfnMap,
        AccessContext sysCtx
    )
    {
        System.out.println("current resources with volumes:");
        try
        {
            for (Entry<ResourceName, ResourceDefinition> entry : rscDfnMap.entrySet())
            {
                System.out.println("RscDfn: " + entry.getKey());
                for (Resource rsc : entry.getValue().streamResource(sysCtx).collect(Collectors.toList()))
                {
                    if (!rsc.isDeleted())
                    {
                        System.out.println(
                            "   " + rsc.getKey() +
                            " " + FlagsHelper.toStringList(RscFlags.class, rsc.getStateFlags().getFlagsBits(sysCtx)) +
                            " " + (rsc.isDeleted() ? "DELETED" : "")
                        );
                        for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                        {
                            if (!vlm.isDeleted())
                            {
                                System.out.println(
                                    "      " + vlm.getKey() +
                                    " " + FlagsHelper.toStringList(VlmFlags.class, vlm.getFlags().getFlagsBits(sysCtx))
                                );
                                if (vlm.getVolumeDefinition().isDeleted())
                                {
                                    System.out.println("ERROR: vlmDfn DELETED");
                                }
                            }
                            else
                            {
                                System.out.println("ERROR: " + vlm.getKey() + " DELETED");
                            }
                        }
                    }
                    else
                    {
                        System.out.println("ERROR: " + rsc.getKey() + " DELETED");
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private boolean isOrphaned(Resource rsc, Map<Resource, StorageException> exceptions)
    {
        boolean orphaned = false;
        try
        {
            Resource parent = rsc.getParentResource(sysCtx);
            while (parent != null)
            {
                if (exceptions.containsKey(parent))
                {
                    // do not delete "rsc", as at least one of its parents had an exception.
                    break;
                }
                if (parent.isDeleted())
                {
                    orphaned = true;
                    break;
                }
                parent = parent.getParentResource(sysCtx);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return orphaned;
    }
}

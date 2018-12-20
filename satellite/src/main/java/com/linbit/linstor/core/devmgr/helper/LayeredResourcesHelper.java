package com.linbit.linstor.core.devmgr.helper;

import com.linbit.utils.AccessUtils;
import com.linbit.utils.RemoveAfterDevMgrRework;

import static com.linbit.utils.AccessUtils.execPrivileged;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceType;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeConnection;
import com.linbit.linstor.VolumeConnectionDataFactory;
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceConnection.RscConnFlags;
import com.linbit.linstor.ResourceConnectionDataSatelliteFactory;
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
import com.linbit.linstor.storage.layer.provider.swordfish.SfVlmDfnDataStlt;
import com.linbit.linstor.storage.utils.SwordfishConsts;
import com.linbit.linstor.transaction.TransactionMgr;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RemoveAfterDevMgrRework
@Singleton
public class LayeredResourcesHelper
{
    private final AccessContext sysCtx;
    private final ResourceDataFactory rscFactory;
    private final VolumeDataFactory vlmFactory;
    private final ResourceConnectionDataSatelliteFactory rscConnFactory;
    private final VolumeConnectionDataFactory vlmConnFactory;
    private final LayerDataFactory layerDataFactory;
    private final ControllerPeerConnector controllerPeerConnector;
    private final ResourceDefinitionMap rscDfnMap;
    private final ErrorReporter errorReporter;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public LayeredResourcesHelper(
        @SystemContext AccessContext sysCtxRef,
        ResourceDataFactory rscFactoryRef,
        VolumeDataFactory vlmFactoryRef,
        ResourceConnectionDataSatelliteFactory rscConnFactoryRef,
        VolumeConnectionDataFactory vlmConnFactoryRef,
        LayerDataFactory layerDataFactoryRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        ErrorReporter errorReporterRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        sysCtx = sysCtxRef;
        rscFactory = rscFactoryRef;
        vlmFactory = vlmFactoryRef;
        rscConnFactory = rscConnFactoryRef;
        vlmConnFactory = vlmConnFactoryRef;
        layerDataFactory = layerDataFactoryRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        rscDfnMap = rscDfnMapRef;
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;
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

                Resource currentRsc = null;

                currentRsc = createStorageRsc(layeredResources, origRsc);

                if (needsCrypt(origRsc))
                {
                    currentRsc = nextRsc(layeredResources, origRsc, currentRsc, ResourceType.CRYPT);
                }
                if (needsDrbd(origRsc))
                {
                    currentRsc = nextRsc(layeredResources, origRsc, currentRsc, ResourceType.DRBD);
                }
            }
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

    /**
     * This method is called to synchronize the changes performed onto a typed resource to be tracked on the
     * default resource
     * @param origResources
     */
    public void cleanupResources(Collection<Resource> origResources)
    {
        try
        {
            for (Resource origRsc : origResources)
            {
                Resource drbdRsc = origRsc.getDefinition().getResource(
                    sysCtx,
                    origRsc.getAssignedNode().getName(),
                    ResourceType.DRBD
                );
                if (drbdRsc != null && !drbdRsc.isCreatePrimary() && origRsc.isCreatePrimary())
                {
                    ((ResourceData) origRsc).unsetCreatePrimary();
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Resource createStorageRsc(List<Resource> layeredResources, Resource origRsc)
        throws AccessDeniedException, SQLException, InvalidKeyException
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

        Map<String, String> typedRscPropsMap = typedRsc.getProps(sysCtx).map();
        typedRscPropsMap.clear();
        typedRscPropsMap.putAll(origRsc.getProps(sysCtx).map());

        Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(sysCtx);
        while (vlmDfnIt.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIt.next();
            Volume origVlm = origRsc.getVolume(vlmDfn.getVolumeNumber());
            VlmFlags[] origVlmFlags =
                FlagsHelper.toFlagsArray(Volume.VlmFlags.class, origVlm.getFlags(), sysCtx);
            Volume typedVlm = vlmFactory.getInstanceSatellite(
                sysCtx,
                UUID.randomUUID(),
                typedRsc,
                vlmDfn,
                origVlm.getStorPool(sysCtx),
                null,
                null,
                origVlmFlags
            );

            if (typedVlm != null)
            {
                typedVlm.getFlags().disableAllFlags(sysCtx);
                typedVlm.getFlags().enableFlags(sysCtx, origVlmFlags);

                Map<String, String> typedVlmPropsMap = typedVlm.getProps(sysCtx).map();
                typedVlmPropsMap.clear();
                typedVlmPropsMap.putAll(origVlm.getProps(sysCtx).map());

                typedVlm.setStorPool(sysCtx, origVlm.getStorPool(sysCtx));

                // maybe not needed. Will be removed anyways if the rework is completed
                SfVlmDfnDataStlt vlmDfnData = vlmDfn.getLayerData(sysCtx, SfVlmDfnDataStlt.class);
                if (vlmDfnData == null)
                {
                    String vlmOdata = vlmDfn.getProps(sysCtx)
                        .getProp(SwordfishConsts.ODATA, ApiConsts.NAMESPC_STORAGE_DRIVER);
                    if (vlmOdata != null)
                    {
                        vlmDfnData = new SfVlmDfnDataStlt(
                            transMgrProvider,
                            true,
                            new ArrayList<>(),
                            vlmDfn.getVolumeSize(sysCtx),
                            vlmOdata,
                            false // TODO: be careful with this
                        );
                    }
                    else
                    {
                        vlmDfnData = new SfVlmDfnDataStlt(transMgrProvider);
                    }
                    vlmDfn.setLayerData(sysCtx, vlmDfnData);
                }
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

        Map<String, String> typedRscPropsMap = typedResource.getProps(sysCtx).map();
        typedRscPropsMap.clear();
        typedRscPropsMap.putAll(origRsc.getProps(sysCtx).map());

        origRsc.streamResourceConnections(sysCtx).forEach(
            rscCon -> convertRscCon(origRsc, type, typedResource, rscCon)
        );

        if (origRsc.isCreatePrimary())
        {
            ((ResourceData) typedResource).setCreatePrimary();
        }
        try
        {
            Iterator<VolumeDefinition> vlmDfnIt = origRscDfn.iterateVolumeDfn(sysCtx);
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                Volume origVlm = origRsc.getVolume(vlmDfn.getVolumeNumber());
                Volume typedVlm = vlmFactory.getInstanceSatellite(
                    sysCtx,
                    UUID.randomUUID(),
                    typedResource,
                    vlmDfn,
                    origVlm.getStorPool(sysCtx),
                    null,
                    null,
                    FlagsHelper.toFlagsArray(Volume.VlmFlags.class, origVlm.getFlags(), sysCtx)
                );
                typedVlm.setAllocatedSize(sysCtx, origVlm.getAllocatedSize(sysCtx));
                typedVlm.setBackingDiskPath(sysCtx, origVlm.getBackingDiskPath(sysCtx));
                typedVlm.setMetaDiskPath(sysCtx, origVlm.getMetaDiskPath(sysCtx));
                if (origVlm.isUsableSizeSet(sysCtx))
                {
                    typedVlm.setUsableSize(sysCtx, origVlm.getUsableSize(sysCtx));
                }

                Map<String, String> typedVlmPropsMap = typedVlm.getProps(sysCtx).map();
                typedVlmPropsMap.clear();
                typedVlmPropsMap.putAll(origVlm.getProps(sysCtx).map());


                origVlm.streamVolumeConnections(sysCtx).forEach(
                    vlmCon -> convertVlmCon(typedVlm, origVlm, vlmCon)
                );

                // TODO: copy device path? drbd?
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

    private void convertRscCon(
        Resource origRsc,
        ResourceType type,
        Resource typedResource,
        ResourceConnection rscCon
    )
        throws ImplementationError
    {
        try
        {
            Resource other = rscCon.getSourceResource(sysCtx);
            if (origRsc.equals(other))
            {
                other = rscCon.getTargetResource(sysCtx);
            }

            RscFlags[] otherFlags = FlagsHelper.toFlagsArray(
                RscFlags.class,
                other.getStateFlags(),
                sysCtx
            );
            Resource otherTyped = rscFactory.getTypedInstanceSatellite(
                sysCtx,
                UUID.randomUUID(),
                other.getAssignedNode(),
                other.getDefinition(),
                other.getNodeId(),
                otherFlags,
                type,
                null
            );

            ResourceConnection typedCon = typedResource.getResourceConnection(sysCtx, otherTyped);
            RscConnFlags[] rscConFlags = FlagsHelper.toFlagsArray(
                RscConnFlags.class,
                rscCon.getStateFlags(),
                sysCtx
            );
            if (typedCon == null)
            {
                typedCon = rscConnFactory.getInstanceSatellite(
                    sysCtx,
                    UUID.randomUUID(),
                    typedResource,
                    otherTyped,
                    rscConFlags,
                    rscCon.getPort(sysCtx)
                );
            }
            else
            {
                typedCon.getStateFlags().resetFlagsTo(sysCtx, rscConFlags);
            }
            Map<String, String> typedPropsMap = typedCon.getProps(sysCtx).map();
            typedPropsMap.clear();
            typedPropsMap.putAll(rscCon.getProps(sysCtx).map());
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void convertVlmCon(Volume typedVlm, Volume origVlm, VolumeConnection vlmCon)
    {
        try
        {
            ResourceDefinition rscDfn = origVlm.getResourceDefinition();
            Resource origRsc = origVlm.getResource();

            Volume other = vlmCon.getSourceVolume(sysCtx);
            if (origVlm.equals(other))
            {
                other = vlmCon.getTargetVolume(sysCtx);
            }

            RscFlags[] origRscFlags = FlagsHelper.toFlagsArray(
                RscFlags.class,
                origRsc.getStateFlags(),
                sysCtx
            );
            Resource otherTypedRsc = rscFactory.getTypedInstanceSatellite(
                sysCtx,
                UUID.randomUUID(),
                origRsc.getAssignedNode(),
                rscDfn,
                origRsc.getNodeId(),
                origRscFlags,
                typedVlm.getResource().getType(),
                null
            );
            VlmFlags[] otherFlags = FlagsHelper.toFlagsArray(
                VlmFlags.class,
                other.getFlags(),
                sysCtx
            );
            Volume otherTyped = vlmFactory.getInstanceSatellite(
                sysCtx,
                UUID.randomUUID(),
                otherTypedRsc,
                origVlm.getVolumeDefinition(),
                origVlm.getStorPool(sysCtx),
                origVlm.getBackingDiskPath(sysCtx),
                origVlm.getMetaDiskPath(sysCtx),
                otherFlags
            );

            VolumeConnection typedCon = typedVlm.getVolumeConnection(sysCtx, otherTyped);
            if (typedCon == null)
            {
                typedCon = vlmConnFactory.getInstanceSatellite(
                    sysCtx,
                    UUID.randomUUID(),
                    typedVlm,
                    otherTyped
                );
            }
            Map<String, String> typedPropsMap = typedCon.getProps(sysCtx).map();
            typedPropsMap.clear();
            typedPropsMap.putAll(vlmCon.getProps(sysCtx).map());
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }
}

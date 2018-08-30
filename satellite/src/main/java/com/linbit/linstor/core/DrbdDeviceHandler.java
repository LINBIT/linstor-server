package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.drbd.DrbdAdm;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.ConfFileBuilder;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.api.pojo.VolumeStateDevManager;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.drbdstate.DrbdConnection;
import com.linbit.linstor.drbdstate.DrbdResource;
import com.linbit.linstor.drbdstate.DrbdStateStore;
import com.linbit.linstor.drbdstate.DrbdVolume;
import com.linbit.linstor.drbdstate.NoInitialStateException;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceDeploymentStateEvent;
import com.linbit.linstor.event.satellite.InProgressSnapshotEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.linbit.linstor.timer.CoreTimer;
import com.linbit.utils.StringUtils;
import org.slf4j.event.Level;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/* TODO
 *
 * createResource() -- handling of resources known to LINSTOR vs. rogue resources not known to LINSTOR
 *                     required restructuring
 * vlmState.skip -- needs a better definition
 * rscState should possibly contain all vlmStates
 * vlmState should probably know whether the volume is a LINSTOR volume or a volume only seen by DRBD
 */
@Singleton
class DrbdDeviceHandler implements DeviceHandler
{
    private final ErrorReporter errLog;
    private final AccessContext wrkCtx;
    private final Provider<DeviceManager> deviceManagerProvider;
    private final FileSystemWatch fileSystemWatch;
    private final CoreTimer timer;
    private final DrbdStateStore drbdState;
    private final DrbdAdm drbdUtils;
    private final CtrlStltSerializer interComSerializer;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final MetaDataApi drbdMd;
    private final WhitelistProps whitelistProps;
    private final ResourceDeploymentStateEvent resourceDeploymentStateEvent;
    private final InProgressSnapshotEvent inProgressSnapshotEvent;

    // Number of activity log stripes for DRBD meta data; this should be replaced with a property of the
    // resource definition, a property of the volume definition, or otherwise a system-wide default
    private static final int FIXME_STRIPES = 1;

    // Number of activity log stripes; this should be replaced with a property of the resource definition,
    // a property of the volume definition, or or otherwise a system-wide default
    private static final long FIXME_STRIPE_SIZE = 32;

    // DRBD configuration file suffix; this should be replaced by a meaningful constant
    private static final String DRBD_CONFIG_SUFFIX = ".res";
    private StltConfigAccessor stltCfgAccessor;

    @Inject
    DrbdDeviceHandler(
        ErrorReporter errLogRef,
        @DeviceManagerContext AccessContext wrkCtxRef,
        Provider<DeviceManager> deviceManagerProviderRef,
        FileSystemWatch fileSystemWatchRef,
        CoreTimer timerRef,
        DrbdStateStore drbdStateRef,
        DrbdAdm drbdUtilsRef,
        CtrlStltSerializer interComSerializerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        WhitelistProps whitelistPropsRef,
        ResourceDeploymentStateEvent resourceDeploymentStateEventRef,
        InProgressSnapshotEvent inProgressSnapshotEventRef,
        StltConfigAccessor stltCfgAccessorRef
    )
    {
        errLog = errLogRef;
        wrkCtx = wrkCtxRef;
        deviceManagerProvider = deviceManagerProviderRef;
        fileSystemWatch = fileSystemWatchRef;
        timer = timerRef;
        drbdState = drbdStateRef;
        drbdUtils = drbdUtilsRef;
        interComSerializer = interComSerializerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        whitelistProps = whitelistPropsRef;
        resourceDeploymentStateEvent = resourceDeploymentStateEventRef;
        inProgressSnapshotEvent = inProgressSnapshotEventRef;
        stltCfgAccessor = stltCfgAccessorRef;
        drbdMd = new MetaData();
    }

    private ResourceState initializeResourceState(
        final ResourceDefinition rscDfn,
        final Collection<VolumeNumber> vlmNrs
    )
    {
        ResourceState rscState = new ResourceState();

        Map<VolumeNumber, VolumeState> vlmStateMap = new TreeMap<>();
        for (VolumeNumber vlmNr : vlmNrs)
        {
            VolumeStateDevManager vlmState = new VolumeStateDevManager(vlmNr);
            vlmState.setStorVlmName(computeVlmName(rscDfn, vlmNr));
            vlmStateMap.put(vlmNr, vlmState);
        }
        rscState.setVolumes(vlmStateMap);

        return rscState;
    }

    private ResourceState fillResourceState(final Resource rsc, final ResourceState rscState)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        String peerSlotsProp = rsc.getProps(wrkCtx).getProp(ApiConsts.KEY_PEER_SLOTS);
        // Property is checked when the API sets it; if it still throws for whatever reason, it is logged as an
        // unexpected exception in dispatchResource()
        short peerSlots = peerSlotsProp == null ?
            InternalApiConsts.DEFAULT_PEER_SLOTS : Short.parseShort(peerSlotsProp);

        // FIXME: Temporary fix: If the NIC selection property on a storage pool is changed retrospectively,
        //        then rewriting the DRBD resource configuration file and 'drbdadm adjust' is required,
        //        but there is not yet a mechanism to notify the device handler to perform an adjust action.
        rscState.setRequiresAdjust(true);
        rscState.setRscName(rsc.getDefinition().getName().getDisplayName());
        {
            Iterator<Volume> vlmIter = rsc.iterateVolumes();
            while (vlmIter.hasNext())
            {
                Volume vlm = vlmIter.next();
                VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

                VolumeStateDevManager vlmState = (VolumeStateDevManager) rscState.getVolumeState(vlmNr);

                Props props = vlm.getProps(wrkCtx);
                String restoreFromResourceProp = props.getProp(ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE);
                String restoreFromSnapshotProp = props.getProp(ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT);
                if (restoreFromResourceProp != null && restoreFromSnapshotProp != null)
                {
                    String overrideVlmIdProp = props.getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID);
                    String restoreVlmName = overrideVlmIdProp != null ?
                        overrideVlmIdProp :
                        computeVlmName(
                            new ResourceName(restoreFromResourceProp),
                            vlmNr
                        );

                    // Parse into 'Name' objects in order to validate the property contents
                    SnapshotName restoreFromSnapshotName = new SnapshotName(restoreFromSnapshotProp);

                    vlmState.setRestoreVlmName(restoreVlmName);
                    vlmState.setRestoreSnapshotName(restoreFromSnapshotName.displayValue);
                }

                vlmState.setNetSize(vlmDfn.getVolumeSize(wrkCtx));
                vlmState.setMarkedForDelete(vlm.getFlags().isSet(wrkCtx, Volume.VlmFlags.DELETE) ||
                    vlmDfn.getFlags().isSet(wrkCtx, VolumeDefinition.VlmDfnFlags.DELETE));
                vlmState.setMinorNr(vlmDfn.getMinorNr(wrkCtx));
                vlmState.setPeerSlots(peerSlots);

                rscState.setRequiresAdjust(rscState.requiresAdjust() | vlmState.isMarkedForDelete());
            }
        }

        return rscState;
    }

    private String computeVlmName(ResourceDefinition rscDfn, VolumeNumber vlmNr)
    {
        String identifier;
        try
        {
            VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmNr);
            String overrideId = vlmDfn.getProps(wrkCtx).getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID);

            identifier =
                overrideId != null ?
                overrideId :
                computeVlmName(rscDfn.getName(), vlmNr);
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return identifier;
    }

    private String computeVlmName(ResourceName rscName, VolumeNumber vlmNr)
    {
        return rscName.displayValue + "_" + String.format("%05d", vlmNr.value);
    }

    /**
     * Entry point for the DeviceManager
     */
    @Override
    public void dispatchResource(
        ResourceDefinition rscDfn,
        Collection<Snapshot> inProgressSnapshots
    )
    {
        ResourceName rscName = rscDfn.getName();
        errLog.logTrace(
            "DrbdDeviceHandler: dispatchRsc() - Begin actions: Resource '" +
                rscName.displayValue + "'"
        );

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        boolean resourceDeleted = false;
        try
        {
            Node localNode = controllerPeerConnector.getLocalNode();
            NodeName localNodeName = localNode.getName();

            Resource rsc = rscDfn.getResource(wrkCtx, localNodeName);
            if (rsc != null)
            {
                // Volatile state information of the resource and its volumes
                ResourceState rscState = initializeResourceState(
                    rscDfn,
                    rscDfn.streamVolumeDfn(wrkCtx).map(VolumeDefinition::getVolumeNumber).collect(Collectors.toSet())
                );

                // Evaluate resource & volumes state by checking the DRBD state
                evaluateDrbdResource(localNodeName, rscDfn, rscState);

                fillResourceState(rsc, rscState);

                if (rsc.getStateFlags().isSet(wrkCtx, Resource.RscFlags.DELETE) ||
                    rscDfn.getFlags().isSet(wrkCtx, ResourceDefinition.RscDfnFlags.DELETE))
                {
                    deleteResource(rsc, rscDfn, rscState);
                    apiCallRc.addEntries(makeDeleteRc(rsc, rscName, localNodeName));
                    resourceDeleted = true;
                }
                else
                {
                    createResource(localNode, rscName, rsc, rscDfn, rscState);
                    apiCallRc.addEntry("Resource '" + rscName + "' deployed", ApiConsts.CREATED);
                }
            }

            {
                Set<VolumeNumber> snapshotVlmNumbers = new HashSet<>();
                for (Snapshot snapshot : inProgressSnapshots)
                {
                    snapshotVlmNumbers.addAll(
                        snapshot.getAllSnapshotVolumes(wrkCtx).stream()
                            .map(SnapshotVolume::getVolumeNumber)
                            .collect(Collectors.toSet())
                    );
                }

                // Volatile state information of the resource and its volumes
                ResourceState rscState = initializeResourceState(
                    rscDfn,
                    snapshotVlmNumbers
                );

                // Evaluate resource & volumes state by checking the DRBD state
                evaluateDrbdResource(localNodeName, rscDfn, rscState);

                handleSnapshots(rscName, inProgressSnapshots, rscState);
            }
        }
        catch (ResourceException rscExc)
        {
            ResponseUtils.reportStatic(
                rscExc, rscExc.getMessage(), rscExc.getCauseText(), rscExc.getDetailsText(), rscExc.getCorrectionText(),
                ApiConsts.FAIL_UNKNOWN_ERROR, null, apiCallRc, errLog, null, null
            );
        }
        catch (AccessDeniedException accExc)
        {
            ResponseUtils.reportStatic(
                accExc, "Satellite worker access context not authorized to perform a required operation",
                ApiConsts.FAIL_IMPL_ERROR,
                null, apiCallRc, errLog, null, null
            );
        }
        catch (NoInitialStateException drbdStateExc)
        {
            ResponseUtils.reportStatic(
                drbdStateExc,
                "DRBD state tracking is unavailable, operations on resource '" + rscName.displayValue +
                    "' were aborted.",
                getAbortMsg(rscName),
                "DRBD state tracking is unavailable",
                "Operations will continue automatically when DRBD state tracking is recovered",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null, apiCallRc, errLog, null, null
            );
        }
        catch (Exception | ImplementationError exc)
        {
            ResponseUtils.reportStatic(
                exc, exc.getMessage() == null ? exc.getClass().getSimpleName() : exc.getMessage(),
                ApiConsts.FAIL_UNKNOWN_ERROR, null, apiCallRc, errLog, null, null
            );
        }

        deviceManagerProvider.get().notifyResourceDispatchResponse(rscName, apiCallRc);

        try
        {
            ObjectIdentifier objectIdentifier = ObjectIdentifier.resourceDefinition(rscName);
            resourceDeploymentStateEvent.get().triggerEvent(objectIdentifier, apiCallRc);
            if (resourceDeleted)
            {
                resourceDeploymentStateEvent.get().closeStream(objectIdentifier);
            }
        }
        catch (Exception exc)
        {
            errLog.reportError(
                Level.ERROR,
                new ImplementationError(
                    "Exception updating resource deployment state",
                    exc
                )
            );
        }

        errLog.logTrace(
            "DrbdDeviceHandler: dispatchRsc() - End actions: Resource '" +
            rscName.displayValue + "'"
        );
    }

    private ApiCallRcImpl makeDeleteRc(Resource rsc, ResourceName rscName, NodeName localNodeName)
    {
        ApiCallRcImpl apiCallDelRc = new ApiCallRcImpl();

        // objrefs
        Map<String, String> objRefs = new HashMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscName.displayValue);
        objRefs.put(ApiConsts.KEY_NODE, localNodeName.displayValue);

        // varRefs
        Map<String, String> varRefs = new HashMap<>();
        varRefs.put(ApiConsts.KEY_RSC_NAME, rscName.displayValue);
        objRefs.put(ApiConsts.KEY_NODE_NAME, localNodeName.displayValue);

        ResponseUtils.reportSuccessStatic(
            String.format("Resource '%s' on node '%s' successfully deleted.",
                rscName.displayValue,
                localNodeName.displayValue),
            String.format("Resource '%s' on node '%s' with UUID '%s' deleted.",
                rscName.displayValue,
                localNodeName.displayValue,
                rsc.getUuid().toString()),
            ApiConsts.DELETED | ApiConsts.MASK_RSC | ApiConsts.MASK_SUCCESS,
            apiCallDelRc,
            objRefs,
            errLog
        );
        return apiCallDelRc;
    }

    private void sendRequestPrimaryResource(
        final Peer ctrlPeer,
        final String rscName,
        final String rscUuid
    )
    {
        byte[] data = interComSerializer
            .onewayBuilder(InternalApiConsts.API_REQUEST_PRIMARY_RSC)
            .primaryRequest(rscName, rscUuid)
            .build();

        if (data != null)
        {
            ctrlPeer.sendMessage(data);
        }
    }

    private void evaluateDelDrbdResource(
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState
    )
        throws NoInitialStateException
    {
        // Check whether the resource is known to DRBD
        DrbdResource drbdRsc = drbdState.getDrbdResource(rscDfn.getName().displayValue);
        if (drbdRsc != null)
        {
            rscState.setPresent(true);
            evaluateDrbdRole(rscState, drbdRsc);
        }
        else
        {
            rscState.setPresent(false);
            rscState.setPrimary(false);
        }
    }

    private void evaluateDrbdResource(
        NodeName localNodeName,
        ResourceDefinition rscDfn,
        ResourceState rscState
    )
        throws NoInitialStateException, AccessDeniedException
    {
        // Evaluate resource & volumes state by checking the DRBD state
        DrbdResource drbdRsc = drbdState.getDrbdResource(rscDfn.getName().displayValue);
        if (drbdRsc != null)
        {
            rscState.setPresent(true);
            evaluateDrbdRole(rscState, drbdRsc);
            evaluateDrbdConnections(localNodeName, rscDfn, rscState, drbdRsc);
            evaluateDrbdVolumes(rscState, drbdRsc);
            rscState.setSuspendedUser(drbdRsc.getSuspendedUser() == null ? false : drbdRsc.getSuspendedUser());
        }
        else
        {
            rscState.setRequiresAdjust(true);
        }
    }

    private void evaluateDrbdRole(
        ResourceState rscState,
        DrbdResource drbdRsc
    )
    {
        DrbdResource.Role rscRole = drbdRsc.getRole();
        if (rscRole == DrbdResource.Role.UNKNOWN)
        {
            rscState.setRequiresAdjust(true);
        }
        else
        if (rscRole == DrbdResource.Role.PRIMARY)
        {
            rscState.setPrimary(true);
        }
    }

    private void evaluateDrbdConnections(
        NodeName localNodeName,
        ResourceDefinition rscDfn,
        ResourceState rscState,
        DrbdResource drbdRsc
    )
        throws NoInitialStateException, AccessDeniedException
    {
        // Check connections to peer resources
        // TODO: Check for missing connections
        // TODO: Check for connections that should not be there
        Iterator<Resource> peerRscIter = rscDfn.iterateResource(wrkCtx);
        while (!rscState.requiresAdjust() && peerRscIter.hasNext())
        {
            Resource peerRsc = peerRscIter.next();

            // Skip the local resource
            if (!peerRsc.getAssignedNode().getName().equals(localNodeName))
            {
                DrbdConnection drbdConn = drbdRsc.getConnection(localNodeName.displayValue);
                if (drbdConn != null)
                {
                    DrbdConnection.State connState = drbdConn.getState();
                    switch (connState)
                    {
                        case STANDALONE:
                            // fall-through
                        case DISCONNECTING:
                            // fall-through
                        case UNCONNECTED:
                            // fall-through
                        case TIMEOUT:
                            // fall-through
                        case BROKEN_PIPE:
                            // fall-through
                        case NETWORK_FAILURE:
                            // fall-through
                        case PROTOCOL_ERROR:
                            // fall-through
                        case TEAR_DOWN:
                            // fall-through
                        case UNKNOWN:
                            // fall-through
                            rscState.setRequiresAdjust(true);
                            break;
                        case CONNECTING:
                            break;
                        case CONNECTED:
                            break;
                        default:
                            throw new ImplementationError(
                                "Missing switch case for enumeration value '" +
                                connState.name() + "'",
                                null
                            );
                    }
                }
                else
                {
                    // Missing connection
                    rscState.setRequiresAdjust(true);
                }
            }
        }
    }

    private void evaluateDrbdVolumes(
        ResourceState rscState,
        DrbdResource drbdRsc
    )
    {
        Map<VolumeNumber, DrbdVolume> volumes = drbdRsc.getVolumesMap();
        for (VolumeState vlmState : rscState.getVolumes())
        {
            DrbdVolume drbdVlm = volumes.remove(vlmState.getVlmNr());
            if (drbdVlm != null)
            {
                vlmState.setPresent(true);
                DrbdVolume.DiskState diskState = drbdVlm.getDiskState();
                vlmState.setDiskState(diskState.toString());
                switch (diskState)
                {
                    case DISKLESS:
                        if (!drbdVlm.isClient())
                        {
                            vlmState.setDiskFailed(true);
                            rscState.setRequiresAdjust(true);
                        }
                        break;
                    case DETACHING:
                        // TODO: May be a transition from storage to client
                        // fall-through
                    case FAILED:
                        vlmState.setDiskFailed(true);
                        // fall-through
                    case NEGOTIATING:
                        // fall-through
                    case UNKNOWN:
                        // The local disk state should not be unknown,
                        // try adjusting anyways
                        rscState.setRequiresAdjust(true);
                        break;
                    case UP_TO_DATE:
                        // fall-through
                    case CONSISTENT:
                        // fall-through
                    case INCONSISTENT:
                        // fall-through
                    case OUTDATED:
                        vlmState.setHasMetaData(true);
                        // No additional check for existing meta data is required
                        vlmState.setCheckMetaData(false);
                        // fall-through
                    case ATTACHING:
                        vlmState.setHasDisk(true);
                        break;
                    default:
                        throw new ImplementationError(
                            "Missing switch case for enumeration value '" +
                            diskState.name() + "'",
                            null
                        );
                }
            }
            else
            {
                // Missing volume, adjust the resource
                rscState.setRequiresAdjust(true);
            }
        }
        if (!volumes.isEmpty())
        {
            // The DRBD resource has additional unknown volumes,
            // adjust the resource
            rscState.setRequiresAdjust(true);
        }
    }

    private void ensureStorageDriver(
        ResourceName rscName,
        StorPool storagePool,
        VolumeStateDevManager vlmState
    )
        throws AccessDeniedException, VolumeException
    {
        if (vlmState.getDriver() == null)
        {
            try
            {
                StorageDriver driver = null;
                if (storagePool == null)
                {
                    throw new VolumeException(
                        "No storage pool set for volume " + vlmState.getVlmNr().value +
                            " of resource '" + rscName.displayValue + "'"
                    );
                }

                driver = storagePool.getDriver(wrkCtx, errLog, fileSystemWatch, timer, stltCfgAccessor);
                if (driver == null)
                {
                    throw new VolumeException(
                        "Cannot find driver for storage pool '" + storagePool.getName().displayValue + "' for volume " +
                            vlmState.getVlmNr().value + " of resource '" + rscName.displayValue + "'"
                    );
                }

                Optional<Props> optNodeNamespace = storagePool
                    .getNode()
                    .getProps(wrkCtx)
                    .getNamespace(ApiConsts.NAMESPC_STORAGE_DRIVER);
                ReadOnlyProps nodeNamespace;
                if (optNodeNamespace.isPresent())
                {
                    nodeNamespace = new ReadOnlyProps(optNodeNamespace.get());
                }
                else
                {
                    nodeNamespace = ReadOnlyProps.emptyRoProps();
                }
                storagePool.reconfigureStorageDriver(
                    driver,
                    nodeNamespace,
                    stltCfgAccessor.getReadonly(ApiConsts.NAMESPC_STORAGE_DRIVER)
                );
                vlmState.setDriver(driver);
            }
            catch (StorageException storExc)
            {
                throw new ImplementationError(
                    "Storage configuration exception",
                    storExc
                );
            }
        }
    }

    private void evaluateStorageVolume(
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn,
        VolumeStateDevManager vlmState
    )
        throws AccessDeniedException, VolumeException, MdException
    {
        // Evaluate volumes state by checking for the presence of backend-storage
        Volume vlm = rsc.getVolume(vlmState.getVlmNr());
        VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmState.getVlmNr());
        errLog.logTrace(
            "Evaluating storage volume for resource '" + rscDfn.getName().displayValue + "' " +
            "volume " + vlmState.getVlmNr().toString()
        );

        ensureStorageDriver(rscName, vlm.getStorPool(wrkCtx), vlmState);

        StorageDriver storDrv = vlmState.getDriver();

        // TODO: change to createNamespace as soon as it is implemented
        Props vlmDfnProps = vlm.getVolumeDefinition().getProps(wrkCtx);

        try
        {
            if (!vlmState.hasDisk())
            {
                boolean isEncrypted = vlmDfn.getFlags().isSet(wrkCtx, VlmDfnFlags.ENCRYPTED);
                vlmState.setHasDisk(storDrv.volumeExists(
                    vlmState.getStorVlmName(), isEncrypted, vlmDfnProps));

                if (!vlmState.hasDisk())
                {
                    attemptVolumeStart(vlmDfn, vlmState, storDrv);

                    vlmState.setHasDisk(storDrv.volumeExists(
                        vlmState.getStorVlmName(), isEncrypted, vlmDfnProps));
                }
            }

            if (vlmState.hasDisk())
            {
                // Calculate the backend storage volume's size
                long netSize = vlmDfn.getVolumeSize(wrkCtx);
                vlmState.setNetSize(netSize);
                long requiredSize = drbdMd.getGrossSize(
                    netSize, vlmState.getPeerSlots(), FIXME_STRIPES, FIXME_STRIPE_SIZE
                );

                // Check the size of the backend storage
                StorageDriver.SizeComparison sizeComparison =
                    storDrv.compareVolumeSize(vlmState.getStorVlmName(), requiredSize, vlmDfnProps);

                if (sizeComparison == StorageDriver.SizeComparison.TOO_SMALL)
                {
                    errLog.logDebug("Need to resize volume " + vlmDfn.getVolumeNumber().value + " of resource '" +
                        rscDfn.getName().displayValue);
                    vlmState.setDiskNeedsResize(true);
                }
                else if (sizeComparison == StorageDriver.SizeComparison.TOO_LARGE &&
                    !Boolean.valueOf(vlmDfn.getProps(wrkCtx).getProp(ApiConsts.KEY_ALLOW_LARGER_VOLUME_SIZE)))
                {
                    throw new VolumeException(
                        "Storage volume " + vlmDfn.getVolumeNumber().value + " of resource '" +
                            rscDfn.getName().displayValue + "' too large. Expected " + requiredSize +
                            "KiB, but was : " + storDrv.getSize(vlmState.getStorVlmName(), vlmDfnProps) + "KiB."
                    );
                }

                errLog.logTrace(
                    "Existing storage volume found for resource '" +
                        rscDfn.getName().displayValue + "' " + "volume " + vlmState.getVlmNr().toString()
                );
            }
        }
        catch (StorageException exc)
        {
            throw new VolumeException(
                "Storage error for volume " + vlmDfn.getVolumeNumber().value + " of resource '" +
                    rscDfn.getName().displayValue + "'",
                exc
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void attemptVolumeStart(VolumeDefinition vlmDfn, VolumeStateDevManager vlmState, StorageDriver storDrv)
        throws AccessDeniedException
    {
        try
        {
            storDrv.startVolume(vlmState.getStorVlmName(), vlmDfn.getKey(wrkCtx), vlmDfn.getProps(wrkCtx));
        }
        catch (StorageException exc)
        {
            // This can occur when the backend volume has not been created yet and the driver attempts to start
            // the encryption layer
        }
    }

    private void restoreStorageVolume(
        ResourceDefinition rscDfn,
        VolumeStateDevManager vlmState
    )
        throws VolumeException
    {
        try
        {
            VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmState.getVlmNr());
            vlmState.getDriver().restoreSnapshot(
                vlmState.getRestoreVlmName(),
                vlmState.getRestoreSnapshotName(),
                vlmState.getStorVlmName(),
                vlmDfn.getKey(wrkCtx),
                vlmDfn.getProps(wrkCtx)
            );
        }
        catch (StorageException storExc)
        {
            throw new VolumeException(
                "Storage volume restoration failed for resource '" + rscDfn.getName().displayValue + "' volume " +
                    vlmState.getVlmNr().value,
                getAbortMsg(rscDfn.getName(), vlmState.getVlmNr()),
                null,
                null,
                null,
                storExc
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void createStorageVolume(
        ResourceDefinition rscDfn,
        VolumeStateDevManager vlmState
    )
        throws MdException, VolumeException
    {
        try
        {
            vlmState.setGrossSize(drbdMd.getGrossSize(
                vlmState.getNetSize(), vlmState.getPeerSlots(), FIXME_STRIPES, FIXME_STRIPE_SIZE
            ));
            VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmState.getVlmNr());
            vlmState.getDriver().createVolume(
                vlmState.getStorVlmName(),
                vlmState.getGrossSize(),
                vlmDfn.getKey(wrkCtx),
                vlmDfn.getProps(wrkCtx)
            );
        }
        catch (StorageException storExc)
        {
            throw new VolumeException(
                "Storage volume creation failed for resource '" + rscDfn.getName().displayValue + "' volume " +
                vlmState.getVlmNr().value,
                getAbortMsg(rscDfn.getName(), vlmState.getVlmNr()),
                "Creation of the storage volume failed",
                "- Check whether there is sufficient space in the storage pool selected for the volume\n" +
                "- Check whether the storage pool is operating flawlessly\n",
                null,
                storExc
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void resizeStorageVolume(
        ResourceDefinition rscDfn,
        VolumeStateDevManager vlmState
    )
        throws MdException, VolumeException
    {
        try
        {
            vlmState.setGrossSize(drbdMd.getGrossSize(
                vlmState.getNetSize(), vlmState.getPeerSlots(), FIXME_STRIPES, FIXME_STRIPE_SIZE
            ));
            VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmState.getVlmNr());
            vlmState.getDriver().resizeVolume(
                vlmState.getStorVlmName(),
                vlmState.getGrossSize(),
                vlmDfn.getKey(wrkCtx),
                vlmDfn.getProps(wrkCtx)
            );
        }
        catch (StorageException storExc)
        {
            throw new VolumeException(
                "Storage volume creation failed for resource '" + rscDfn.getName().displayValue + "' volume " +
                vlmState.getVlmNr().value,
                getAbortMsg(rscDfn.getName(), vlmState.getVlmNr()),
                "Creation of the storage volume failed",
                "- Check whether there is sufficient space in the storage pool selected for the volume\n" +
                "- Check whether the storage pool is operating flawlessly\n",
                null,
                storExc
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void deleteStorageVolume(
        ResourceDefinition rscDfn,
        VolumeStateDevManager vlmState,
        boolean onlyDeleteDisk
    )
        throws VolumeException
    {
        try
        {
            boolean isEncrypted = rscDfn.getVolumeDfn(wrkCtx, vlmState.getVlmNr()).getFlags()
                .isSet(wrkCtx, VlmDfnFlags.ENCRYPTED);
            Volume vlm = rscDfn.getResource(wrkCtx, controllerPeerConnector.getLocalNode().getName())
                .getVolume(vlmState.getVlmNr());

            vlmState.getDriver().deleteVolume(
                vlmState.getStorVlmName(),
                isEncrypted,
                vlm.getVolumeDefinition().getProps(wrkCtx)
            );

            if (!onlyDeleteDisk)
            {
                // Notify the controller of successful deletion of the resource
                deviceManagerProvider.get().notifyVolumeDeleted(
                    vlm,
                    vlmState.getDriver().getFreeSpace()
                );
            }
        }
        catch (StorageException storExc)
        {
            throw new VolumeException(
                "Deletion of the storage volume failed for resource '" + rscDfn.getName().displayValue +
                "' volume " + vlmState.getVlmNr().value,
                getAbortMsg(rscDfn.getName(), vlmState.getVlmNr()),
                "Deletion of the storage volume failed",
                "- Check whether the storage pool is operating flawlessly\n",
                null,
                storExc
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(
                "Worker access context has not enough privileges to access resource on satellite.",
                exc
            );
        }
    }

    private void createVolumeMetaData(
        ResourceDefinition rscDfn,
        Resource rsc,
        VolumeStateDevManager vlmState
    )
        throws ExtCmdFailedException, StorageException, ResourceException, VolumeException, AccessDeniedException
    {
        ResourceName rscName = rscDfn.getName();
        drbdUtils.createMd(rscName, vlmState.getVlmNr(), vlmState.getPeerSlots());

        if (isThinVlm(vlmState))
        {
            VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmState.getVlmNr());
            boolean isEncrypted = vlmDfn.getFlags().isSet(wrkCtx, VlmDfnFlags.ENCRYPTED);
            VolumeNumber vlmNr = vlmState.getVlmNr();

            Props vlmDfnProps = vlmDfn.getProps(wrkCtx);

            String currentGi = null;
            try
            {
                currentGi = vlmDfn.getProps(wrkCtx).getProp(ApiConsts.KEY_DRBD_CURRENT_GI);
            }
            catch (InvalidKeyException invKeyExc)
            {
                throw new ImplementationError(
                    "API constant contains an invalid key",
                    invKeyExc
                );
            }
            if (currentGi == null)
            {
                throw new VolumeException(
                    "Meta data creation for resource '" + rscDfn.getName().displayValue + "' volume " +
                    vlmNr.value + " failed",
                    getAbortMsg(rscName, vlmState.getVlmNr()),
                    "Volume " + vlmNr.value + " of the resource uses a thin provisioning storage driver,\n" +
                    "but no initial value for the DRBD current generation is set on the volume definition",
                    "- Ensure that the initial DRBD current generation is set on the volume definition\n" +
                    "or\n" +
                    "- Recreate the volume definition",
                    "The key of the initial DRBD current generation property is:\n" +
                    ApiConsts.KEY_DRBD_CURRENT_GI,
                    null
                );
            }
            drbdUtils.setGi(
                rsc.getNodeId(),
                vlmState.getMinorNr(),
                vlmState.getDriver().getVolumePath(vlmState.getStorVlmName(), isEncrypted, vlmDfnProps),
                currentGi,
                null,
                false
            );
        }
    }

    private void createResource(
        Node localNode,
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState
    )
        throws AccessDeniedException, ResourceException
    {
        createResourceStorage(rscName, rsc, rscDfn, rscState);

        if (rsc.getVolumeCount() == 0)
        {
            errLog.logDebug("Skipping DRBD configuration steps for empty resource '" + rscName + "'");
        }
        else if (allVolumesMarkedForDelete(rscState))
        {
            deleteDrbdResource(rscName);
        }
        else
        {
            createResourceConfiguration(rscName, rsc, rscDfn);

            createResourceMetaData(rscName, rsc, rscDfn, rscState);

            adjustResource(rsc, rscState);

            condInitialOrSkipSync(localNode, rscName, rsc, rscDfn, rscState);
        }

        deleteResourceVolumes(rscName, rsc, rscDfn, rscState);

        deviceManagerProvider.get().notifyResourceApplied(rsc);
    }

    private void createResourceStorage(
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState
    )
        throws AccessDeniedException, ResourceException
    {
        for (VolumeState vlmStateBase : rscState.getVolumes())
        {
            VolumeStateDevManager vlmState = (VolumeStateDevManager) vlmStateBase;
            try
            {
                // Check backend storage
                evaluateStorageVolume(
                    rscName, rsc, rscDfn, vlmState
                );

                if (!vlmState.isMarkedForDelete())
                {
                    Volume vlm = rsc.getVolume(vlmState.getVlmNr());

                    if (vlm != null)
                    {
                        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                        Props vlmDfnProps = vlmDfn.getProps(wrkCtx);

                        ensureStorageDriver(rscName, vlm.getStorPool(wrkCtx), vlmState);

                        // Check DRBD meta data
                        if (!vlmState.hasDisk())
                        {
                            errLog.logTrace(
                                "%s",
                                "Resource " + rscName.displayValue + " Volume " + vlmState.getVlmNr().value +
                                    " has no backend storage => hasMetaData = false, checkMetaData = false"
                            );
                            // If there is no disk, then there cannot be any meta data
                            vlmState.setCheckMetaData(false);
                            vlmState.setHasMetaData(false);
                        }
                        else if (vlmState.isCheckMetaData())
                        {
                            // Check for the existence of meta data
                            try
                            {
                                boolean isEncrypted = rscDfn.getVolumeDfn(wrkCtx, vlmState.getVlmNr()).getFlags()
                                    .isSet(wrkCtx, VlmDfnFlags.ENCRYPTED);

                                vlmState.setHasMetaData(drbdUtils.hasMetaData(
                                    vlmState.getDriver().getVolumePath(
                                        vlmState.getStorVlmName(), isEncrypted, vlmDfnProps
                                    ),
                                    vlmState.getMinorNr().value, "internal"
                                ));
                                errLog.logTrace(
                                    "%s",
                                    "Resource " + rscName.displayValue + " Volume " + vlmState.getVlmNr().value +
                                        " meta data check result: hasMetaData = " + vlmState.hasMetaData()
                                );
                            }
                            catch (ExtCmdFailedException cmdExc)
                            {
                                errLog.reportError(Level.ERROR, cmdExc);
                            }
                        }

                        // Create backend storage if required
                        if (!vlmState.hasDisk())
                        {
                            if (vlmState.getRestoreVlmName() != null && vlmState.getRestoreSnapshotName() != null)
                            {
                                restoreStorageVolume(rscDfn, vlmState);
                                vlmState.setHasMetaData(true);
                            }
                            else
                            {
                                createStorageVolume(rscDfn, vlmState);
                                vlmState.setHasMetaData(false);
                            }

                            vlmState.setHasDisk(true);
                        }

                        // TODO: Wait for the backend storage block device files to appear in the /dev directory
                        //       if the volume is supposed to have backend storage

                        if (vlmState.diskNeedsResize())
                        {
                            resizeStorageVolume(rscDfn, vlmState);
                            vlmState.setDiskNeedsResize(false);
                        }

                        if (vlm.getFlags().isSet(wrkCtx, Volume.VlmFlags.RESIZE))
                        {
                            // Notify the controller that the volume was resized even if we haven't actually changed the
                            // size, because notification may have been missed when it was actually resized.
                            deviceManagerProvider.get().notifyVolumeResized(vlm);
                        }

                        // Set block device paths
                        if (vlmState.hasDisk())
                        {
                            // store the real size of the volume (after applied extent size, etc)
                            vlm.setRealSize(
                                wrkCtx,
                                vlmState.getDriver().getSize(vlmState.getStorVlmName(), vlmDfnProps)
                            );

                            boolean isEncrypted = rscDfn.getVolumeDfn(wrkCtx, vlmState.getVlmNr()).getFlags()
                                .isSet(wrkCtx, VlmDfnFlags.ENCRYPTED);

                            String bdPath = vlmState.getDriver().getVolumePath(
                                vlmState.getStorVlmName(), isEncrypted, vlmDfnProps);
                            vlm.setBackingDiskPath(wrkCtx, bdPath);
                            vlm.setMetaDiskPath(wrkCtx, "internal");
                        }
                        else
                        {
                            vlm.setBackingDiskPath(wrkCtx, "none");
                            vlm.setMetaDiskPath(wrkCtx, null);
                        }
                        errLog.logTrace(
                            "Resource '" + rscName + "' volume " + vlmState.getVlmNr().toString() +
                                " block device = %s, meta disk = %s",
                            vlm.getBackingDiskPath(wrkCtx),
                            vlm.getMetaDiskPath(wrkCtx)
                        );
                    }
                    else
                    {
                        // If there is no volume for the volumeState, then LINSTOR does not know about
                        // this volume, and the volume will later be removed from the resource
                        // when the resource is adjusted.
                        // Therefore, the volume is ignored, and no backend storage is created for the volume
                        vlmState.setSkip(true);
                    }
                }
            }
            catch (MdException mdExc)
            {
                throw new ResourceException(
                    "Meta data calculation for resource '" + rscName.displayValue + "' volume " +
                        vlmState.getVlmNr().value + " failed",
                    "Operations on resource " + rscName.displayValue + " volume " + vlmState.getVlmNr().value +
                        " were aborted",
                    "The calculation of the volume's DRBD meta data size failed",
                    "Check whether the volume's properties, such as size, DRBD peer count and activity log " +
                        "settings, are within the range supported by DRBD",
                    mdExc.getMessage(),
                    mdExc
                );
            }
            catch (VolumeException vlmExc)
            {
                throw new ResourceException(
                    "Initialization of storage for resource '" + rscName.displayValue + "' volume " +
                        vlmState.getVlmNr().value + " failed",
                    null, vlmExc.getMessage(),
                    null, null, vlmExc
                );
            }
            catch (StorageException storExc)
            {
                throw new ResourceException(
                    "The storage driver could not determine the block device path for " +
                        "volume " + vlmState.getVlmNr() + " of resource " + rscName.displayValue,
                    this.getAbortMsg(rscName, vlmState.getVlmNr()),
                    "The storage driver could not determine the block device path for the volume's " +
                        "backend storage",
                    "- Check whether the storage driver is configured correctly\n" +
                        "- Check whether any external programs required by the storage driver are\n" +
                        "  functional\n",
                    null,
                    storExc
                );
            }
        }
    }

    private void createResourceConfiguration(
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn
    )
        throws AccessDeniedException, ResourceException
    {
        errLog.logTrace(
            "%s",
            "Creating resource " + rscName.displayValue + " configuration file"
        );

        List<Resource> peerResources = new ArrayList<>();
        {
            Iterator<Resource> peerRscIter = rscDfn.iterateResource(wrkCtx);
            while (peerRscIter.hasNext())
            {
                Resource peerRsc = peerRscIter.next();
                if (peerRsc != rsc)
                {
                    peerResources.add(peerRsc);
                }
            }
        }

        try (
            FileOutputStream resFileOut = new FileOutputStream(
                SatelliteCoreModule.CONFIG_PATH + "/" + rscName.displayValue + DRBD_CONFIG_SUFFIX
            )
        )
        {
            String content = new ConfFileBuilder(
                this.errLog,
                wrkCtx,
                rsc,
                peerResources,
                whitelistProps
            ).build();
            resFileOut.write(content.getBytes());
        }
        catch (IOException ioExc)
        {
            String ioErrorMsg = ioExc.getMessage();
            if (ioErrorMsg == null)
            {
                ioErrorMsg = "The runtime environment or operating system did not provide a description of " +
                    "the I/O error";
            }
            throw new ResourceException(
                "Creation of the DRBD configuration file for resource '" + rscName.displayValue +
                    "' failed due to an I/O error",
                getAbortMsg(rscName),
                "Creation of the DRBD configuration file failed due to an I/O error",
                "- Check whether enough free space is available for the creation of the file\n" +
                    "- Check whether the application has write access to the target directory\n" +
                    "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                ioExc
            );
        }
    }

    private void createResourceMetaData(
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState
    )
        throws ResourceException, AccessDeniedException
    {
        for (VolumeState vlmStateBase : rscState.getVolumes())
        {
            VolumeStateDevManager vlmState = (VolumeStateDevManager) vlmStateBase;
            if (!(vlmState.isSkip() || vlmState.isMarkedForDelete()))
            {
                try
                {
                    if (vlmState.hasDisk() && !vlmState.hasMetaData())
                    {
                        errLog.logTrace(
                            "%s",
                            "Creating resource " + rscName.displayValue + " Volume " +
                            vlmState.getVlmNr().value + " meta data"
                        );
                        createVolumeMetaData(rscDfn, rsc, vlmState);
                    }
                }
                catch (StorageException storExc)
                {
                    throw new ResourceException(
                        "Meta data creation for resource '" + rscName.displayValue + "' volume " +
                        vlmState.getVlmNr().value + " failed",
                        getAbortMsg(rscName),
                        "Meta data creation failed due to an error of the storage driver",
                        "Check the configuration of the storage pool associated with the volume",
                        null,
                        storExc
                    );
                }
                catch (VolumeException vlmExc)
                {
                    throw new ResourceException(
                        "Meta data creation for resource '" + rscName.displayValue + "' volume " +
                        vlmState.getVlmNr().value + " failed",
                        getAbortMsg(rscName),
                        "Meta data creation for volume " + vlmState.getVlmNr().value + " +" +
                        "failed",
                        null,
                        null,
                        vlmExc
                    );
                }
                catch (ExtCmdFailedException cmdExc)
                {
                    throw new ResourceException(
                        "Meta data creation for resource '" + rscName.displayValue + "' volume " +
                        vlmState.getVlmNr().value + " failed",
                        getAbortMsg(rscName),
                        "Meta data creation for volume " + vlmState.getVlmNr().value + " +" +
                        "failed because the execution of an external command failed",
                        "- Check whether the required software is installed\n" +
                        "- Check whether the application's search path includes the location\n" +
                        "  of the external software\n" +
                        "- Check whether the application has execute permission for " +
                        "the external command\n",
                        null,
                        cmdExc
                    );
                }
            }
        }
    }

    private void adjustResource(Resource rsc, ResourceState rscState)
        throws ResourceException, AccessDeniedException
    {
        ResourceName rscName = rsc.getDefinition().getName();

        if (rscState.requiresAdjust())
        {
            errLog.logTrace(
                "%s",
                "Adjusting resource " + rscName.displayValue
            );
            try
            {
                drbdUtils.adjust(rscName, false, false, false, null);
            }
            catch (ExtCmdFailedException cmdExc)
            {
                throw new ResourceException(
                    "Adjusting the DRBD state of resource '" + rscName.displayValue + "' failed",
                    getAbortMsg(rscName),
                    "The external command for adjusting the DRBD state of the resource failed",
                    "- Check whether the required software is installed\n" +
                        "- Check whether the application's search path includes the location\n" +
                        "  of the external software\n" +
                        "- Check whether the application has execute permission for the external command\n",
                    null,
                    cmdExc
                );
            }
        }

        Iterator<Volume> vlmIter = rsc.iterateVolumes();
        while (vlmIter.hasNext())
        {
            Volume vlm = vlmIter.next();

            if (vlm.getFlags().isSet(wrkCtx, Volume.VlmFlags.DRBD_RESIZE))
            {
                VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();
                VolumeStateDevManager vlmState = (VolumeStateDevManager) rscState.getVolumeState(vlmNr);
                resizeDrbdResource(rscName, vlmNr, vlmState);

                deviceManagerProvider.get().notifyDrbdVolumeResized(vlm);
            }
        }
    }

    private void resizeDrbdResource(ResourceName rscName, VolumeNumber vlmNr, VolumeStateDevManager vlmState)
        throws ResourceException
    {
        try
        {
            boolean assumeCleanFlag = isThinVlm(vlmState);
            drbdUtils.resize(rscName, vlmNr, assumeCleanFlag);
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new ResourceException(
                "Resizing DRBD volume " + vlmNr + " of resource '" + rscName.displayValue + "' failed",
                getAbortMsg(rscName, vlmNr),
                "The external command for resizing the DRBD state of the resource failed",
                null,
                null,
                cmdExc
            );
        }
    }

    private boolean allVolumesMarkedForDelete(ResourceState rscState)
        throws AccessDeniedException, ResourceException
    {
        boolean deletingAll = true;
        for (VolumeState vlmStateBase : rscState.getVolumes())
        {
            VolumeStateDevManager vlmState = (VolumeStateDevManager) vlmStateBase;

            if (!vlmState.isMarkedForDelete() && !vlmState.isSkip())
            {
                deletingAll = false;
            }
        }
        return deletingAll;
    }

    private void deleteResourceVolumes(
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState
    )
        throws AccessDeniedException, ResourceException
    {
        // Delete volumes
        for (VolumeState vlmStateBase : rscState.getVolumes())
        {
            VolumeStateDevManager vlmState = (VolumeStateDevManager) vlmStateBase;

            try
            {
                boolean removingDisk = rsc.getStateFlags().isSet(wrkCtx, Resource.RscFlags.DISK_REMOVING);
                if ((vlmState.isMarkedForDelete() || removingDisk) && !vlmState.isSkip())
                {
                    Volume vlm = rsc.getVolume(vlmState.getVlmNr());
                    VolumeDefinition vlmDfn = vlm != null ? vlm.getVolumeDefinition() : null;
                    // If this is a volume state that describes a volume seen by DRBD but not known
                    // to LINSTOR (e.g., manually configured in DRBD), then no volume object and
                    // volume definition object will be present.
                    // The backing storage for such volumes is ignored, as they are not managed
                    // by LINSTOR.
                    if (vlm != null && vlmDfn != null)
                    {
                        ensureStorageDriver(rscName, vlm.getStorPool(wrkCtx), vlmState);
                        deleteStorageVolume(rscDfn, vlmState, removingDisk);
                    }
                }
            }
            catch (VolumeException vlmExc)
            {
                throw new ResourceException(
                    "Deletion of storage for resource '" + rscName.displayValue + "' volume " +
                        vlmState.getVlmNr().value + " failed",
                    vlmExc
                );
            }
        }
    }

    private void condInitialOrSkipSync(
        Node localNode,
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState
    )
        throws AccessDeniedException, ResourceException
    {
        try
        {
            if (rscDfn.getProps(wrkCtx).getProp(InternalApiConsts.PROP_PRIMARY_SET) == null &&
                rsc.getStateFlags().isUnset(wrkCtx, Resource.RscFlags.DISKLESS))
            {
                errLog.logTrace("Requesting primary on %s", rscName.getDisplayName());
                sendRequestPrimaryResource(
                    localNode.getPeer(wrkCtx),
                    rscDfn.getName().getDisplayName(),
                    rsc.getUuid().toString()
                );
            }
            else
            if (rsc.isCreatePrimary() && !rscState.isPrimary())
            {
                // First, skip the resync on all thinly provisioned volumes
                boolean haveFatVlm = false;
                Iterator<Volume> vlmIter = rsc.iterateVolumes();
                while (vlmIter.hasNext())
                {
                    Volume vlm = vlmIter.next();
                    VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();
                    VolumeState vlmState = rscState.getVolumeState(vlmNr);
                    if (isThinVlm((VolumeStateDevManager) vlmState))
                    {
                        skipSync(rscName, vlmNr);
                    }
                    else
                    {
                        haveFatVlm = true;
                    }
                }

                // Set the resource primary (--force) to trigger an initial sync of all
                // fat provisioned volumes
                ((ResourceData) rsc).unsetCreatePrimary();
                if (haveFatVlm)
                {
                    errLog.logTrace("Setting resource primary on %s", rscName.getDisplayName());
                    setResourcePrimary(rsc);
                }
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void setResourcePrimary(
        Resource rsc
    )
        throws ResourceException
    {
        ResourceName rscName = rsc.getDefinition().getName();
        try
        {
            drbdUtils.primary(rscName, true, false);
            // setting to secondary because of two reasons:
            // * bug in drbdsetup: cannot down a primary resource
            // * let the user choose which satellite should be primary (or let it be handled by auto-promote)
            drbdUtils.secondary(rscName);
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new ResourceException(
                "Starting the initial resync of the DRBD resource '" + rscName.getDisplayName() + " failed",
                getAbortMsg(rscName),
                "The external command for changing the DRBD resource's role failed",
                "- Check whether the required software is installed\n" +
                "- Check whether the application's search path includes the location\n" +
                "  of the external software\n" +
                "- Check whether the application has execute permission for the external command\n",
                null,
                cmdExc
            );
        }
    }

    private void skipSync(
        ResourceName rscName,
        VolumeNumber vlmNr
    )
        throws ResourceException
    {
        try
        {
            drbdUtils.newCurrentUuid(rscName, vlmNr);
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new ResourceException(
                "Skipping the initial resync of the DRBD resource '" + rscName.getDisplayName() + " volume " +
                vlmNr.value + " failed",
                getAbortMsg(rscName),
                "The external command for adjusting the state of the DRBD resource's generation identifiers failed",
                "- Check whether the required software is installed\n" +
                "- Check whether the application's search path includes the location\n" +
                "  of the external software\n" +
                "- Check whether the application has execute permission for the external command\n",
                null,
                cmdExc
            );
        }
    }

    private void handleSnapshots(ResourceName rscName, Collection<Snapshot> snapshots, ResourceState rscState)
        throws ResourceException, AccessDeniedException, StorageException
    {
        errLog.logTrace("Handle snapshots for " + rscName.getDisplayName());

        boolean snapshotInProgress = false;
        boolean shouldSuspend = false;
        for (Snapshot snapshot : snapshots)
        {
            if (errLog.isTraceEnabled())
            {
                errLog.logTrace("Snapshot " + snapshot.getSnapshotDefinition() + " target state '" +
                    new StringUtils.ConditionalStringJoiner(", ")
                        .addIf(snapshot.getFlags().isSet(wrkCtx, Snapshot.SnapshotFlags.DELETE), "delete")
                        .addIf(snapshot.getSuspendResource(wrkCtx), "suspend")
                        .addIf(snapshot.getTakeSnapshot(wrkCtx), "take")
                        .toString() + "'");
            }

            if (!snapshot.getFlags().isSet(wrkCtx, Snapshot.SnapshotFlags.DELETE))
            {
                snapshotInProgress = true;
            }

            if (snapshot.getSuspendResource(wrkCtx))
            {
                shouldSuspend = true;
            }
        }

        boolean isSuspended = rscState.isSuspendedUser();

        adjustSuspended(rscName, shouldSuspend, isSuspended);

        for (Snapshot snapshot : snapshots)
        {
            SnapshotName snapshotName = snapshot.getSnapshotName();

            boolean snapshotTaken;
            boolean snapshotDeleted;

            if (snapshot.getFlags().isSet(wrkCtx, Snapshot.SnapshotFlags.DELETE))
            {
                for (SnapshotVolume snapshotVolume : snapshot.getAllSnapshotVolumes(wrkCtx))
                {
                    VolumeStateDevManager vlmState = (VolumeStateDevManager) rscState.getVolumeState(
                        snapshotVolume.getVolumeNumber());

                    try
                    {
                        ensureStorageDriver(rscName, snapshotVolume.getStorPool(wrkCtx), vlmState);

                        deleteVolumeSnapshot(
                            snapshotVolume,
                            snapshotName,
                            vlmState
                        );
                    }
                    catch (VolumeException vlmExc)
                    {
                        throw new ResourceException(
                            "Deletion of snapshot '" + snapshotName +
                                "' for resource '" + rscName.displayValue +
                                "' volume " + vlmState.getVlmNr().value + " failed",
                            null, vlmExc.getMessage(),
                            null, null, vlmExc
                        );
                    }
                }

                deviceManagerProvider.get().notifySnapshotDeleted(snapshot);

                snapshotTaken = true;
                snapshotDeleted = true;
            }
            else
            {
                if (snapshot.getTakeSnapshot(wrkCtx))
                {
                    for (SnapshotVolume snapshotVolume : snapshot.getAllSnapshotVolumes(wrkCtx))
                    {
                        VolumeStateDevManager vlmState = (VolumeStateDevManager) rscState.getVolumeState(
                            snapshotVolume.getVolumeNumber());

                        try
                        {
                            ensureStorageDriver(rscName, snapshotVolume.getStorPool(wrkCtx), vlmState);

                            takeVolumeSnapshot(
                                snapshot.getResourceDefinition(),
                                snapshotName,
                                vlmState
                            );
                        }
                        catch (VolumeException vlmExc)
                        {
                            throw new ResourceException(
                                "Deployment of snapshot '" + snapshotName +
                                    "' for resource '" + rscName.displayValue +
                                    "' volume " + vlmState.getVlmNr().value + " failed",
                                null, vlmExc.getMessage(),
                                null, null, vlmExc
                            );
                        }
                    }

                    snapshotTaken = true;
                }
                else
                {
                    snapshotTaken = false;
                }
                snapshotDeleted = false;
            }

            ObjectIdentifier objectIdentifier = ObjectIdentifier.snapshotDefinition(rscName, snapshotName);

            inProgressSnapshotEvent.get().triggerEvent(objectIdentifier, new SnapshotState(
                shouldSuspend,
                snapshotTaken,
                snapshotDeleted
            ));

            if (snapshotDeleted)
            {
                inProgressSnapshotEvent.get().closeStream(objectIdentifier);
            }
        }
    }

    private void adjustSuspended(
        ResourceName rscName,
        boolean shouldSuspend,
        boolean isSuspended
    )
        throws ResourceException
    {
        if (shouldSuspend && !isSuspended)
        {
            try
            {
                drbdUtils.suspendIo(rscName);
            }
            catch (ExtCmdFailedException cmdExc)
            {
                throw new ResourceException(
                    "Suspend of the DRBD resource '" + rscName.displayValue + " failed",
                    getAbortMsg(rscName),
                    "The external command for suspending the DRBD resource failed",
                    null, null, cmdExc
                );
            }
        }
        else if (!shouldSuspend && isSuspended)
        {
            try
            {
                drbdUtils.resumeIo(rscName);
            }
            catch (ExtCmdFailedException cmdExc)
            {
                throw new ResourceException(
                    "Resume of the DRBD resource '" + rscName.displayValue + " failed",
                    getAbortMsg(rscName),
                    "The external command for resuming the DRBD resource failed",
                    null, null, cmdExc
                );
            }
        }
    }

    private void deleteVolumeSnapshot(
        SnapshotVolume snapshotVolume,
        SnapshotName snapshotName,
        VolumeStateDevManager vlmState
    )
        throws VolumeException, AccessDeniedException, StorageException
    {
        ResourceDefinition rscDfn = snapshotVolume.getResourceDefinition();

        try
        {
            vlmState.getDriver().deleteSnapshot(
                vlmState.getStorVlmName(),
                snapshotName.displayValue
            );
        }
        catch (StorageException storExc)
        {
            throw new VolumeException(
                "Snapshot deletion failed for resource '" + rscDfn.getName().displayValue + "' volume " +
                    vlmState.getVlmNr().value,
                getAbortMsg(rscDfn.getName(), vlmState.getVlmNr()),
                "Deletion of the snapshot failed",
                null,
                null,
                storExc
            );
        }
    }

    private void takeVolumeSnapshot(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        VolumeStateDevManager vlmState
    )
        throws VolumeException
    {
        if (!DrbdVolume.DS_LABEL_UP_TO_DATE.equals(vlmState.getDiskState()))
        {
            throw new VolumeException("Refusing to take snapshot for non-UpToDate " +
                "resource '" + rscDfn.getName().displayValue + "' volume " + vlmState.getVlmNr().value);
        }

        boolean exists;
        try
        {
            exists = vlmState.getDriver().snapshotExists(
                vlmState.getStorVlmName(),
                snapshotName.displayValue
            );
        }
        catch (StorageException storExc)
        {
            throw new VolumeException(
                "Snapshot existence check failed for resource '" + rscDfn.getName().displayValue + "' volume " +
                    vlmState.getVlmNr().value,
                getAbortMsg(rscDfn.getName(), vlmState.getVlmNr()),
                null,
                null,
                null,
                storExc
            );
        }

        if (!exists)
        {
            try
            {
                vlmState.getDriver().createSnapshot(
                    vlmState.getStorVlmName(),
                    snapshotName.displayValue
                );
            }
            catch (StorageException storExc)
            {
                throw new VolumeException(
                    "Snapshot creation failed for resource '" + rscDfn.getName().displayValue + "' volume " +
                        vlmState.getVlmNr().value,
                    getAbortMsg(rscDfn.getName(), vlmState.getVlmNr()),
                    "Creation of the snapshot failed",
                    null,
                    null,
                    storExc
                );
            }
        }
    }

    /**
     * Deletes a resource
     * (and will somehow have to inform the device manager, or directly the controller, if the resource
     * was successfully deleted)
     */
    private void deleteResource(
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState
    )
        throws AccessDeniedException, ResourceException, NoInitialStateException
    {
        ResourceName rscName = rscDfn.getName();

        // Determine the state of the DRBD resource
        evaluateDelDrbdResource(rsc, rscDfn, rscState);

        if (rsc.getVolumeCount() == 0)
        {
            errLog.logDebug("Skipping DRBD configuration deletion steps for empty resource '" + rscName + "'");
        }
        else
        {
            deleteDrbdResource(rscName);
        }

        boolean vlmDelFailed = false;
        // Delete backend storage volumes
        for (VolumeState vlmStateBase : rscState.getVolumes())
        {
            VolumeStateDevManager vlmState = (VolumeStateDevManager) vlmStateBase;

            try
            {
                Volume vlm = rsc.getVolume(vlmState.getVlmNr());
                VolumeDefinition vlmDfn = vlm != null ? vlm.getVolumeDefinition() : null;
                // If this is a volume state that describes a volume seen by DRBD but not known
                // to LINSTOR (e.g., manually configured in DRBD), then no volume object and
                // volume definition object will be present.
                // The backing storage for such volumes is ignored, as they are not managed
                // by LINSTOR.
                if (vlm != null && vlmDfn != null)
                {
                    ensureStorageDriver(rscName, vlm.getStorPool(wrkCtx), vlmState);
                    deleteStorageVolume(rscDfn, vlmState, false);
                }
            }
            catch (VolumeException vlmExc)
            {
                vlmDelFailed = true;
                errLog.reportProblem(Level.ERROR, vlmExc, null, null, null);
            }
        }
        if (vlmDelFailed)
        {
            throw new ResourceException(
                "Deletion of resource '" + rscName.displayValue + "' failed because deletion of the resource's " +
                "volumes failed",
                getAbortMsg(rscName),
                "Deletion of at least one of the resource's volumes failed",
                "Review the reports and/or log entries for the failed operations on the resource's volumes\n" +
                "for more information on the cause of the error and possible correction measures",
                null
            );
        }

        // Notify the controller of successful deletion of the resource
        deviceManagerProvider.get().notifyResourceDeleted(rsc);
    }

    private void deleteDrbdResource(ResourceName rscName)
        throws ResourceException
    {
        // Shut down the DRBD resource
        try
        {
            drbdUtils.down(rscName);
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new ResourceException(
                "Shutdown of the DRBD resource '" + rscName.displayValue + " failed",
                getAbortMsg(rscName),
                "The external command for stopping the DRBD resource failed",
                "- Check whether the required software is installed\n" +
                    "- Check whether the application's search path includes the location\n" +
                    "  of the external software\n" +
                    "- Check whether the application has execute permission for the external command\n",
                null,
                cmdExc
            );
        }

        // TODO: Wait for the DRBD resource to disappear

        // Delete the DRBD resource configuration file
        deleteResourceConfiguration(rscName);
    }

    /**
     * Deletes the DRBD resource configuration file for a resource
     *
     * @param rscName Name of the resource that should have its DRBD configuration file deleted
     * @throws ResourceException if deletion of the DRBD configuration file fails
     */
    private void deleteResourceConfiguration(ResourceName rscName)
        throws ResourceException
    {
        try
        {
            FileSystem dfltFs = FileSystems.getDefault();
            Path cfgFilePath = dfltFs.getPath(
                SatelliteCoreModule.CONFIG_PATH,
                rscName.displayValue + DRBD_CONFIG_SUFFIX
            );
            Files.delete(cfgFilePath);

            // Double-check whether the file exists
            File cfgFile = cfgFilePath.toFile();
            if (cfgFile.exists())
            {
                throw new IOException(
                    "File still exists after a delete operation reported successful completion"
                );
            }
        }
        catch (NoSuchFileException ignored)
        {
            // Failed deletion of a file that does not exist in the first place
            // is not an error
        }
        catch (IOException ioExc)
        {
            String ioErrorMsg = ioExc.getMessage();
            if (ioErrorMsg == null)
            {
                ioErrorMsg = "The runtime environment or operating system did not provide a description of " +
                    "the I/O error";
            }
            throw new ResourceException(
                "Deletion of the DRBD configuration file for resource '" + rscName.displayValue +
                "' failed due to an I/O error",
                getAbortMsg(rscName),
                "Deletion of the DRBD configuration file failed due to an I/O error",
                "- Check whether the application has write access to the target directory\n" +
                "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                ioExc
            );
        }
    }

    private boolean isThinVlm(VolumeStateDevManager vlmState)
        throws ResourceException
    {
        return vlmState.getDriver().getKind().usesThinProvisioning();
    }

    private String getAbortMsg(ResourceName rscName)
    {
        return "Operations on resource '" + rscName.displayValue + "' were aborted";
    }

    private String getAbortMsg(ResourceName rscName, VolumeNumber vlmNr)
    {
        return "Operations on volume " + vlmNr.value + " of resource '" + rscName.displayValue + "' were aborted";
    }

    public void debugListSatelliteObjects()
    {
        System.out.println();
        System.out.println("\u001b[1;31m== BEGIN DrbdDeviceHandler.debugListSatelliteObjects() ==\u001b[0m");
        if (controllerPeerConnector.getLocalNode() != null)
        {
            System.out.printf(
                "localNode = %s\n",
                controllerPeerConnector.getLocalNode().getName().displayValue
            );
        }
        else
        {
            System.out.printf("localNode = not initialized\n");
        }
        for (Node curNode : nodesMap.values())
        {
            System.out.printf(
                "Node %s\n",
                curNode.getName().displayValue
            );
        }
        for (ResourceDefinition curRscDfn : rscDfnMap.values())
        {
            try
            {
                System.out.printf(
                    "    RscDfn %-24s Port %5d\n",
                    curRscDfn.getName(), curRscDfn.getPort(wrkCtx).value
                );
            }
            catch (AccessDeniedException ignored)
            {
            }
        }
        Node localNode = controllerPeerConnector.getLocalNode();
        if (localNode != null)
        {
            try
            {
                for (Resource curRsc : localNode.streamResources(wrkCtx).collect(Collectors.toList()))
                {
                    System.out.printf(
                        "Assigned resource %-24s:\n",
                        curRsc.getDefinition().getName().displayValue
                    );
                    Iterator<Volume> vlmIter = curRsc.iterateVolumes();
                    while (vlmIter.hasNext())
                    {
                        Volume curVlm = vlmIter.next();
                        VolumeDefinition curVlmDfn = curVlm.getVolumeDefinition();
                        try
                        {
                            System.out.printf(
                                "    Volume %5d Size %9d\n",
                                curVlm.getVolumeDefinition().getVolumeNumber().value,
                                curVlmDfn.getVolumeSize(wrkCtx)
                            );
                        }
                        catch (AccessDeniedException ignored)
                        {
                        }
                    }
                }

                localNode.streamStorPools(wrkCtx).forEach(curStorPool ->
                {
                    String driverClassName = curStorPool.getDriverName();
                    System.out.printf(
                        "Storage pool %-24s Driver %s\n",
                        curStorPool.getName().displayValue,
                        driverClassName
                    );
                }
                );
            }
            catch (AccessDeniedException ignored)
            {
            }
        }
        System.out.println("\u001b[1;31m== END DrbdDeviceHandler.debugListSatelliteObjects() ==\u001b[0m");
        System.out.println();
    }

    static class ResourceException extends LinStorException
    {
        ResourceException(String message)
        {
            super(message);
        }

        ResourceException(String message, Throwable cause)
        {
            super(message, cause);
        }

        ResourceException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, null);
        }

        ResourceException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText,
            Throwable cause
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, cause);
        }
    }

    static class VolumeException extends LinStorException
    {
        VolumeException(String message)
        {
            super(message);
        }

        VolumeException(String message, Throwable cause)
        {
            super(message, cause);
        }

        VolumeException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, null);
        }

        VolumeException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText,
            Throwable cause
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, cause);
        }
    }
}

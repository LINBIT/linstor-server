package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuard;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Shared logic of "make-available --auto-manage-dual-primary" and "unmake-available": determining the
 * live-migration source, computing effective DRBD net options and setting/reverting the dual-primary
 * properties (allow-two-primaries, protocol C) including the internal marker props on the rsc-dfn.
 */
@Singleton
public class CtrlRscLiveMigrateHelper
{
    private static final String KEY_ALLOW_TWO_PRIMARIES = "allow-two-primaries";
    private static final String KEY_PROTOCOL = "protocol";
    private static final String PROTOCOL_C = "C";
    private static final String VALUE_YES = "yes";

    private final SystemConfRepository systemConfRepository;
    private final CtrlRscConnectionHelper rscConnHelper;
    private final SharedResourceManager sharedRscMgr;

    @Inject
    public CtrlRscLiveMigrateHelper(
        SystemConfRepository systemConfRepositoryRef,
        CtrlRscConnectionHelper rscConnHelperRef,
        SharedResourceManager sharedRscMgrRef
    )
    {
        systemConfRepository = systemConfRepositoryRef;
        rscConnHelper = rscConnHelperRef;
        sharedRscMgr = sharedRscMgrRef;
    }

    /**
     * Whether the resource's data lives in a shared storage pool, including pools whose locks are
     * managed externally (e.g. lvmlockd) instead of by LINSTOR.
     */
    public boolean hasSharedStorPool(Resource rsc)
    {
        boolean shared = false;
        for (StorPool sp : LayerVlmUtils.getStorPools(rsc))
        {
            if (sp.isShared())
            {
                shared = true;
                break;
            }
        }
        return shared;
    }

    /**
     * Determines the live-migration source resource: the resource currently reported as in-use (primary)
     * by its satellite.
     *
     * @return the in-use resource the live migration to the given target node starts from. Returns null
     *     if there is no live migration to prepare, which is the case if the resource is not in use on
     *     any node at all or if the resource is already in use on the target node itself; the caller is
     *     expected to perform a plain make-available then. Especially not being in use anywhere is
     *     deliberately not an error, so that clients that cannot tell a live-migration attach from a
     *     plain attach (e.g. Proxmox) can always set auto_manage_dual_primary.
     *
     * @throws ApiRcException {@link ApiConsts#FAIL_EXISTS_LIVE_MIGRATE} if another migration (different
     *     source/target pair) is already prepared or the resource is in use on multiple unrelated nodes
     */
    public @Nullable Resource findMigrationSource(ResourceDefinition rscDfn, String tgtNodeNameRef)
    {
        List<Resource> inUseRscs = getInUseResources(rscDfn);
        @Nullable String markerSrc = getMarker(rscDfn, InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE);
        @Nullable String markerTgt = getMarker(rscDfn, InternalApiConsts.KEY_LIVE_MIGRATE_TARGET_NODE);

        @Nullable Resource srcRsc;
        if (inUseRscs.isEmpty())
        {
            // no in-use resource means no live migration is in progress, so there is nothing to
            // prepare and the call degrades to a plain make-available
            srcRsc = null;
        }
        else if (inUseRscs.size() == 1)
        {
            srcRsc = inUseRscs.get(0);
            String srcNodeName = srcRsc.getNode().getName().displayValue;
            if (markerSrc != null && markerTgt != null &&
                !(equalsNodeName(markerSrc, srcNodeName) && equalsNodeName(markerTgt, tgtNodeNameRef)))
            {
                throw failConflictingMigration(rscDfn, markerSrc, markerTgt);
            }
            if (equalsNodeName(srcNodeName, tgtNodeNameRef))
            {
                // resource is already in use on the target node, nothing to prepare
                srcRsc = null;
            }
        }
        else
        {
            /*
             * in use on multiple nodes: only allowed as idempotent re-apply while the prepared migration
             * (marker source/target pair) is running
             */
            srcRsc = null;
            boolean validReapply = markerSrc != null && markerTgt != null &&
                equalsNodeName(markerTgt, tgtNodeNameRef) && inUseRscs.size() == 2;
            if (validReapply)
            {
                for (Resource inUseRsc : inUseRscs)
                {
                    String inUseNodeName = inUseRsc.getNode().getName().displayValue;
                    if (equalsNodeName(inUseNodeName, markerSrc))
                    {
                        srcRsc = inUseRsc;
                    }
                    else if (!equalsNodeName(inUseNodeName, markerTgt))
                    {
                        validReapply = false;
                    }
                }
            }
            if (!validReapply || srcRsc == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_EXISTS_LIVE_MIGRATE,
                        "Resource '" + rscDfn.getName().displayValue + "' is in use on multiple nodes"
                    )
                        .setCause("The live-migration source cannot be determined.")
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
        return srcRsc;
    }

    /**
     * Verifies that the resource of the given migration source supports being active on two nodes of a
     * shared storage pool at once. The source's data must live in a shared storage pool: the caller
     * finds it via {@link SharedResourceManager}, i.e. as the active copy of the shared data the
     * migration target reuses. Note that none of the checks restricts the conflicting operation
     * itself - e.g. any number of concurrent clones of the source may run, they only delay opening
     * the dual-active window until they finish.
     *
     * @throws ApiRcException {@link ApiConsts#FAIL_INVLD_LAYER_STACK} if the source resource uses DRBD,
     *     {@link ApiConsts#FAIL_INVLD_PROVIDER} if an externally locked storage pool's provider cannot
     *     activate volumes with shared locks,
     *     {@link ApiConsts#FAIL_EXISTS_SNAPSHOT} if snapshots of the resource or its shared data exist,
     *     {@link ApiConsts#FAIL_IN_USE} if the resource is currently being cloned or one of its volumes
     *     is being resized
     */
    public void ensureSharedDualActiveSupported(Resource srcRsc)
    {
        if (!sharedRscMgr.isBackedBySharedStorPool(srcRsc))
        {
            throw new ImplementationError(
                "Dual-active checked for resource without shared storage pool: " + srcRsc
            );
        }
        ResourceDefinition rscDfn = srcRsc.getResourceDefinition();
        if (srcRsc.hasDrbd())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_LAYER_STACK,
                    "Activating resource '" + rscDfn.getName().displayValue +
                        "' on two nodes of a shared storage pool is not possible with DRBD",
                    true
                )
            );
        }
        for (StorPool sp : LayerVlmUtils.getStorPools(srcRsc))
        {
            if (sp.isExternalLocking() && !sp.getDeviceProviderKind().isSharedVolumeSupported())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_PROVIDER,
                        "Storage pool '" + sp.getName().displayValue + "' of provider kind '" +
                            sp.getDeviceProviderKind().name() + "' cannot activate volumes with a shared lock"
                    )
                        .setCause(
                            "Activating the resource on two nodes of an externally locked storage pool " +
                                "requires the backing volumes to support shared locks."
                        )
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns())
        {
            // the snapshot data lives once on the shared pool, so snapshots forbid the dual-active
            // window even if the migration source is missing the per-node snapshot objects
            if (snapDfn.getSnapshot(srcRsc.getNode().getName()) != null ||
                sharedRscMgr.findSnapshotOnSharedSp(snapDfn, srcRsc) != null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_EXISTS_SNAPSHOT,
                        "Resource '" + rscDfn.getName().displayValue + "' still has snapshot '" +
                            snapDfn.getName().displayValue + "'"
                    )
                        .setCause(
                            "Activating a shared resource on two nodes is not supported while " +
                                "snapshots of the resource or its shared data exist."
                        )
                        .setCorrection("Delete the snapshots first.")
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
        for (Map.Entry<String, String> cloneProp : srcRsc.getProps().map().entrySet())
        {
            if (cloneProp.getKey().startsWith(InternalApiConsts.CLONE_PROP_PREFIX))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        "Resource '" + rscDfn.getName().displayValue + "' is currently being cloned to '" +
                            cloneProp.getValue() + "'"
                    )
                        .setCause(
                            "Activating a shared resource on two nodes is not supported while a clone " +
                                "of the resource is in progress: the clone reads from a snapshot of the " +
                                "shared data."
                        )
                        .setCorrection("Wait for the clone to finish.")
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
        // the counterpart of VolumeDefinitionResizeCheckUtils#ensureSharedDataNotActiveOnMultipleNodes,
        // which refuses a resize while the dual-active window is already open
        Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn();
        while (vlmDfnIt.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIt.next();
            if (vlmDfn.getFlags().isSet(VolumeDefinition.Flags.RESIZE))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        "Volume definition " + vlmDfn.getVolumeNumber() + " of resource '" +
                            rscDfn.getName().displayValue + "' is currently being resized"
                    )
                        .setCause(
                            "Activating a shared resource on two nodes is not supported while one of " +
                                "its volumes is being resized."
                        )
                        .setCorrection("Wait for the resize to finish.")
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
    }

    /**
     * Sets allow-two-primaries (and protocol C if the effective protocol differs) for the given
     * source/target pair, plus the internal marker props on the rsc-dfn. The props are set on the
     * resource-connection between source and target, or on the rsc-dfn if both resources are DRBD
     * diskless, since no connection section is generated between two diskless resources.
     * Caller must commit and update the satellites of the rsc-dfn afterwards.
     */
    public ApiCallRcImpl setDualPrimaryProps(Resource srcRsc, Resource tgtRsc)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResourceDefinition rscDfn = srcRsc.getResourceDefinition();
        String rscNameStr = rscDfn.getName().displayValue;
        String srcNodeNameStr = srcRsc.getNode().getName().displayValue;
        String tgtNodeNameStr = tgtRsc.getNode().getName().displayValue;

        boolean bothDiskless = srcRsc.isDrbdDiskless() && tgtRsc.isDrbdDiskless();
        Props scopeProps;
        String scope;
        String scopeDescr;
        if (bothDiskless)
        {
            scopeProps = rscDfn.getProps();
            scope = InternalApiConsts.SET_ON_RSC_DFN;
            scopeDescr = "resource-definition '" + rscNameStr + "'";
        }
        else
        {
            ResourceConnection rscConn = rscConnHelper.loadOrCreateRscConn(
                null,
                srcNodeNameStr,
                tgtNodeNameStr,
                rscNameStr
            );
            scopeProps = rscConn.getProps();
            scope = InternalApiConsts.SET_ON_RSC_CONN;
            scopeDescr = "resource connection between nodes '" + srcNodeNameStr + "' and '" +
                tgtNodeNameStr + "' of resource '" + rscNameStr + "'";
        }

        Props rscDfnProps = rscDfn.getProps();
        // evaluate the effective protocol before changing anything
        String effectiveProtocol = getEffectiveProtocol(srcRsc, tgtRsc);
        if (!PROTOCOL_C.equalsIgnoreCase(effectiveProtocol))
        {
            @Nullable String prevExplicit = getProp(scopeProps, KEY_PROTOCOL, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
            setProp(scopeProps, KEY_PROTOCOL, PROTOCOL_C, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
            setProp(
                rscDfnProps,
                InternalApiConsts.KEY_LIVE_MIGRATE_PROTOCOL_SET_ON,
                scope,
                InternalApiConsts.NAMESPC_LIVE_MIGRATE
            );
            if (prevExplicit != null)
            {
                setProp(
                    rscDfnProps,
                    InternalApiConsts.KEY_LIVE_MIGRATE_PREV_PROTOCOL,
                    prevExplicit,
                    InternalApiConsts.NAMESPC_LIVE_MIGRATE
                );
            }
            responses.addEntry(
                "DRBD protocol temporarily changed from '" + effectiveProtocol + "' to 'C' on " + scopeDescr,
                ApiConsts.MASK_INFO
            );
        }

        setProp(scopeProps, KEY_ALLOW_TWO_PRIMARIES, VALUE_YES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
        setProp(
            rscDfnProps,
            InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE,
            srcNodeNameStr,
            InternalApiConsts.NAMESPC_LIVE_MIGRATE
        );
        setProp(
            rscDfnProps,
            InternalApiConsts.KEY_LIVE_MIGRATE_TARGET_NODE,
            tgtNodeNameStr,
            InternalApiConsts.NAMESPC_LIVE_MIGRATE
        );
        responses.addEntry(
            "Enabled allow-two-primaries on " + scopeDescr + " for the live migration from node '" +
                srcNodeNameStr + "' to node '" + tgtNodeNameStr + "'. Revert with unmake-available.",
            ApiConsts.MASK_INFO
        );
        return responses;
    }

    /**
     * Unconditionally removes allow-two-primaries (from the rsc-dfn, from all resource-connections of the
     * given resource and from the marker-recorded source/target connection), restores the protocol
     * recorded by make-available and removes the internal live-migrate marker props.
     * Caller must commit and update the satellites of the rsc-dfn afterwards.
     *
     * @return true if any property was changed, so the satellites need to be updated
     */
    public boolean cleanupDualPrimaryProps(ResourceDefinition rscDfn, @Nullable Resource rsc, ApiCallRcImpl responses)
    {
        boolean changed = false;
        Props rscDfnProps = rscDfn.getProps();
        String rscNameStr = rscDfn.getName().displayValue;

        @Nullable String markerSrc = getMarker(rscDfn, InternalApiConsts.KEY_LIVE_MIGRATE_SOURCE_NODE);
        @Nullable String markerTgt = getMarker(rscDfn, InternalApiConsts.KEY_LIVE_MIGRATE_TARGET_NODE);
        @Nullable ResourceConnection markerRscConn = null;
        if (markerSrc != null && markerTgt != null)
        {
            markerRscConn = rscConnHelper.loadRscConnOrNull(markerSrc, markerTgt, rscNameStr);
        }

        changed |= removeProp(rscDfnProps, KEY_ALLOW_TWO_PRIMARIES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
        if (rsc != null)
        {
            for (ResourceConnection rscConn : rsc.getAbsResourceConnections())
            {
                changed |= removeProp(rscConn.getProps(), KEY_ALLOW_TWO_PRIMARIES, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
            }
        }
        if (markerRscConn != null)
        {
            changed |= removeProp(
                markerRscConn.getProps(),
                KEY_ALLOW_TWO_PRIMARIES,
                ApiConsts.NAMESPC_DRBD_NET_OPTIONS
            );
        }

        @Nullable String protocolSetOn = getMarker(rscDfn, InternalApiConsts.KEY_LIVE_MIGRATE_PROTOCOL_SET_ON);
        @Nullable String prevProtocol = getMarker(rscDfn, InternalApiConsts.KEY_LIVE_MIGRATE_PREV_PROTOCOL);
        if (protocolSetOn != null)
        {
            @Nullable Props protocolProps = null;
            if (InternalApiConsts.SET_ON_RSC_DFN.equals(protocolSetOn))
            {
                protocolProps = rscDfnProps;
            }
            else if (markerRscConn != null)
            {
                // if the connection is already gone its props died with it, nothing to restore
                protocolProps = markerRscConn.getProps();
            }
            if (protocolProps != null)
            {
                if (prevProtocol != null)
                {
                    setProp(protocolProps, KEY_PROTOCOL, prevProtocol, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
                    responses.addEntry(
                        "DRBD protocol of resource '" + rscNameStr + "' restored to '" + prevProtocol + "'",
                        ApiConsts.MASK_INFO
                    );
                }
                else
                {
                    removeProp(protocolProps, KEY_PROTOCOL, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
                }
                changed = true;
            }
        }

        try
        {
            changed |= rscDfnProps.removeNamespace(InternalApiConsts.NAMESPC_LIVE_MIGRATE);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return changed;
    }

    /**
     * The effective DRBD protocol between the two given resources, mirroring the priority order of the
     * satellite's ConfFileBuilder: rsc-conn, rsc-dfn, node-conn, rsc-grp, controller.
     */
    public String getEffectiveProtocol(Resource srcRsc, Resource tgtRsc)
    {
        PriorityProps prioProps = new PriorityProps();
        @Nullable ResourceConnection rscConn = srcRsc.getAbsResourceConnection(tgtRsc);
        if (rscConn != null)
        {
            prioProps.addProps(rscConn.getProps());
        }
        ResourceDefinition rscDfn = srcRsc.getResourceDefinition();
        prioProps.addProps(rscDfn.getProps());
        @Nullable NodeConnection nodeConn = srcRsc.getNode().getNodeConnection(tgtRsc.getNode());
        if (nodeConn != null)
        {
            prioProps.addProps(nodeConn.getProps());
        }
        prioProps.addProps(
            rscDfn.getResourceGroup().getProps(),
            systemConfRepository.getStltConfForView(),
            systemConfRepository.getCtrlConfForView()
        );
        @Nullable String protocol;
        try
        {
            protocol = prioProps.getProp(KEY_PROTOCOL, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return protocol == null ? PROTOCOL_C : protocol;
    }

    public @Nullable String getMarker(ResourceDefinition rscDfn, String key)
    {
        return getProp(rscDfn.getProps(), key, InternalApiConsts.NAMESPC_LIVE_MIGRATE);
    }

    private List<Resource> getInUseResources(ResourceDefinition rscDfn)
    {
        List<Resource> inUseRscs = new ArrayList<>();
        Iterator<Resource> rscIt = rscDfn.iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (isInUse(rsc))
            {
                inUseRscs.add(rsc);
            }
        }
        return inUseRscs;
    }

    /**
     * Whether the resource is Primary on its node. Deliberately not {@link SatelliteResourceState#isInUseOrOpen()}:
     * a live migration is defined by which nodes are Primary, a device that is merely opened (e.g. read-only)
     * on a Secondary is neither a migration source nor a target.
     */
    private boolean isInUse(Resource rsc)
    {
        @Nullable Boolean inUse = null;
        Node node = rsc.getNode();
        @Nullable Peer peer = node.getPeer();
        if (peer != null)
        {
            try (LockGuard ignored = LockGuard.createLocked(peer.getSatelliteStateLock().readLock()))
            {
                inUse = peer.getSatelliteState().getFromResource(
                    rsc.getResourceDefinition().getName(),
                    SatelliteResourceState::isInUse
                );
            }
        }
        return inUse != null && inUse;
    }

    private ApiRcException failConflictingMigration(
        ResourceDefinition rscDfn,
        String markerSrc,
        String markerTgt
    )
    {
        return new ApiRcException(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.FAIL_EXISTS_LIVE_MIGRATE,
                "A live migration of resource '" + rscDfn.getName().displayValue + "' from node '" +
                    markerSrc + "' to node '" + markerTgt + "' is already prepared"
            )
                .setCorrection(
                    "Finish that migration and revert it with 'unmake-available " + markerSrc + " " +
                        rscDfn.getName().displayValue + "' first."
                )
                .setSkipErrorReport(true)
                .build()
        );
    }

    private static boolean equalsNodeName(String nodeNameA, String nodeNameB)
    {
        return nodeNameA.equalsIgnoreCase(nodeNameB);
    }

    private static @Nullable String getProp(Props props, String key, String namespace)
    {
        try
        {
            return props.getProp(key, namespace);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private static void setProp(Props props, String key, String value, String namespace)
    {
        try
        {
            props.setProp(key, value, namespace);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private static boolean removeProp(Props props, String key, String namespace)
    {
        try
        {
            return props.removeProp(key, namespace) != null;
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }
}

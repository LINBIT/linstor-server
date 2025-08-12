package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@Singleton
public class ZfsChecks
{
    private final AccessContext apiCtx;
    private final CtrlPropsHelper ctrlPropsHelper;

    @Inject
    public ZfsChecks(
        @SystemContext AccessContext apiCtxRef,
        CtrlPropsHelper ctrlPropsHelperRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
    }

    /**
     * Determines whether to use the old rollback strategy (i.e. "zfs rollback") or the new (rollback via restore with
     * safetySnap). <code>false</code> return value represents old rollback strategy, <code>true</code> means new
     * strategy.
     *
     * <p>Return scenarios:</p>
     * <ul>
     * <li><code>false</code> if snapshot or resources are at least partially non-ZFS </li>
     * <li>If all snapshots + resources are ZFS and <code>useZfsRollbackRef</code> is non-null, returns
     *  <code>useZfsRollbackRef</code></li>
     * <li>If <code>useZfsRollbackRef</code> is <code>null</code>, check the controller property
     * {@value ZfsRollbackStrategy#FULL_KEY_USE_ZFS_ROLLBACK_PROP} for the strategy.
     *  <ul>
     *   <li>{@value ApiConsts#VAL_STOR_POOL_ZFS_ROLLBACK_STRAT_CLONE} forces new behavior
     *      (i.e. <code>true</code> return)</li>
     *   <li>{@value ApiConsts#VAL_STOR_POOL_ZFS_ROLLBACK_STRAT_ROLLBACK} forces old behavior
     *      (i.e. <code>false</code> return)</li>
     *   <li>{@value ApiConsts#VAL_STOR_POOL_ZFS_ROLLBACK_STRAT_DYNAMIC} returns whether old behavior
     *      is applicable or not</li>
     *  </ul>
     * </li>
     * </ul>
     */
    public boolean useOldRollback(
        SnapshotDefinition snapDfnRef,
        @Nullable String zfsRollbackStrategyFromClientRef
    )
    {
        boolean isZfsSnapshot = isZfsSnapshot(snapDfnRef);

        boolean ret;
        if (!isZfsSnapshot)
        {
            ret = false;
        }
        else
        {
            ResourceDefinition rscDfn = snapDfnRef.getResourceDefinition();

            try
            {
                ZfsRollbackStrategy zfsRollbackStrategy = ZfsRollbackStrategy.getStrat(
                    zfsRollbackStrategyFromClientRef,
                    rscDfn,
                    ctrlPropsHelper.getCtrlPropsForView(),
                    apiCtx
                );

                switch (zfsRollbackStrategy)
                {
                    case CLONE:
                        ret = false; // noop, clone always works
                        break;
                    case ROLLBACK:
                        if (!isMostRecentSnapshot(snapDfnRef))
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_DEPENDEND_BACKUP,
                                    "'" + snapDfnRef.getName() +
                                        "' is not the most recent snapshot. Cannot 'zfs rollback ...' " +
                                        "to the given snapshot!"
                                ).setSkipErrorReport(true)
                            );
                        }
                        if (isZvolAlreadyDeleted(snapDfnRef))
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_DEPENDEND_BACKUP,
                                    "'" + rscDfn.getName() +
                                        "' was already rolled back using 'Clone' strategy. The current version of " +
                                        "LINSTOR cannot switch to 'Rollback' strategy in this case!"
                                ).setSkipErrorReport(true)
                            );
                        }
                        ret = true;
                        break;
                    case DYNAMIC:
                        ret = isMostRecentSnapshot(snapDfnRef) && !isZvolAlreadyDeleted(snapDfnRef);
                        break;
                    default:
                        throw new ImplementationError(
                            "'" + zfsRollbackStrategy + "' is not a valid value for '" +
                                ZfsRollbackStrategy.FULL_KEY_USE_ZFS_ROLLBACK_PROP + "'"
                        );
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError(accDeniedExc);
            }
        }
        return ret;
    }

    private boolean isZvolAlreadyDeleted(SnapshotDefinition snapDfnRef)
    {
        try
        {
            boolean hasRenameSuffix = false;
            for (Snapshot snapshot : snapDfnRef.getAllSnapshots(apiCtx))
            {
                if (snapshot.getSnapProps(apiCtx)
                    .getProp(CtrlRscDeleteApiCallHandler.PROP_KEY_ZFS_RENAME_SUFFIX) != null)
                {
                    hasRenameSuffix = true;
                    break;
                }
            }
            return hasRenameSuffix;
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public boolean isZfsSnapshot(SnapshotDefinition snapDfnRef) throws ImplementationError
    {
        boolean allSnapsZfs = true;
        try
        {
            for (Snapshot snap : snapDfnRef.getAllNotDeletingSnapshots(apiCtx))
            {
                if (!isZfs(snap))
                {
                    allSnapsZfs = false;
                    break;
                }
            }
            if (allSnapsZfs)
            {
                for (Resource rsc : snapDfnRef.getResourceDefinition().getNotDeletedDiskful(apiCtx))
                {
                    if (!isZfs(rsc))
                    {
                        allSnapsZfs = false;
                        break;
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return allSnapsZfs;
    }

    public boolean hasZfs(ResourceDefinition rscDfnRef) throws AccessDeniedException
    {
        return hasZfs(rscDfnRef, apiCtx);
    }

    public static boolean hasZfs(ResourceDefinition rscDfnRef, AccessContext accCtxRef) throws AccessDeniedException
    {
        boolean ret = false;
        for (Resource rsc : rscDfnRef.getDiskfulResources(accCtxRef))
        {
            if (isZfs(rsc, accCtxRef))
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    public <RSC extends AbsResource<RSC>> boolean isZfs(RSC absRscRef) throws AccessDeniedException
    {
        return isZfs(absRscRef, apiCtx);
    }

    public static <RSC extends AbsResource<RSC>> boolean isZfs(RSC absRscRef, AccessContext accCtx)
        throws AccessDeniedException
    {
        boolean ret = true;
        for (StorPool sp : LayerVlmUtils.getStorPools(absRscRef, accCtx, true))
        {
            DeviceProviderKind kind = sp.getDeviceProviderKind();
            if (kind != DeviceProviderKind.ZFS && kind != DeviceProviderKind.ZFS_THIN)
            {
                ret = false;
                break;
            }
        }
        return ret;
    }

    private boolean isMostRecentSnapshot(SnapshotDefinition snapDfnRef)
    {
        long maxSequenceNumber = getMaxSequenceNumber(snapDfnRef.getResourceDefinition());
        long sequenceNumber = getSequenceNumber(snapDfnRef);
        return sequenceNumber == maxSequenceNumber;
    }

    private long getMaxSequenceNumber(ResourceDefinition rscDfn)
    {
        long maxSequenceNumber;
        try
        {
            maxSequenceNumber = SnapshotDefinitionControllerFactory.maxSequenceNumber(apiCtx, rscDfn);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check sequence numbers of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        return maxSequenceNumber;
    }

    private long getSequenceNumber(SnapshotDefinition snapshotDfn)
    {
        long sequenceNumber;
        try
        {
            sequenceNumber = Long.parseLong(
                ctrlPropsHelper.getProps(snapshotDfn, false).getProp(ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER)
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Internal property not valid", exc);
        }
        catch (NumberFormatException exc)
        {
            throw new ImplementationError(
                "Unable to parse internal value of internal property " +
                    ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER,
                exc
            );
        }
        return sequenceNumber;
    }

    /**
     *  "zfs rollback" is only supported with the most recent snapshot since ZFS would force us to delete the
     *   intermediate snapshots which we simply leave for the user to do if they really want this behavior
     */
    public void ensureMostRecentSnapshot(SnapshotDefinition snapDfnRef)
    {
        if (!isMostRecentSnapshot(snapDfnRef))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                    "Rollback is only allowed with the most recent snapshot"
                ).setSkipErrorReport(true)
            );
        }
    }

    public static ZfsDeleteStrategy getDeleteStrategy(
        VolumeDefinition vlmDfnRef,
        ReadOnlyProps ctrlPropsRef,
        AccessContext accCtxRef
    )
        throws InvalidKeyException, AccessDeniedException
    {
        return getDeleteStrategy(vlmDfnRef.getResourceDefinition(), ctrlPropsRef, accCtxRef);
    }

    public ZfsDeleteStrategy getDeleteStrategy(ResourceDefinition rscDfnRef)
    {
        try
        {
            return getDeleteStrategy(rscDfnRef, ctrlPropsHelper.getCtrlPropsForView(apiCtx), apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public static ZfsDeleteStrategy getDeleteStrategy(
        ResourceDefinition rscDfnRef,
        ReadOnlyProps ctrlPropsRef,
        AccessContext accCtxRef
    )
        throws AccessDeniedException
    {
        ZfsDeleteStrategy strat = ZfsDeleteStrategy.getStrat(rscDfnRef, ctrlPropsRef, accCtxRef);
        switch (strat)
        {
            case DELETE:
                if (!rscDfnRef.getSnapshotDfns(accCtxRef).isEmpty())
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_EXISTS_SNAPSHOT,
                            "Cannot use " + strat + " strategy since the resource definition '" + rscDfnRef.getName() +
                                "' still has snapshots"
                        ).setSkipErrorReport(true)
                    );
                }
                break;
            case DYNAMIC:
                strat = rscDfnRef.getSnapshotDfns(accCtxRef).isEmpty() ?
                    ZfsDeleteStrategy.DELETE :
                    ZfsDeleteStrategy.RENAME;
                break;
            case RENAME:
                // noop, delete via rename is always possible
                break;
            default:
                throw new ImplementationError("Strategy '" + strat + "' not implemented");
        }
        return strat;
    }

    public void ensureNoDependentSnapshots(Resource rscRef)
    {
        try
        {
            if (getDeleteStrategy(rscRef.getResourceDefinition()) == ZfsDeleteStrategy.DELETE)
            {
                ensureNoDependentSnapshots(rscRef, apiCtx, true);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public static ApiCallRc ensureNoDependentSnapshots(Resource rscRef, AccessContext accCtx, boolean throwApiExc)
        throws AccessDeniedException
    {
        ApiCallRcImpl ret = new ApiCallRcImpl();
        if (isZfs(rscRef, accCtx))
        {
            for (SnapshotDefinition snapshotDfn : rscRef.getResourceDefinition().getSnapshotDfns(accCtx))
            {
                @Nullable Snapshot snapshot = snapshotDfn.getSnapshot(accCtx, rscRef.getNode().getName());
                if (snapshot != null)
                {
                    Iterator<SnapshotVolume> snapVlmIt = snapshot.iterateVolumes();
                    while (snapVlmIt.hasNext())
                    {
                        SnapshotVolume snapshotVlm = snapVlmIt.next();
                        Set<StorPool> storPoolSet = LayerVlmUtils.getStorPoolSet(snapshotVlm, accCtx, true);
                        for (StorPool storPool : storPoolSet)
                        {
                            if (storPool.getDeviceProviderKind().isSnapshotDependent())
                            {
                                ApiCallRcEntry simpleEntry = ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                                    "Resource '" + rscRef.getResourceDefinition().getName() +
                                        "' cannot be deleted because volume " +
                                        snapshotVlm.getVolumeNumber() + " has dependent snapshot '" +
                                        snapshot.getSnapshotName() + "'"
                                );
                                if (throwApiExc)
                                {
                                    throw new ApiRcException(simpleEntry);
                                }
                                else
                                {
                                    ret.add(simpleEntry);
                                }
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }

    public void ensureNoDependentSnapshots(VolumeDefinition vlmDfnRef)
    {
        try
        {
            if (getDeleteStrategy(vlmDfnRef.getResourceDefinition()) == ZfsDeleteStrategy.DELETE)
            {
                ensureNoDependentSnapshots(vlmDfnRef, apiCtx, true);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public static ApiCallRc ensureNoDependentSnapshots(
        VolumeDefinition vlmDfnRef,
        AccessContext accCtxRef,
        boolean throwApiExcRef
    )
        throws AccessDeniedException
    {
        ApiCallRc ret = new ApiCallRcImpl();
        if (hasZfs(vlmDfnRef.getResourceDefinition(), accCtxRef))
        {
            ResourceDefinition rscDfn = vlmDfnRef.getResourceDefinition();
            for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(accCtxRef))
            {
                for (Snapshot snapshot : snapshotDfn.getAllSnapshots(accCtxRef))
                {
                    SnapshotVolume snapshotVlm = snapshot.getVolume(vlmDfnRef.getVolumeNumber());
                    if (snapshotVlm != null)
                    {
                        Map<String, StorPool> storPoolMap = LayerVlmUtils.getStorPoolMap(snapshotVlm, accCtxRef);
                        for (StorPool storPool : storPoolMap.values())
                        {
                            if (storPool.getDeviceProviderKind().isSnapshotDependent())
                            {
                                ApiCallRcEntry simpleEntry = ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                                    "Volume definition " + vlmDfnRef.getVolumeNumber() + " of '" + rscDfn.getName() +
                                        "' cannot be deleted because dependent snapshot '" +
                                        snapshot.getSnapshotName() +
                                        "' is present on node '" + snapshot.getNodeName() + "'"
                                );
                                if (throwApiExcRef)
                                {
                                    throw new ApiRcException(simpleEntry);
                                }
                                else
                                {
                                    ret.add(simpleEntry);
                                }
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }
}

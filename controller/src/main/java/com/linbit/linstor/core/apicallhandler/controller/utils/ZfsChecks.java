package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
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
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Singleton;

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

    public <RSC extends AbsResource<RSC>> boolean isZfs(RSC absRscRef) throws AccessDeniedException
    {
        boolean ret = true;
        for (StorPool sp : LayerVlmUtils.getStorPools(absRscRef, apiCtx, true))
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
}

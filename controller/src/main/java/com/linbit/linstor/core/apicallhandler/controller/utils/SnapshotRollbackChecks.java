package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class SnapshotRollbackChecks
{
    private final ZfsChecks zfsChecks;

    @Inject
    public SnapshotRollbackChecks(
        ZfsChecks zfsChecksRef
    )
    {
        zfsChecks = zfsChecksRef;
    }

    /**
     * Determines whether to use the old rollback strategy (i.e. "zfs rollback") or the new (rollback via restore with
     * safetySnap). <code>true</code> return value represents the old rollback strategy, <code>false</code> means new
     * "rollback via restore with safetySnap" strategy.
     * <p>
     * Return scenarios by priority (first rule that applies wins):
     * </p>
     * <ul>
     * <li>If the SnapDfn is fully ZFS, delegate to
     * {@link ZfsChecks#useOldRollback(SnapshotDefinition, String)}.</li>
     * <li>If the SnapDfn is at least partially EBS based: <code>true</code> (old strategy). "rollback via restore" must
     * not be used with EBS resources since an EBS snapshot must reach "completed" state before it can be used,
     * including the safety-snapshot that is automatically created by LINSTOR during "rollback via restore". Reaching
     * the "completed" state might take a long time since it needs to be copied AWS internally.</li>
     * <li>Otherwise: <code>false</code></li>
     * </ul>
     */
    public boolean useOldRollback(
        SnapshotDefinition snapDfnRef,
        @Nullable String zfsRollbackStrategyFromClientRef
    )
    {
        boolean isZfsSnapshot = zfsChecks.isZfsSnapshot(snapDfnRef);

        boolean ret;
        if (isZfsSnapshot)
        {
            ret = zfsChecks.useOldRollback(snapDfnRef, zfsRollbackStrategyFromClientRef);
        }
        else if (EbsUtils.isAnySnapshotEbs(snapDfnRef))
        {
            /*
             * With EBS we must not use "rollback via restore" since that first creates a safety-snapshot which
             * might be used to revert back if something goes wrong. Good in theory but bad in combination with EBS
             * since an EBS snapshot is created fast but immediately enters into a "pending" state. Such a snapshot
             * cannot be used to revert back to until it reaches completed state.
             * That means that we would need to wait - at least in the bad-path - for the safetysnap to reach this
             * "completed" state, which might take minutes or longer, depending on the volume/snapshot size.
             */
            ret = true;
        }
        else
        {
            ret = false;
        }
        return ret;
    }
}

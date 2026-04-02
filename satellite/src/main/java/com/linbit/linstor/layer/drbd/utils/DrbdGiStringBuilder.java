package com.linbit.linstor.layer.drbd.utils;

import com.linbit.linstor.annotation.Nullable;

public class DrbdGiStringBuilder
{
    private boolean upToDate = false;
    private boolean consistent = false;
    private boolean peerDiskWasOutdatedOrInconsistent = false;
    private @Nullable String currentUuid = null;
    private @Nullable String bitmapBaseDataUuid = null;
    private @Nullable String youngerHistoryUuid = null;

    public String build()
    {
        StringBuilder sb = new StringBuilder();
        append(sb, currentUuid);
        append(sb, bitmapBaseDataUuid);
        append(sb, youngerHistoryUuid);
        // appending uuids added the ":" as suffix. this means that right now we have "A:B:C:".
        // we skip calling append with null for the "older history" and continue with adding booleans.
        // adding booleans will continue with ":" as prefix, so if the first boolean is true, we continue
        // with "A:B:C::1"
        append(sb, consistent || upToDate); // Data consistency flag
        append(sb, upToDate); // Data was/is currently up-to-date
        if (peerDiskWasOutdatedOrInconsistent)
        {
            /*
             * consolidating the following bits in order:
             *
             * * Node was/is currently primary
             * * This node was a crashed primary, and has not seen its peer since
             * * The activity-log was applied, the disk can be attached
             * * The activity-log was disabled, peer is completely out of sync
             * * This node was primary when it lost quorum
             * * Node was/is currently connected
             */
            sb.append("::::::");
            append(sb, peerDiskWasOutdatedOrInconsistent); // The peer's disk was out-dated or inconsistent
        }
        /*
         * skipping following trailing bits in order:
         * * A fence policy other the dont-care was used
         * * Node was in the progress of marking all blocks as out of sync
         * * At least once we saw this node with a backing device attached
         */
        return sb.toString();
    }

    private void append(StringBuilder sbRef, @Nullable String uuidRef)
    {
        if (uuidRef != null)
        {
            sbRef.append(uuidRef);
        }
        sbRef.append(":");
    }

    private void append(StringBuilder sbRef, boolean enabledRef)
    {
        sbRef.append(":");
        if (enabledRef)
        {
            sbRef.append("1");
        }
    }

    public DrbdGiStringBuilder withCurrentUUID(String currentUuidRef)
    {
        currentUuid = currentUuidRef;
        return this;
    }

    public DrbdGiStringBuilder withBitmapBaseDataUUID(String bitmapBaseDataUuidRef)
    {
        bitmapBaseDataUuid = bitmapBaseDataUuidRef;
        return this;
    }

    public DrbdGiStringBuilder withYoungerUUID(String youngerHistoryUuidRef)
    {
        youngerHistoryUuid = youngerHistoryUuidRef;
        return this;
    }
    public DrbdGiStringBuilder withUpToDate(boolean upToDateRef)
    {
        upToDate = upToDateRef;
        return this;
    }

    public DrbdGiStringBuilder withConsistent(boolean consistentRef)
    {
        consistent = consistentRef;
        return this;
    }

    public DrbdGiStringBuilder withPeerDiskWasOutdateOrInconsistent(boolean peerOutdatedRef)
    {
        peerDiskWasOutdatedOrInconsistent = peerOutdatedRef;
        return this;
    }

    public @Nullable String getCurrentUuid()
    {
        return currentUuid;
    }

    public boolean isUpToDate()
    {
        return upToDate;
    }
}

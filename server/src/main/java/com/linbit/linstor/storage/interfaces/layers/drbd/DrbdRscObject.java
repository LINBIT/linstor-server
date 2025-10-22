package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface DrbdRscObject<RSC extends AbsResource<RSC>>
    extends AbsRscLayerObject<RSC>
{
    NodeId getNodeId();

    /** Nullable for Snapshots, not for Resources */
    @Nullable
    Collection<TcpPortNumber> getTcpPortList();

    void setNodeId(NodeId nodeIdRef) throws DatabaseException;

    boolean isDiskless(AccessContext accCtx) throws AccessDeniedException;

    boolean isDisklessForPeers(AccessContext accCtx) throws AccessDeniedException;

    short getPeerSlots();

    int getAlStripes();

    long getAlStripeSize();

    @SuppressWarnings("checkstyle:magicnumber")
    enum DrbdRscFlags implements Flags
    {
        CLEAN(1L << 0),
        DELETE(1L << 1),
        DISKLESS(1L << 2),
        DISK_ADD_REQUESTED(1L << 3),
        DISK_ADDING(1L << 4),
        DISK_REMOVE_REQUESTED(1L << 5),
        DISK_REMOVING(1L << 6),
        INITIALIZED(1L << 7),
        FROM_BACKUP(1L << 8),
        FORCE_NEW_METADATA(1L << 9),
        INVALIDATE(1L << 10);

        public final long flagValue;

        DrbdRscFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static DrbdRscFlags[] restoreFlags(long rscFlags)
        {
            List<DrbdRscFlags> flagList = new ArrayList<>();
            for (DrbdRscFlags flag : DrbdRscFlags.values())
            {
                if ((rscFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new DrbdRscFlags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(DrbdRscFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(DrbdRscFlags.class, listFlags);
        }
    }
}

package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Resource extends TransactionObject, DbgInstanceUuid
{
    UUID getUuid();

    ObjectProtection getObjProt();

    ResourceDefinition getDefinition();

    Volume getVolume(VolumeNumber volNr);

    Iterator<Volume> iterateVolumes();

    Stream<Volume> streamVolumes();

    Node getAssignedNode();

    NodeId getNodeId();

    Stream<ResourceConnection> streamResourceConnections(AccessContext accCtx)
        throws AccessDeniedException;

    ResourceConnection getResourceConnection(AccessContext accCtx, Resource otherResource)
        throws AccessDeniedException;

    void setResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException;

    void removeResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    StateFlags<RscFlags> getStateFlags();

    void adjustVolumes(AccessContext apiCtx, String defaultStorPoolName)
        throws InvalidNameException, LinStorException;

    void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    RscApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    boolean isCreatePrimary();

    enum RscFlags implements Flags
    {
        CLEAN(1L),
        DELETE(2L),
        DISKLESS(4L);

        public final long flagValue;

        RscFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static RscFlags[] restoreFlags(long rscFlags)
        {
            List<RscFlags> flagList = new ArrayList<>();
            for (RscFlags flag : RscFlags.values())
            {
                if ((rscFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new RscFlags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(RscFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(RscFlags.class, listFlags);
        }
    }

    public interface RscApi
    {
        UUID getUuid();
        String getName();
        UUID getNodeUuid();
        String getNodeName();
        UUID getRscDfnUuid();
        Map<String, String> getProps();
        long getFlags();
        List<? extends Volume.VlmApi> getVlmList();
    }
}

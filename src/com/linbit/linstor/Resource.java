package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Resource extends TransactionObject, DbgInstanceUuid
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public ResourceDefinition getDefinition();

    public Volume getVolume(VolumeNumber volNr);

    public Iterator<Volume> iterateVolumes();

    public Node getAssignedNode();

    public NodeId getNodeId();

    public ResourceConnection getResourceConnection(AccessContext accCtx, Resource otherResource)
        throws AccessDeniedException;

    public void setResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException;

    public void removeResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public StateFlags<RscFlags> getStateFlags();

    public void adjustVolumes(
        AccessContext apiCtx,
        TransactionMgr transMgr,
        String defaultStorPoolName
    )
        throws InvalidNameException, LinStorException;

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public RscApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    public enum RscFlags implements Flags
    {
        CLEAN(1L),
        DELETE(2L),
        DISKLESS(4L);

        public final long flagValue;

        private RscFlags(long value)
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

    public interface RscApi {
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

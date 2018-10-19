package com.linbit.linstor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObject;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ResourceConnection extends DbgInstanceUuid, TransactionObject
{
    UUID getUuid();

    Resource getSourceResource(AccessContext accCtx) throws AccessDeniedException;

    Resource getTargetResource(AccessContext accCtx) throws AccessDeniedException;

    Props getProps(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;

    RscConnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    StateFlags<RscConnFlags> getStateFlags();

    TcpPortNumber getPort(AccessContext accCtx) throws AccessDeniedException;

    TcpPortNumber setPort(AccessContext accCtx, TcpPortNumber port)
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ValueInUseException;

    void autoAllocatePort(AccessContext accCtx)
        throws AccessDeniedException, SQLException, ExhaustedPoolException;

    @SuppressWarnings("checkstyle:magicnumber")
    enum RscConnFlags implements Flags
    {
        DELETED(1L << 0),
        LOCAL_DRBD_PROXY(1L << 1);

        public final long flagValue;

        RscConnFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static RscConnFlags[] restoreFlags(long rscFlags)
        {
            List<RscConnFlags> flagList = new ArrayList<>();
            for (RscConnFlags flag : RscConnFlags.values())
            {
                if ((rscFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new RscConnFlags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(RscConnFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(RscConnFlags.class, listFlags);
        }
    }

    interface RscConnApi
    {
        UUID getUuid();
        String getSourceNodeName();
        String getTargetNodeName();
        String getResourceName();
        Map<String, String> getProps();
        long getFlags();
        Integer getPort();
    }
}

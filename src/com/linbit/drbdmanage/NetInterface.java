package com.linbit.drbdmanage;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Network interface of a node
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface NetInterface extends TransactionObject
{
    public UUID getUuid();

    public NetInterfaceName getName();

    public Node getNode();

    public DmIpAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException;

    public void setAddress(AccessContext accCtx, DmIpAddress newAddress)
        throws AccessDeniedException, SQLException;

    public NetInterfaceType getNetInterfaceType(AccessContext accCtx)
        throws AccessDeniedException;

    public void setNetInterfaceType(AccessContext accCtx, NetInterfaceType type)
        throws AccessDeniedException, SQLException;

    public int getNetInterfacePort(AccessContext accCtx)
        throws AccessDeniedException;

    public void setNetInterfacePort(AccessContext accCtx, int port)
        throws AccessDeniedException, SQLException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public static enum NetInterfaceType
    {
        IP, RDMA, RoCE;

        public static NetInterfaceType byValue(String str)
        {
            NetInterfaceType type = null;
            switch (str.toUpperCase())
            {
                case "IP":
                    type = IP;
                    break;
                case "RDMA":
                    type = RDMA;
                    break;
                case "ROCE":
                    type = RoCE;
                    break;
            }
            return type;
        }

        public static NetInterfaceType valueOfIgnoreCase(String string, NetInterfaceType defaultValue)
            throws IllegalArgumentException
        {
            NetInterfaceType ret = defaultValue;
            if (string != null)
            {
                NetInterfaceType val = valueOf(string.toUpperCase());
                if (val != null)
                {
                    ret = val;
                }
            }
            return ret;
        }
    }
}

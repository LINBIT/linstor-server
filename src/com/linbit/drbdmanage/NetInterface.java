package com.linbit.drbdmanage;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.net.InetAddress;
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

    public ObjectProtection getObjProt();

    public NetInterfaceName getName();

    public Node getNode();

    public InetAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException;

    public void setAddress(AccessContext accCtx, InetAddress newAddress)
        throws AccessDeniedException, SQLException;

    public NetInterfaceType getNetInterfaceType(AccessContext accCtx)
        throws AccessDeniedException;

    public void setNetInterfaceType(AccessContext accCtx, NetInterfaceType type)
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
                    type = IP;
                    break;
                case "ROCE":
                    type = IP;
                    break;
            }
            return type;
        }
    }
}

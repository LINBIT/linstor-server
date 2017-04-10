package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.net.InetAddress;
import java.util.UUID;

/**
 * Implementation of a network interface
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NetInterfaceData implements NetInterface
{
    private NetInterfaceName niName;
    private InetAddress niAddress;
    private ObjectProtection objProt;
    private UUID niUuid;

    NetInterfaceData(AccessContext accCtx, NetInterfaceName name, InetAddress addr)
    {
        objProt = new ObjectProtection(accCtx);
        niName = name;
        niAddress = addr;
        niUuid = UUID.randomUUID();
    }

    @Override
    public UUID getUuid()
    {
        return niUuid;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public NetInterfaceName getName()
    {
        return niName;
    }

    @Override
    public InetAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return niAddress;
    }

    @Override
    public void setAddress(AccessContext accCtx, InetAddress newAddress)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        niAddress = newAddress;
    }
}

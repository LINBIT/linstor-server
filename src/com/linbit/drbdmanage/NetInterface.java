package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.net.InetAddress;
import java.util.UUID;

/**
 * Network interface of a node
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface NetInterface
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public NetInterfaceName getName();

    public InetAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException;

    public void setAddress(AccessContext accCtx, InetAddress newAddress)
        throws AccessDeniedException;
}

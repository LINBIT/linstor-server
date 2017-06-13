package com.linbit.drbdmanage;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionSimpleObject;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Implementation of a network interface
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NetInterfaceData implements NetInterface
{
    private Node niNode;
    private NetInterfaceName niName;
    private TransactionSimpleObject<InetAddress> niAddress;
    private ObjectProtection objProt;
    private UUID niUuid;

    NetInterfaceData(AccessContext accCtx, Node node, NetInterfaceName name, InetAddress addr) throws SQLException
    {
        this(accCtx, node, name, addr, null);
    }

    NetInterfaceData(AccessContext accCtx, Node node, NetInterfaceName name, InetAddress addr, TransactionMgr transMgr) throws SQLException
    {
        niNode = node;
        niName = name;
        niAddress = new TransactionSimpleObject<InetAddress>(
            addr,
            node.getNetInterfaceDriver(name)
        );
        niUuid = UUID.randomUUID();
        objProt = ObjectProtection.create(
            ObjectProtection.buildPath(this),
            accCtx,
            transMgr
        );
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
    public Node getNode()
    {
        return niNode;
    }

    @Override
    public InetAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return niAddress.get();
    }

    @Override
    public void setAddress(AccessContext accCtx, InetAddress newAddress)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        niAddress.set(newAddress);
    }

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        transMgr.register(this);
        niAddress.setConnection(transMgr);
    }

    @Override
    public void commit()
    {
        niAddress.commit();
    }

    @Override
    public void rollback()
    {
        niAddress.rollback();
    }

    @Override
    public boolean isDirty()
    {
        return niAddress.isDirty();
    }
}

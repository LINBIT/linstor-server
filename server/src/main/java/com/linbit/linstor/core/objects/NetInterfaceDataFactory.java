package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.UUID;

public class NetInterfaceDataFactory
{
    private final NetInterfaceDataDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public NetInterfaceDataFactory(
        NetInterfaceDataDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public NetInterfaceData create(
        AccessContext accCtx,
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        TcpPortNumber port,
        EncryptionType encrType
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        NetInterfaceData netData = null;

        netData = (NetInterfaceData) node.getNetInterface(accCtx, netName);

        if (netData != null)
        {
            throw new LinStorDataAlreadyExistsException("The NetInterface already exists");
        }

        netData = new NetInterfaceData(
            UUID.randomUUID(),
            netName,
            node,
            addr,
            port,
            encrType,
            driver,
            transObjFactory,
            transMgrProvider
        );
        driver.create(netData);
        ((NodeData) node).addNetInterface(accCtx, netData);

        return netData;
    }

    public NetInterfaceData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr
    )
        throws ImplementationError
    {

        NetInterfaceData netData;
        try
        {
            netData = (NetInterfaceData) node.getNetInterface(accCtx, netName);
            if (netData == null)
            {
                /*
                 * stlt conn port and encr type are default null as one satellite
                 * should not communicate with any other satellite directly, thus
                 * the port and encr type are not needed
                 */

                netData = new NetInterfaceData(
                    uuid,
                    netName,
                    node,
                    addr,
                    null, // stlt conn port
                    null, // stlt conn encr type
                    driver,
                    transObjFactory,
                    transMgrProvider
                );
                ((NodeData) node).addNetInterface(accCtx, netData);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return netData;
    }
}

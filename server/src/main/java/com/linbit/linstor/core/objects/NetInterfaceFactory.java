package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.UUID;

public class NetInterfaceFactory
{
    private final NetInterfaceDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public NetInterfaceFactory(
        NetInterfaceDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public NetInterface create(
        AccessContext accCtx,
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        @Nullable TcpPortNumber port,
        @Nullable EncryptionType encrType
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        NetInterface netData = null;

        netData = node.getNetInterface(accCtx, netName);

        if (netData != null)
        {
            throw new LinStorDataAlreadyExistsException("The NetInterface already exists");
        }

        netData = new NetInterface(
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
        node.addNetInterface(accCtx, netData);

        return netData;
    }

    public NetInterface getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr
    )
        throws ImplementationError
    {

        NetInterface netData;
        try
        {
            netData = node.getNetInterface(accCtx, netName);
            if (netData == null)
            {
                /*
                 * stlt conn port and encr type are default null as one satellite
                 * should not communicate with any other satellite directly, thus
                 * the port and encr type are not needed
                 */

                netData = new NetInterface(
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
                node.addNetInterface(accCtx, netData);
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

package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeNetInterfaces.INET_ADDRESS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeNetInterfaces.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeNetInterfaces.NODE_NET_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeNetInterfaces.NODE_NET_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeNetInterfaces.STLT_CONN_ENCR_TYPE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeNetInterfaces.STLT_CONN_PORT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeNetInterfaces.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Objects;

@Singleton
public final class NetInterfaceDbDriver
    extends AbsProtectedDatabaseDriver<NetInterface, Void, Map<NodeName, ? extends Node>>
    implements NetInterfaceCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;
    private final SingleColumnDatabaseDriver<NetInterface, LsIpAddress> addressDriver;
    private final SingleColumnDatabaseDriver<NetInterface, TcpPortNumber> portDriver;
    private final SingleColumnDatabaseDriver<NetInterface, EncryptionType> encrTypeDriver;

    @Inject
    public NetInterfaceDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.NODE_NET_INTERFACES, dbEngineRef, objProtFactoryRef);
        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, netIf -> netIf.getUuid().toString());
        setColumnSetter(NODE_NAME, netIf -> netIf.getNode().getName().value);
        setColumnSetter(NODE_NET_NAME, netIf -> netIf.getName().value);
        setColumnSetter(NODE_NET_DSP_NAME, netIf -> netIf.getName().displayValue);
        setColumnSetter(INET_ADDRESS, netIf -> netIf.getAddress(dbCtxRef).getAddress());
        switch (getDbType())
        {
            case SQL: // fallthrough
            case ETCD:
                setColumnSetter(
                    STLT_CONN_PORT,
                    netIf -> TcpPortNumber.getValueNullable(netIf.getStltConnPort(dbCtxRef))
                );
                break;
            case K8S_CRD:
                // TODO: change NetIf.STLT_CONN_PORT from SHORT to INTEGER with SQL migration
                setColumnSetter(
                    STLT_CONN_PORT,
                    netIf ->
                    {
                        Integer nullable = TcpPortNumber.getValueNullable(netIf.getStltConnPort(dbCtxRef));
                        return nullable != null ? nullable.shortValue() : null;
                    }
                );
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }
        setColumnSetter(STLT_CONN_ENCR_TYPE, netIf ->
        {
            EncryptionType type = netIf.getStltConnEncryptionType(dbCtxRef);
            String ret = null;
            if (type != null)
            {
                ret = type.name();
            }
            return ret;
        });

        addressDriver = generateSingleColumnDriver(
            INET_ADDRESS,
            netIf -> netIf.getAddress(dbCtxRef).getAddress(),
            LsIpAddress::getAddress
        );
        portDriver = generateSingleColumnDriver(
            STLT_CONN_PORT,
            netIf -> Objects.toString(netIf.getStltConnPort(dbCtxRef)),
            TcpPortNumber::getValueNullable
        );
        encrTypeDriver = generateSingleColumnDriver(
            STLT_CONN_ENCR_TYPE,
            netIf -> Objects.toString(netIf.getStltConnEncryptionType(dbCtxRef)),
            EncryptionType::nameOfNullable
        );
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterface, LsIpAddress> getNetInterfaceAddressDriver()
    {
        return addressDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterface, TcpPortNumber> getStltConnPortDriver()
    {
        return portDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterface, EncryptionType> getStltConnEncrTypeDriver()
    {
        return encrTypeDriver;
    }

    @Override
    protected Pair<NetInterface, Void> load(
        RawParameters raw,
        Map<NodeName, ? extends Node> nodesMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final TcpPortNumber port;

        switch (getDbType())
        {
            case ETCD:
                String portStr = raw.get(STLT_CONN_PORT);
                port = portStr != null ? new TcpPortNumber(Integer.parseInt(portStr)) : null;
                break;
            case SQL: // fall-through
            case K8S_CRD:
                Object portObj = raw.get(STLT_CONN_PORT);
                if (portObj != null)
                {
                    int portInt;
                    if (portObj instanceof Integer)
                    {
                        portInt = ((Integer) portObj);
                    }
                    else
                    if (portObj instanceof Short)
                    {
                        portInt = ((Short) portObj).intValue();
                    }
                    else
                    {
                        throw new LinStorDBRuntimeException(
                            "Unexpected type for " + STLT_CONN_PORT.getName() + ": " + portObj.getClass()
                        );
                    }
                    port = new TcpPortNumber(portInt);
                }
                else
                {
                    port = null;
                }
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        return new Pair<>(
            new NetInterface(
                raw.build(UUID, java.util.UUID::fromString),
                raw.build(NODE_NET_DSP_NAME, NetInterfaceName::new),
                nodesMap.get(raw.build(NODE_NAME, NodeName::new)),
                raw.build(INET_ADDRESS, LsIpAddress::new),
                port,
                raw.build(STLT_CONN_ENCR_TYPE, EncryptionType.class),
                this,
                transObjFactory,
                transMgrProvider
            ),
            null
        );
    }

    @Override
    protected String getId(NetInterface netIf)
    {
        return "(NodeName=" + netIf.getNode().getName().displayValue +
            " NetInterfaceName=" + netIf.getName().displayValue + ")";
    }

}

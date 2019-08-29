package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
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
public class NetInterfaceDbDriver
    extends AbsDatabaseDriver<NetInterfaceData, Void, Map<NodeName, ? extends Node>>
    implements NetInterfaceDataDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> addressDriver;
    private final SingleColumnDatabaseDriver<NetInterfaceData, TcpPortNumber> portDriver;
    private final SingleColumnDatabaseDriver<NetInterfaceData, EncryptionType> encrTypeDriver;

    @Inject
    public NetInterfaceDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.NODE_NET_INTERFACES, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, netIf -> netIf.getUuid().toString());
        setColumnSetter(NODE_NAME, netIf -> netIf.getNode().getName().value);
        setColumnSetter(NODE_NET_NAME, netIf -> netIf.getName().value);
        setColumnSetter(NODE_NET_DSP_NAME, netIf -> netIf.getName().displayValue);
        setColumnSetter(INET_ADDRESS, netIf -> netIf.getAddress(dbCtxRef).getAddress());
        setColumnSetter(STLT_CONN_PORT, netIf -> TcpPortNumber.getValueNullable(netIf.getStltConnPort(dbCtxRef)));
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
            EncryptionType::name
        );
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> getNetInterfaceAddressDriver()
    {
        return addressDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, TcpPortNumber> getStltConnPortDriver()
    {
        return portDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, EncryptionType> getStltConnEncrTypeDriver()
    {
        return encrTypeDriver;
    }

    @Override
    protected Pair<NetInterfaceData, Void> load(
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
            case SQL:
                Short portShort = raw.<Short> get(STLT_CONN_PORT);
                port = portShort != null ? new TcpPortNumber(portShort.intValue()) : null;
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        return new Pair<>(
            new NetInterfaceData(
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
    protected String getId(NetInterfaceData netIf)
    {
        return "(NodeName=" + netIf.getNode().getName().displayValue +
            " NetInterfaceName=" + netIf.getName().displayValue + ")";
    }

}

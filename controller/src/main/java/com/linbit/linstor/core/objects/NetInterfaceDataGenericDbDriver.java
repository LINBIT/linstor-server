package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterfaceData;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Singleton
public class NetInterfaceDataGenericDbDriver implements NetInterfaceDataDatabaseDriver
{
    private static final String TBL_NODE_NET = DbConstants.TBL_NODE_NET_INTERFACES;

    private static final String NODE_NAME = DbConstants.NODE_NAME;

    private static final String NET_UUID = DbConstants.UUID;
    private static final String NET_NAME = DbConstants.NODE_NET_NAME;
    private static final String NET_DSP_NAME = DbConstants.NODE_NET_DSP_NAME;
    private static final String INET_ADDRESS = DbConstants.INET_ADDRESS;
    private static final String STLT_CONN_PORT = DbConstants.STLT_CONN_PORT;
    private static final String STLT_CONN_ENCR_TYPE = DbConstants.STLT_CONN_ENCR_TYPE;

    private static final String NNI_SELECT_ALL =
        " SELECT "  + NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
                      INET_ADDRESS + ", " + STLT_CONN_PORT + ", " + STLT_CONN_ENCR_TYPE +
        " FROM " + TBL_NODE_NET;
    private static final String NNI_SELECT_BY_NODE_AND_NET =
        NNI_SELECT_ALL +
        " WHERE " + NODE_NAME + " = ? AND " +
                    NET_NAME  + " = ?";

    private static final String NNI_INSERT_WITH_STLT_CONN =
        " INSERT INTO " + TBL_NODE_NET +
        " (" +
        NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
        INET_ADDRESS + ", " + STLT_CONN_PORT + ", " + STLT_CONN_ENCR_TYPE +
        ") VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String NNI_DELETE =
        " DELETE FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";
    private static final String NNI_UPDATE_ADR =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + INET_ADDRESS  + " = ? " +
        " WHERE " + NODE_NAME     + " = ? AND " +
        "       " + NET_NAME      + " = ?";
    private static final String NNI_UPDATE_STLT_CONN_PORT =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + STLT_CONN_PORT + " = ? " +
        " WHERE " + NODE_NAME      + " = ? AND " +
        "       " + NET_NAME       + " = ?";
    private static final String NNI_UPDATE_STLT_CONN_ENCR_TYPE =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + STLT_CONN_ENCR_TYPE + " = ? " +
        " WHERE " + NODE_NAME           + " = ? AND " +
        "       " + NET_NAME            + " = ?";

    private final SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> netIfAddressDriver;
    private final SingleColumnDatabaseDriver<NetInterfaceData, TcpPortNumber> netIfStltConnPortDriver;
    private final SingleColumnDatabaseDriver<NetInterfaceData, EncryptionType> netIfStltConnEncrTypeDriver;

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public NetInterfaceDataGenericDbDriver(
        @SystemContext AccessContext ctx,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = ctx;
        errorReporter = errorReporterRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        netIfAddressDriver = new NodeNetInterfaceAddressDriver();
        netIfStltConnPortDriver = new StltConnPortDriver();
        netIfStltConnEncrTypeDriver = new StltConnEncrTypeDriver();
    }

    public List<NetInterfaceData> loadAll(Map<NodeName, ? extends Node> tmpNodesMap) throws DatabaseException
    {
        errorReporter.logTrace("Loading all NetworkInterfaces");
        List<NetInterfaceData> netIfs = new ArrayList<>();
        String nodeNameStr = null;
        try (PreparedStatement stmt = getConnection().prepareStatement(NNI_SELECT_ALL))
        {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next())
            {
                nodeNameStr = resultSet.getString(NODE_NAME);
                NodeName nodeName = asNodeName(nodeNameStr);
                NetInterfaceName netName = asNetName(resultSet.getString(NET_DSP_NAME));

                netIfs.add(
                    restoreInstance(
                        tmpNodesMap.get(nodeName),
                        netName,
                        resultSet
                    )
                );
            }
            resultSet.close();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d NetworkInterfaces", netIfs.size());
        return netIfs;
    }

    private NodeName asNodeName(String nodeNameStr)
    {
        NodeName nodeName;
        try
        {
            nodeName = new NodeName(nodeNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                TBL_NODE_NET + " contains invalid node name: '" + nodeNameStr + "'",
                invalidNameExc
            );
        }
        return nodeName;
    }

    private NetInterfaceName asNetName(String netNameStr)
    {
        NetInterfaceName netName;
        try
        {
            netName = new NetInterfaceName(netNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                TBL_NODE_NET + " contains invalid net interface name: '" + netNameStr + "'",
                invalidNameExc
            );
        }
        return netName;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(NetInterfaceData netInterfaceData) throws DatabaseException
    {
        errorReporter.logTrace("Creating NetInterface %s", getId(netInterfaceData));
        try (PreparedStatement stmt = getConnection().prepareStatement(NNI_INSERT_WITH_STLT_CONN))
        {
            LsIpAddress inetAddress = getAddress(netInterfaceData);

            stmt.setString(1, netInterfaceData.getUuid().toString());
            stmt.setString(2, netInterfaceData.getNode().getName().value);
            stmt.setString(3, netInterfaceData.getName().value);
            stmt.setString(4, netInterfaceData.getName().displayValue);
            stmt.setString(5, inetAddress.getAddress());

            if (netInterfaceData.isUsableAsStltConn(dbCtx))
            {
                stmt.setInt(6, netInterfaceData.getStltConnPort(dbCtx).value);
                stmt.setString(7, netInterfaceData.getStltConnEncryptionType(dbCtx).name());
            }
            else
            {
                stmt.setNull(6, Types.INTEGER);
                stmt.setNull(7, Types.VARCHAR);
            }
            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accDeniedExc);
        }
        errorReporter.logTrace("NetInterface created %s", getId(netInterfaceData));
    }

    public void ensureEntryExists(NetInterfaceData netIfData) throws DatabaseException
    {
        errorReporter.logTrace("Ensuring NetInterface exists %s", getId(netIfData));
        try (PreparedStatement stmt = getConnection().prepareStatement(NNI_SELECT_BY_NODE_AND_NET))
        {
            stmt.setString(1, netIfData.getNode().getName().value);
            stmt.setString(2, netIfData.getName().value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (!resultSet.next())
                {
                    create(netIfData);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        // no traceLog
    }

    @Override
    public void delete(NetInterfaceData netInterfaceData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting NetInterface %s", getId(netInterfaceData));
        try (PreparedStatement stmt = getConnection().prepareStatement(NNI_DELETE))
        {
            stmt.setString(1, netInterfaceData.getNode().getName().value);
            stmt.setString(2, netInterfaceData.getName().value);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("NetInterface deleted %s", getId(netInterfaceData));
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> getNetInterfaceAddressDriver()
    {
        return netIfAddressDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, EncryptionType> getStltConnEncrTypeDriver()
    {
        return netIfStltConnEncrTypeDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, TcpPortNumber> getStltConnPortDriver()
    {
        return netIfStltConnPortDriver;
    }

    private LsIpAddress getAddress(NetInterface value)
    {
        LsIpAddress ip = null;
        try
        {
            ip = value.getAddress(dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
        return ip;
    }

    private NetInterfaceData restoreInstance(
        Node node,
        NetInterfaceName netName,
        ResultSet resultSet
    )
        throws DatabaseException
    {
        try
        {
            UUID uuid = UUID.fromString(resultSet.getString(NET_UUID));
            LsIpAddress addr;
            TcpPortNumber stltPort = null;
            EncryptionType stltEncrType = null;

            Integer tmpPort = null;
            String tmpEncrType = null;
            try
            {
                addr = new LsIpAddress(resultSet.getString(INET_ADDRESS));
                tmpPort = resultSet.getInt(STLT_CONN_PORT);
                if (resultSet.wasNull())
                {
                    tmpPort = null;
                }
                tmpEncrType = resultSet.getString(STLT_CONN_ENCR_TYPE);
                if (resultSet.wasNull())
                {
                    tmpEncrType = null;
                }

                if (tmpPort != null && tmpEncrType != null)
                {
                    stltPort = new TcpPortNumber(tmpPort);
                    stltEncrType = EncryptionType.valueOfIgnoreCase(tmpEncrType);
                }
            } catch (InvalidIpAddressException invalidIpAddressExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The ip address of a stored NetInterface could not be restored " +
                            "(NodeName=%s, NetInterfaceName=%s, invalid address=%s)",
                        node.getName().displayValue,
                        netName.displayValue,
                        resultSet.getString(INET_ADDRESS)
                    ),
                    invalidIpAddressExc
                );
            } catch (ValueOutOfRangeException valOutOfRangeExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The satellite port of a stored NetInterface could not be restored " +
                            "(NodeName=%s, NetInterfaceName=%s, invalid satellite port=%d)",
                        node.getName().displayValue,
                        netName.displayValue,
                        tmpPort
                    ),
                    valOutOfRangeExc
                );
            } catch (IllegalArgumentException illegalArgExc)
            {
                // exc from EncryptionType.valueOfIgnoreCase(...)
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The satellite encryption type of a stored NetInterface could not be restored " +
                            "(NodeName=%s, NetInterfaceName=%s, invalid satellite encryption type=%s)",
                        node.getName().displayValue,
                        netName.displayValue,
                        tmpEncrType
                    ),
                    illegalArgExc
                );
            }
            NetInterfaceData ret = new NetInterfaceData(
                uuid,
                netName,
                node,
                addr,
                stltPort,
                stltEncrType,
                this,
                transObjFactory,
                transMgrProvider
            );

            return ret;
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(NetInterfaceData netIfData)
    {
        return getId(
            netIfData.getNode().getName().displayValue,
            netIfData.getName().displayValue
        );
    }

    private String getId(String nodeName, String niName)
    {
        return "(NodeName=" + nodeName + " NetInterfaceName=" + niName + ")";
    }

    private class NodeNetInterfaceAddressDriver implements SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(NetInterfaceData parent, LsIpAddress inetAddress)
            throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating NetInterface's address from [%s] to [%s] %s",
                getAddress(parent).getAddress(),
                inetAddress.getAddress(),
                getId(parent)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(NNI_UPDATE_ADR))
            {
                stmt.setString(1, inetAddress.getAddress());
                stmt.setString(2, parent.getNode().getName().value);
                stmt.setString(3, parent.getName().value);

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "NetInterface's address updated from [%s] to [%s] %s",
                getAddress(parent).getAddress(),
                inetAddress.getAddress(),
                getId(parent)
            );
        }
    }

    private class StltConnPortDriver implements SingleColumnDatabaseDriver<NetInterfaceData, TcpPortNumber>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(NetInterfaceData parent, TcpPortNumber port)
            throws DatabaseException
        {
            Integer portNumber = null;
            if (getStltPort(parent) != null)
            {
                portNumber = getStltPort(parent).value;
            }

            errorReporter.logTrace(
                "Updating NetInterface's satellite connection port from [%d] to [%d] %s",
                portNumber,
                port.value,
                getId(parent)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(NNI_UPDATE_STLT_CONN_PORT))
            {
                stmt.setInt(1, port.value);
                stmt.setString(2, parent.getNode().getName().value);
                stmt.setString(3, parent.getName().value);

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "NetInterface's satellite connection port updated from [%d] to [%d] %s",
                portNumber,
                port.value,
                getId(parent)
            );
        }

        private TcpPortNumber getStltPort(NetInterfaceData netIf)
        {
            TcpPortNumber port = null;
            try
            {
                port = netIf.getStltConnPort(dbCtx);
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
            }
            return port;
        }
    }

    private class StltConnEncrTypeDriver implements SingleColumnDatabaseDriver<NetInterfaceData, EncryptionType>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(NetInterfaceData parent, EncryptionType encrType)
            throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating NetInterface's satellite connections encryption type from [%s] to [%s] %s",
                getStltEncrType(parent),
                encrType.name(),
                getId(parent)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(NNI_UPDATE_STLT_CONN_ENCR_TYPE))
            {
                stmt.setString(1, encrType.name());
                stmt.setString(2, parent.getNode().getName().value);
                stmt.setString(3, parent.getName().value);

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "NetInterface's satellite connections encryption type updated from [%s] to [%s] %s",
                getStltEncrType(parent),
                encrType.name(),
                getId(parent)
            );
        }

        private String getStltEncrType(NetInterfaceData parent)
        {
            String type = null;
            try
            {
                if (parent.getStltConnEncryptionType(dbCtx) != null)
                {
                    type = parent.getStltConnEncryptionType(dbCtx).name();
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DatabaseLoader.handleAccessDeniedException(accDeniedExc);
            }
            return type;
        }
    }
}

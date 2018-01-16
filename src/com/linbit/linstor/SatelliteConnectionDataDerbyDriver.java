package com.linbit.linstor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.SatelliteConnection.EncryptionType;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.SatelliteConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.UuidUtils;

public class SatelliteConnectionDataDerbyDriver implements SatelliteConnectionDataDatabaseDriver
{
    private static final String TBL_SC = DerbyConstants.TBL_SATELLITE_CONNECTIONS;

    private static final String SC_UUID = DerbyConstants.UUID;
    private static final String SC_NODE_NAME = DerbyConstants.NODE_NAME;
    private static final String SC_NODE_NET_NAME = DerbyConstants.NODE_NET_NAME;
    private static final String SC_PORT = DerbyConstants.TCP_PORT;
    private static final String SC_TYPE = DerbyConstants.INET_TYPE;

    private static final String SC_SELECT_BY_NODE =
        " SELECT " + SC_UUID + ", " + SC_NODE_NAME + ", " + SC_NODE_NET_NAME + "," +
                     SC_PORT + ", " + SC_TYPE +
        " FROM " + TBL_SC +
        " WHERE " + SC_NODE_NAME + " = ?";

    private static final String SC_INSERT =
        " INSERT INTO " + TBL_SC +
        " (" + SC_UUID + ", " + SC_NODE_NAME + ", " + SC_NODE_NET_NAME + ", " +
               SC_PORT + ", " + SC_TYPE +
        " ) " +
        " VALUES (?, ?, ?, ?, ?)";
    private static final String SC_UPDATE_PORT =
        " UPDATE " + TBL_SC +
        " SET " + SC_PORT + " = ?" +
        " WHERE " + SC_NODE_NAME + " = ?";
    private static final String SC_UPDATE_TYPE =
        " UPDATE " + TBL_SC +
        " SET " + SC_TYPE + " = ?" +
        " WHERE " + SC_NODE_NAME + " = ?";
    private static final String SC_DELETE =
        " DELETE FROM " + TBL_SC +
        " WHERE " + SC_NODE_NAME + " = ?";

    private final PortDriver portDriver = new PortDriver();
    private final TypeDriver typeDriver = new TypeDriver();

    private final AccessContext privCtx;
    private final ErrorReporter errorReporter;

    private NetInterfaceDataDerbyDriver netInterfaceDriver;

    public SatelliteConnectionDataDerbyDriver(AccessContext privCtxRef, ErrorReporter errorReporterRef)
    {
        privCtx = privCtxRef;
        errorReporter = errorReporterRef;
    }

    public void initialize(NetInterfaceDataDerbyDriver netInterfaceDataDerbyDriverRef)
    {
        netInterfaceDriver = netInterfaceDataDerbyDriverRef;
    }

    @Override
    public void create(SatelliteConnection satelliteConnectionData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating SatelliteConnection %s", getId(satelliteConnectionData));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SC_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(satelliteConnectionData.getUuid()));
            stmt.setString(2, satelliteConnectionData.getNode().getName().value);
            stmt.setString(3, satelliteConnectionData.getNetInterface().getName().value);
            stmt.setInt(4, satelliteConnectionData.getPort().value);
            stmt.setString(5, satelliteConnectionData.getEncryptionType().name());

            stmt.executeUpdate();
        }
        errorReporter.logTrace("SatelliteConnection created %s", getId(satelliteConnectionData));

    }
    @Override
    public SatelliteConnectionData load(
        Node node,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading SatelliteConnection %s", getId(node));
        SatelliteConnectionData stltConn = null;
        try
        {
            // check node
            stltConn = (SatelliteConnectionData) node.getSatelliteConnection(privCtx);
            if (stltConn == null)
            {
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SC_SELECT_BY_NODE))
                {
                    stmt.setString(1, node.getName().value);
                    try (ResultSet resultSet = stmt.executeQuery())
                    {
                        if (resultSet.next())
                        {
                            UUID uuid = UuidUtils.asUuid(resultSet.getBytes(SC_UUID));
                            TcpPortNumber port;
                            try
                            {
                                port = new TcpPortNumber(resultSet.getInt(SC_PORT));
                            }
                            catch (ValueOutOfRangeException valOutOfRangeExc)
                            {
                                throw new LinStorSqlRuntimeException(
                                    String.format(
                                        "A TcpPortNumber of a stored SatelliteConnection in table %s could not be restore. " +
                                            "(NodeName=%s, NetInterfaceName=%s, invalid port=%d)",
                                        TBL_SC,
                                        resultSet.getString(SC_NODE_NAME),
                                        resultSet.getString(SC_NODE_NET_NAME),
                                        resultSet.getInt(SC_PORT)
                                    ),
                                    valOutOfRangeExc
                                );
                            }
                            NetInterfaceName netIfName;
                            try
                            {
                                netIfName = new NetInterfaceName(resultSet.getString(SC_NODE_NET_NAME));
                            }
                            catch (InvalidNameException invalidNameExc)
                            {
                                throw new LinStorSqlRuntimeException(
                                    String.format(
                                        "A Network interface name of a stored SatelliteConnection in table %s " +
                                        " could not be restore. (NodeName=%s, invalid NetInterfaceName=%s) ",
                                        TBL_SC,
                                        resultSet.getString(SC_NODE_NAME),
                                        resultSet.getString(SC_NODE_NET_NAME)
                                    ),
                                    invalidNameExc
                                );
                            }

                            EncryptionType encryptionType = EncryptionType.valueOf(resultSet.getString(SC_TYPE));

                            NetInterface netIf = node.getNetInterface(privCtx, netIfName);

                            stltConn = new SatelliteConnectionData(
                                uuid,
                                node,
                                netIf,
                                port,
                                encryptionType
                            );
                            errorReporter.logTrace("SatelliteConnection restored from DB %s", getId(node));
                        }
                        else
                        {
                            errorReporter.logDebug(
                                "No SatelliteConnections found in DB for node '" +
                                node.getName().displayValue + "'"
                            );
                        }
                    }
                }
            }
            else
            {
                errorReporter.logTrace("SatelliteConnection loaded from cache %s", getId(node));
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }

        return stltConn;
    }

    @Override
    public void delete(SatelliteConnection satelliteConnectionData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting SatelliteConnection %s", getId(satelliteConnectionData));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SC_DELETE))
        {
            stmt.setString(1, satelliteConnectionData.getNode().getName().value);
            stmt.executeUpdate();
        }
        errorReporter.logTrace("SatelliteConnection deleted %s", getId(satelliteConnectionData));
    }

    @Override
    public SingleColumnDatabaseDriver<SatelliteConnectionData, TcpPortNumber> getSatelliteConnectionPortDriver()
    {
        return portDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<SatelliteConnectionData, EncryptionType> getSatelliteConnectionTypeDriver()
    {
        return typeDriver;
    }

    private String getId(Node node)
    {
        return getNodeId(node.getName().displayValue);
    }

    private String getId(SatelliteConnection satelliteConnectionData)
    {
        return getId(
            satelliteConnectionData.getNode().getName().displayValue,
            satelliteConnectionData.getNetInterface().getName().displayValue
        );
    }

    private String getNodeId(String nodeName)
    {
        return "(NodeName = " + nodeName + ")";
    }

    private String getId(String nodeName, String netIfName)
    {
        return "(NodeName = " + nodeName + ", NetIfName = " + netIfName + ")";
    }

    private class PortDriver implements SingleColumnDatabaseDriver<SatelliteConnectionData, TcpPortNumber>
    {
        @Override
        public void update(SatelliteConnectionData stltConn, TcpPortNumber port, TransactionMgr transMgr)
            throws SQLException
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SC_UPDATE_PORT))
            {
                int oldPort = stltConn.getPort().value;

                errorReporter.logTrace(
                    "Updating SatelliteConnection's port from [%d] to [%d] %s",
                    oldPort,
                    port.value,
                    getId(stltConn)
                );

                stmt.setInt(1, port.value);
                stmt.setString(2, stltConn.getNode().getName().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "SatelliteConnection's port updated from [%d] to [%d] %s",
                    oldPort,
                    port.value,
                    getId(stltConn)
                );
            }
        }
    }

    private class TypeDriver implements SingleColumnDatabaseDriver<SatelliteConnectionData, EncryptionType>
    {
        @Override
        public void update(SatelliteConnectionData stltConn, EncryptionType type, TransactionMgr transMgr)
            throws SQLException
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SC_UPDATE_TYPE))
            {
                String oldType = stltConn.getEncryptionType().name();

                errorReporter.logTrace(
                    "Updating SatelliteConnection's port from [%s] to [%s] %s",
                    oldType,
                    type.name(),
                    getId(stltConn)
                );

                stmt.setString(1, type.name());
                stmt.setString(2, stltConn.getNode().getName().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "SatelliteConnection's port updated from [%s] to [%s] %s",
                    oldType,
                    type.name(),
                    getId(stltConn)
                );
            }
        }
    }
}

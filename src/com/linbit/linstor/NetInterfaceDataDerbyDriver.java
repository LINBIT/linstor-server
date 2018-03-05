package com.linbit.linstor;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.UuidUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Singleton
public class NetInterfaceDataDerbyDriver implements NetInterfaceDataDatabaseDriver
{
    private static final String TBL_NODE_NET = DerbyConstants.TBL_NODE_NET_INTERFACES;

    private static final String NODE_NAME = DerbyConstants.NODE_NAME;

    private static final String NET_UUID = DerbyConstants.UUID;
    private static final String NET_NAME = DerbyConstants.NODE_NET_NAME;
    private static final String NET_DSP_NAME = DerbyConstants.NODE_NET_DSP_NAME;
    private static final String INET_ADDRESS = DerbyConstants.INET_ADDRESS;

    private static final String NNI_SELECT_BY_NODE =
        " SELECT "  + NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
                      INET_ADDRESS +
        " FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ?";
    private static final String NNI_SELECT_BY_NODE_AND_NET =
        " SELECT "  + NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
                      INET_ADDRESS +
        " FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
                    NET_NAME  + " = ?";

    private static final String NNI_INSERT =
        " INSERT INTO " + TBL_NODE_NET +
        " (" +
            NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
            INET_ADDRESS +
        ") VALUES (?, ?, ?, ?, ?)";
    private static final String NNI_DELETE =
        " DELETE FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";
    private static final String NNI_UPDATE_ADR =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + INET_ADDRESS  + " = ? " +
        " WHERE " + NODE_NAME     + " = ? AND " +
        "       " + NET_NAME      + " = ?";

    private final SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> netIfAddressDriver;

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public NetInterfaceDataDerbyDriver(
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
    }

    @Override
    public NetInterfaceData load(
        Node node,
        NetInterfaceName niName,
        boolean logWarnIfNotExists
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading NetInterface %s", getId(node, niName));

        NetInterfaceData netIfData = null;

        try (PreparedStatement stmt = getConnection().prepareStatement(NNI_SELECT_BY_NODE_AND_NET))
        {
            stmt.setString(1, node.getName().value);
            stmt.setString(2, niName.value);
            NetInterfaceName loadedNiName;
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    try
                    {
                        loadedNiName = new NetInterfaceName(resultSet.getString(NET_DSP_NAME));
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        throw new LinStorSqlRuntimeException(
                            String.format(
                                "The display name of a stored NetInterface could not be restored " +
                                    "(NodeName=%s, invalid NetInterfaceName=%s)",
                                node.getName().displayValue,
                                resultSet.getString(NET_DSP_NAME)
                            ),
                            invalidNameExc
                        );
                    }
                    netIfData = restoreInstance(node, loadedNiName, resultSet);
                    // ("loaded from [DB|cache]...") msg gets logged in restoreInstance method
                }
                else
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning("NetInterface not found in DB %s", getId(node, niName));
                }
            }
        }
        return netIfData;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(NetInterfaceData netInterfaceData) throws SQLException
    {
        errorReporter.logTrace("Creating NetInterface %s", getId(netInterfaceData));
        try (PreparedStatement stmt = getConnection().prepareStatement(NNI_INSERT))
        {
            LsIpAddress inetAddress = getAddress(netInterfaceData);

            stmt.setBytes(1, UuidUtils.asByteArray(netInterfaceData.getUuid()));
            stmt.setString(2, netInterfaceData.getNode().getName().value);
            stmt.setString(3, netInterfaceData.getName().value);
            stmt.setString(4, netInterfaceData.getName().displayValue);
            stmt.setString(5, inetAddress.getAddress());

            stmt.executeUpdate();
        }
        errorReporter.logTrace("NetInterface created %s", getId(netInterfaceData));
    }

    public void ensureEntryExists(NetInterfaceData netIfData) throws SQLException
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
        // no traceLog
    }

    @Override
    public void delete(NetInterfaceData netInterfaceData) throws SQLException
    {
        errorReporter.logTrace("Deleting NetInterface %s", getId(netInterfaceData));
        try (PreparedStatement stmt = getConnection().prepareStatement(NNI_DELETE))
        {
            stmt.setString(1, netInterfaceData.getNode().getName().value);
            stmt.setString(2, netInterfaceData.getName().value);

            stmt.executeUpdate();
        }
        errorReporter.logTrace("NetInterface deleted %s", getId(netInterfaceData));
    }

    @Override
    public SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> getNetInterfaceAddressDriver()
    {
        return netIfAddressDriver;
    }

    public List<NetInterfaceData> loadNetInterfaceData(Node node)
        throws SQLException
    {
        errorReporter.logTrace("Loading all NetInterfaces by node %s", getId(node));
        List<NetInterfaceData> netIfDataList;
        try (PreparedStatement stmt = getConnection().prepareStatement(NNI_SELECT_BY_NODE))
        {
            stmt.setString(1, node.getName().value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                netIfDataList = new ArrayList<>();
                while (resultSet.next())
                {
                    NetInterfaceName netName;
                    try
                    {
                        netName = new NetInterfaceName(resultSet.getString(NET_DSP_NAME));
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        throw new LinStorSqlRuntimeException(
                            String.format(
                                "The display name of a stored NetInterface could not be restored " +
                                    "(NodeName=%s, invalid NetInterfaceName=%s)",
                                node.getName().displayValue,
                                resultSet.getString(NET_DSP_NAME)
                            ),
                            invalidNameExc
                        );
                    }
                    netIfDataList.add(restoreInstance(node, netName, resultSet));
                }
            }
        }
        errorReporter.logTrace(
            "Loaded %d NetInterfaces for node %s",
            netIfDataList.size(),
            getId(node)
        );
        return netIfDataList;
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
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
        return ip;
    }

    private NetInterfaceData restoreInstance(
        Node node,
        NetInterfaceName netName,
        ResultSet resultSet
    )
        throws SQLException
    {
        NetInterfaceData ret = cacheGet(node, netName);
        if (ret == null)
        {
            UUID uuid = UuidUtils.asUuid(resultSet.getBytes(NET_UUID));
            LsIpAddress addr;

            try
            {
                addr = new LsIpAddress(resultSet.getString(INET_ADDRESS));
            }
            catch (InvalidIpAddressException invalidIpAddressExc)
            {
                throw new LinStorSqlRuntimeException(
                    String.format(
                        "The ip address of a stored NetInterface could not be restored " +
                            "(NodeName=%s, NetInterfaceName=%s, invalid address=%s)",
                        node.getName().displayValue,
                        netName.displayValue,
                        resultSet.getString(INET_ADDRESS)
                    ),
                    invalidIpAddressExc
                );
            }
            try
            {
                ret = new NetInterfaceData(
                    uuid,
                    dbCtx,
                    netName,
                    node,
                    addr,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }

        return ret;
    }

    private NetInterfaceData cacheGet(Node node, NetInterfaceName netName)
    {
        NetInterfaceData ret = null;
        try
        {
            ret = (NetInterfaceData) node.getNetInterface(dbCtx, netName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(Node node, NetInterfaceName niName)
    {
        return getId(
            node.getName().displayValue,
            niName.displayValue
        );
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

    private String getId(Node node)
    {
        return getNodeId(node.getName().displayValue);
    }

    private String getNodeId(String nodeName)
    {
        return "(NodeName=" + nodeName + ")";
    }

    private class NodeNetInterfaceAddressDriver implements SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(NetInterfaceData parent, LsIpAddress inetAddress)
            throws SQLException
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
            errorReporter.logTrace(
                "NetInterface's address updated from [%s] to [%s] %s",
                getAddress(parent).getAddress(),
                inetAddress.getAddress(),
                getId(parent)
            );
        }
    }
}

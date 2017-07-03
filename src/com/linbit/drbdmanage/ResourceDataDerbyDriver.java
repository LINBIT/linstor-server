package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.MapDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.InvalidValueException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class ResourceDataDerbyDriver implements ResourceDataDatabaseDriver
{
    private static final String TBL_RES = DerbyConstants.TBL_NODE_RESOURCE;

    private static final String RES_UUID = DerbyConstants.UUID;
    private static final String RES_NODE_NAME = DerbyConstants.NODE_NAME;
    private static final String RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String RES_NODE_ID = DerbyConstants.NODE_ID;
    private static final String RES_FLAGS = DerbyConstants.RES_FLAGS;

    private static final String RES_SELECT_BY_NODE =
        " SELECT " + RES_UUID + ", " + RES_NAME + ", " + RES_NODE_ID + ", " + RES_FLAGS +
        " FROM " + TBL_RES +
        " WHERE " + RES_NODE_NAME + " = ?";
    private static final String RES_SELECT = RES_SELECT_BY_NODE +
        " AND " + RES_NAME + " = ?";

    private AccessContext dbCtx;
    private NodeName nodeName;
    private ResourceName resName;


    public ResourceDataDerbyDriver(AccessContext accCtx, NodeName nodeName, ResourceName resName)
    {
        this.dbCtx = accCtx;
        this.nodeName = nodeName;
        this.resName = resName;
    }

    @Override
    public MapDatabaseDriver<VolumeNumber, Volume> getVolumeMapDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StateFlagsPersistence getStateFlagPersistence()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResourceData load(Connection con, NodeData node, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RES_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, resName.value);
        ResultSet resultSet = stmt.executeQuery();

        List<ResourceData> list = load(con, resultSet, dbCtx, node, serialGen, transMgr);

        ResourceData ret = null;
        if (!list.isEmpty())
        {
            ret = list.get(0);
        }
        return ret;
    }

    public static List<ResourceData> loadResourceData(Connection con, AccessContext dbCtx, NodeData node, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RES_SELECT_BY_NODE);
        stmt.setString(1, node.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        return load(con, resultSet, dbCtx, node, serialGen, transMgr);
    }

    private static List<ResourceData> load(
        Connection con,
        ResultSet resultSet,
        AccessContext dbCtx,
        NodeData node,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        List<ResourceData> resList = new ArrayList<>();
        while (resultSet.next())
        {
            try
            {
                ResourceName resName = new ResourceName(resultSet.getString(RES_NAME));
                ResourceDefinitionDataDatabaseDriver resDfnDriver = DrbdManage.getResourceDefinitionDataDatabaseDriver(resName);
                ResourceDefinition resDfn = resDfnDriver.load(con, serialGen, transMgr);

                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                    ObjectProtection.buildPath(resName)
                );
                ObjectProtection objProt = objProtDriver.loadObjectProtection(con);

                NodeId nodeId = new NodeId(resultSet.getInt(RES_NODE_ID));
                ResourceData resData = new ResourceData(
                    objProt,
                    UuidUtils.asUUID(resultSet.getBytes(RES_UUID)),
                    resDfn,
                    node,
                    nodeId,
                    serialGen,
                    transMgr
                );

                // restore props
                PropsConDatabaseDriver propDriver = DrbdManage.getPropConDatabaseDriver(PropsContainer.buildPath(node.getName(), resName));
                Props props = resData.getProps(dbCtx);
                Map<String, String> loadedProps = propDriver.load(con);
                for (Entry<String, String> entry : loadedProps.entrySet())
                {
                    try
                    {
                        props.setProp(entry.getKey(), entry.getValue());
                    }
                    catch (InvalidKeyException | InvalidValueException invalidException)
                    {
                        throw new DrbdSqlRuntimeException(
                            "Invalid property loaded from instance: " + PropsContainer.buildPath(node.getName(), resName),
                            invalidException
                        );
                    }
                }

                // restore flags
                long lFlags = resultSet.getLong(RES_FLAGS);
                StateFlags<RscFlags> stateFlags = resData.getStateFlags();
                for (RscFlags flag : RscFlags.values())
                {
                    if ((lFlags & flag.flagValue) == flag.flagValue)
                    {
                        stateFlags.enableFlags(dbCtx, flag);
                    }
                }

                // restore volumes
                VolumeDataDerbyDriver volDataDriver = (VolumeDataDerbyDriver) DrbdManage.getVolumeDataDatabaseDriver();
                List<VolumeData> volList = volDataDriver.load(con, resData, serialGen);
                for (VolumeData volData : volList)
                {
                    resData.setVolume(dbCtx, volData);
                }

                resList.add(resData);
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new DrbdSqlRuntimeException(
                    "A resource name in the table " + TBL_RES + " has been modified in the database to an illegal string.",
                    invalidNameExc
                );
            }
            catch (ValueOutOfRangeException valueOutOfRangeExc)
            {
                throw new DrbdSqlRuntimeException(
                    "A node id in the table " + TBL_RES + " has been modified in the database to an illegal value.",
                    valueOutOfRangeExc
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                throw new ImplementationError(
                    "Database's access context is not permitted to access resource.props",
                    accessDeniedExc
                );
            }
        }
        return resList;
    }
}

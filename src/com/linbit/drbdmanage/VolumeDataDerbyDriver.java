package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.InvalidValueException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsConDerbyDriver;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class VolumeDataDerbyDriver implements VolumeDataDatabaseDriver
{
    private static final String TBL_VOL = DerbyConstants.TBL_VOLUMES;

    private static final String VOL_UUID = DerbyConstants.UUID;
    private static final String VOL_NODE_NAME = DerbyConstants.NODE_NAME;
    private static final String VOL_RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String VOL_ID = DerbyConstants.VLM_NR;
    private static final String VOL_PATH = DerbyConstants.BLOCK_DEVICE_PATH;
    private static final String VOL_FLAGS = DerbyConstants.VLM_FLAGS;

    private static final String VOL_SELECT_BY_RES =
        " SELECT " +  VOL_UUID + ", " + VOL_ID + ", " + VOL_PATH + ", " + VOL_FLAGS +
        " FROM " + TBL_VOL +
        " WHERE " + VOL_NODE_NAME + " = ? AND " +
                    VOL_RES_NAME  + " = ?";
    private static final String VOL_SELECT =  VOL_SELECT_BY_RES +
        " AND " +   VOL_ID +        " = ?";
    private static final String VOL_INSERT =
        " INSERT INTO " + TBL_VOL +
        " VALUES (?, ?, ?, ?, ?, ?)";
    private static final String VOL_UPDATE_FLAGS =
        " UPDATE " + TBL_VOL +
        " SET " + VOL_FLAGS + " = ? " +
        " WHERE " + VOL_NODE_NAME + " = ? AND " +
                    VOL_RES_NAME  + " = ? AND " +
                    VOL_ID        + " = ?";
    private static final String VOL_DELETE =
        " DELETE FROM " + TBL_VOL +
        " WHERE " + VOL_NODE_NAME + " = ? AND " +
                    VOL_RES_NAME  + " = ? AND " +
                    VOL_ID        + " = ?";

    private static Hashtable<PrimaryKey, VolumeData> volCache = new Hashtable<>();
    private PropsConDerbyDriver propsDriver;
    private StateFlagsPersistence flagPersistenceDriver;

    private AccessContext dbCtx;

    private Resource res;
    private VolumeDefinition volDfn;

    public VolumeDataDerbyDriver(AccessContext privCtx, Resource resRef, VolumeDefinition volDfnRef)
    {
        dbCtx = privCtx;
        res = resRef;
        volDfn = volDfnRef;

        try
        {
            propsDriver = new PropsConDerbyDriver(
                PropsContainer.buildPath(
                    resRef.getDefinition().getName(),
                    volDfnRef.getVolumeNumber(dbCtx)
                )
            );
            flagPersistenceDriver = new VolFlagsPersistence();
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to acces VolumeData's volNumber",
                accessDeniedExc
            );
        }
    }

    @Override
    public VolumeData load(
        Connection con,
        TransactionMgr transMgr,
        SerialGenerator serialGen
    )
        throws SQLException
    {
        try(PreparedStatement stmt = con.prepareStatement(VOL_SELECT))
        {
            stmt.setString(1, res.getAssignedNode().getName().value);
            stmt.setString(2, res.getDefinition().getName().value);
            stmt.setInt(3, volDfn.getVolumeNumber(dbCtx).value);
            ResultSet resultSet = stmt.executeQuery();

            List<VolumeData> volList = load(con, resultSet, transMgr, serialGen);
            resultSet.close();

            VolumeData ret = cacheGet(res.getAssignedNode(), res.getDefinition(), volDfn.getVolumeNumber(dbCtx));

            if (ret == null)
            {
                if (!volList.isEmpty())
                {
                    ret = volList.get(0);
                }
            }
            else
            {
                if (volList.isEmpty())
                {
                    // XXX: user deleted db entry during runtime - throw exception?
                    // or just remove the item from the cache + node.removeRes(cachedRes) + warn the user?
                }
            }
            return ret;
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to get VolumeDefinition's volumeNumber",
                accessDeniedExc
            );
        }
    }

    public List<VolumeData> loadAllVolumesByResource(Connection con, TransactionMgr transMgr, SerialGenerator serialGen) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(VOL_SELECT_BY_RES);
        stmt.setString(1, res.getAssignedNode().getName().value);
        stmt.setString(2, res.getDefinition().getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<VolumeData> ret = load(con, resultSet, transMgr, serialGen);
        resultSet.close();
        stmt.close();

        return ret;
    }


    private List<VolumeData> load(Connection con, ResultSet resultSet, TransactionMgr transMgr, SerialGenerator serialGen) throws SQLException
    {
        List<VolumeData> volList = new ArrayList<>();
        while (resultSet.next())
        {
            VolumeDefinition volDfn = null;
            VolumeNumber volNr;
            try
            {
                volNr = new VolumeNumber(resultSet.getInt(VOL_ID));
            }
            catch (ValueOutOfRangeException valueOutOfRangeExc)
            {
                throw new DrbdSqlRuntimeException(
                    String.format("Invalid volumeNumber in %s: %d", TBL_VOL, resultSet.getInt(VOL_ID)),
                    valueOutOfRangeExc
                );
            }
            VolumeDefinitionDataDatabaseDriver volDfnDriver = DrbdManage.getVolumeDefinitionDataDatabaseDriver(
                res.getDefinition(),
                volNr
            );
            volDfn = volDfnDriver.load(con, transMgr, serialGen);

            VolumeData volData = cacheGet(res.getAssignedNode(), res.getDefinition(), volNr);
            if (volData == null)
            {
                volData = new VolumeData(
                    UuidUtils.asUUID(resultSet.getBytes(VOL_UUID)),
                    res,
                    volDfn,
                    resultSet.getString(VOL_PATH),
                    serialGen
                );

                // restore flags
                StateFlags<VlmFlags> flags = volData.getFlags();
                long lFlags = resultSet.getLong(VOL_FLAGS);
                for (VlmFlags flag : VlmFlags.values())
                {
                    if ((lFlags & flag.flagValue) == flag.flagValue)
                    {
                        try
                        {
                            flags.enableFlags(dbCtx, flag);
                        }
                        catch (AccessDeniedException accessDeniedExc)
                        {
                            throw new ImplementationError(
                                "Database's access context has no permission to set VolumeDefinition's flags",
                                accessDeniedExc
                            );
                        }
                    }
                }

                // restore props
                String instanceName = PropsContainer.buildPath(
                    res.getAssignedNode().getName(),
                    res.getDefinition().getName(),
                    volNr
                );
                PropsConDatabaseDriver propsDriver = DrbdManage.getPropConDatabaseDriver(instanceName);

                Map<String, String> propMap = propsDriver.load(con);
                try
                {
                    Props props = volData.getProps(dbCtx);

                    for (Entry<String, String> entry : propMap.entrySet())
                    {
                        try
                        {
                            props.setProp(entry.getKey(), entry.getValue());
                        }
                        catch (InvalidKeyException invalidKeyExc)
                        {
                            throw new DrbdSqlRuntimeException(
                                String.format("Invalid key loaded from PropsCon %s: %s", instanceName, entry.getKey()),
                                invalidKeyExc
                            );
                        }
                        catch (InvalidValueException invalidValueExc)
                        {
                            throw new DrbdSqlRuntimeException(
                                String.format("Invalid value loaded from PropsCon %s: %s", instanceName, entry.getValue()),
                                invalidValueExc
                            );
                        }
                    }

                }
                catch (AccessDeniedException accessDeniedExc)
                {
                    throw new ImplementationError(
                        "Database's access context has no permission to get Volume's propsContainer",
                        accessDeniedExc
                    );
                }
                cache(volData, dbCtx);
            }
            volList.add(volData);
        }
        // resultSet should be closed by caller of this method
        return volList;
    }

    @Override
    public void create(Connection con, VolumeData vol) throws SQLException
    {
        try(PreparedStatement stmt = con.prepareStatement(VOL_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(vol.getUuid()));
            stmt.setString(2, vol.getResource().getAssignedNode().getName().value);
            stmt.setString(3, vol.getResourceDfn().getName().value);
            stmt.setInt(4, vol.getVolumeDfn().getVolumeNumber(dbCtx).value);
            stmt.setString(5, vol.getBlockDevicePath(dbCtx));
            stmt.setLong(6, vol.getFlags().getFlagsBits(dbCtx));
            stmt.executeUpdate();
            cache(vol, dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access Volume",
                accessDeniedExc
            );
        }
    }

    @Override
    public void delete(Connection con) throws SQLException
    {
        try(PreparedStatement stmt = con.prepareStatement(VOL_DELETE))
        {
            stmt.setString(1, res.getAssignedNode().getName().value);
            stmt.setString(2, res.getDefinition().getName().value);
            stmt.setInt(3, volDfn.getVolumeNumber(dbCtx).value);
            stmt.executeUpdate();

            cacheRemove(res, volDfn, dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access Volume",
                accessDeniedExc
            );
        }
    }


    @Override
    public StateFlagsPersistence getStateFlagsPersistence()
    {
        return flagPersistenceDriver;
    }

    @Override
    public PropsConDatabaseDriver getPropsConDriver()
    {
        return propsDriver;
    }

    private static void cache(VolumeData vol, AccessContext accCtx)
    {
        try
        {
            volCache.put(
                new PrimaryKey(
                    vol.getResource().getAssignedNode().getName().value,
                    vol.getResourceDfn().getName().value,
                    vol.getVolumeDfn().getVolumeNumber(accCtx).value),
                vol
            );
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to acces VolumeData's volNumber",
                accessDeniedExc
            );
        }
    }

    private static VolumeData cacheGet(Node node, ResourceDefinition resDfn, VolumeNumber volNr)
    {
        return volCache.get(new PrimaryKey(node.getName().value, resDfn.getName().value, volNr.value));
    }

    private static void cacheRemove(Resource res, VolumeDefinition volDfn, AccessContext accCtx)
    {
        volCache.remove(getPk(res, volDfn, accCtx));
    }

    private static PrimaryKey getPk(Resource res, VolumeDefinition volDfn, AccessContext accCtx)
    {
        try
        {
            return new PrimaryKey(
                res.getAssignedNode().getName().value,
                res.getDefinition().getName().value,
                volDfn.getVolumeNumber(accCtx).value
            );
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to acces VolumeData's volNumber",
                accessDeniedExc
            );
        }
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static void clearCache()
    {
        volCache.clear();
    }

    private class VolFlagsPersistence implements StateFlagsPersistence
    {
        private final String nodeName;
        private final String resName;
        private final int volNr;

        public VolFlagsPersistence() throws AccessDeniedException
        {
            nodeName = res.getAssignedNode().getName().value;
            resName = res.getDefinition().getName().value;
            volNr = volDfn.getVolumeNumber(dbCtx).value;
        }

        @Override
        public void persist(Connection con, long flags) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(VOL_UPDATE_FLAGS);
            stmt.setLong(1, flags);
            stmt.setString(2, nodeName);
            stmt.setString(3, resName);
            stmt.setInt(4, volNr);
            stmt.executeUpdate();
            stmt.close();
        }
    }
}

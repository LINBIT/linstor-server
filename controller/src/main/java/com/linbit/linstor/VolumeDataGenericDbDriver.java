package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.StringUtils;
import com.linbit.utils.Pair;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class VolumeDataGenericDbDriver implements VolumeDataDatabaseDriver
{
    private static final String TBL_VOL = DbConstants.TBL_VOLUMES;

    private static final String VOL_UUID = DbConstants.UUID;
    private static final String VOL_NODE_NAME = DbConstants.NODE_NAME;
    private static final String VOL_RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String VOL_ID = DbConstants.VLM_NR;
    private static final String VOL_STOR_POOL = DbConstants.STOR_POOL_NAME;
    private static final String VOL_FLAGS = DbConstants.VLM_FLAGS;

    private static final String SELECT_ALL =
        " SELECT " + VOL_UUID + ", " + VOL_NODE_NAME + ", " + VOL_RES_NAME + ", " +
            VOL_ID + ", " + VOL_STOR_POOL + ", " + VOL_FLAGS +
        " FROM " + TBL_VOL;

    private static final String INSERT =
        " INSERT INTO " + TBL_VOL +
        " (" +
            VOL_UUID + ", " + VOL_NODE_NAME + ", " + VOL_RES_NAME + ", " +
            VOL_ID + ", " + VOL_STOR_POOL + ", " + VOL_FLAGS +
        ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_FLAGS =
        " UPDATE " + TBL_VOL +
        " SET " + VOL_FLAGS + " = ? " +
        " WHERE " + VOL_NODE_NAME + " = ? AND " +
                    VOL_RES_NAME  + " = ? AND " +
                    VOL_ID        + " = ?";
    private static final String UPDATE_STOR_POOL =
        " UPDATE " + TBL_VOL +
            " SET "   + VOL_STOR_POOL + " = ? " +
            " WHERE " + VOL_NODE_NAME + " = ? AND " +
                        VOL_RES_NAME  + " = ? AND " +
                        VOL_ID        + " = ?";
    private static final String DELETE =
        " DELETE FROM " + TBL_VOL +
        " WHERE " + VOL_NODE_NAME + " = ? AND " +
                    VOL_RES_NAME  + " = ? AND " +
                    VOL_ID        + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final StateFlagsPersistence<VolumeData> flagPersistenceDriver;
    private final SingleColumnDatabaseDriver<VolumeData, StorPool> storPoolDriver;

    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeDataGenericDbDriver(
        @SystemContext AccessContext privCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        flagPersistenceDriver = new VolFlagsPersistence();
        storPoolDriver = new StorPoolDriver();
    }

    public Map<VolumeData, Volume.InitMaps> loadAll(
        Map<Pair<NodeName, ResourceName>, ? extends Resource> rscMap,
        Map<Pair<ResourceName, VolumeNumber>, ? extends VolumeDefinition> vlmDfnMap,
        Map<Pair<NodeName, StorPoolName>, ? extends StorPool> storPoolMap
    )
        throws SQLException
    {
        Map<VolumeData, Volume.InitMaps> vlmMap = new TreeMap<>();
        errorReporter.logTrace("Loading all Volumes");
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    try
                    {
                        NodeName nodeName = new NodeName(resultSet.getString(VOL_NODE_NAME));
                        ResourceName rscName = new ResourceName(resultSet.getString(VOL_RES_NAME));
                        StorPoolName storPoolName = new StorPoolName(resultSet.getString(VOL_STOR_POOL));
                        VolumeNumber vlmNr = new VolumeNumber(resultSet.getInt(VOL_ID));

                        Pair<VolumeData, Volume.InitMaps> pair = restoreVlm(
                            resultSet,
                            rscMap.get(new Pair<>(nodeName, rscName)),
                            vlmDfnMap.get(new Pair<>(rscName, vlmNr)),
                            storPoolMap.get(new Pair<>(nodeName, storPoolName))
                        );
                        vlmMap.put(
                            pair.objA,
                            pair.objB
                        );
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            "Invalid name restored from database: " + exc.invalidName,
                            exc
                        );
                    }
                    catch (ValueOutOfRangeException exc)
                    {
                        throw new ImplementationError(
                            "Invalid volume number restored from database: "  + resultSet.getInt(VOL_ID),
                            exc
                        );
                    }
                }
            }
        }
        errorReporter.logTrace("Loaded %d Volumes", vlmMap.size());
        return vlmMap;
    }

    private Pair<VolumeData, Volume.InitMaps> restoreVlm(
        ResultSet resultSet,
        Resource rsc,
        VolumeDefinition vlmDfn,
        StorPool storPool
    )
        throws SQLException
    {
        Map<Volume.Key, VolumeConnection> vlmConnsMap = new TreeMap<>();

        // XXX restore layerstack
        List<DeviceLayerKind> layerStack = new ArrayList<>();

        VolumeData vlm = new VolumeData(
            java.util.UUID.fromString(resultSet.getString(VOL_UUID)),
            rsc,
            vlmDfn,
            storPool,
            null,
            null,
            resultSet.getLong(VOL_FLAGS),
            this,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            vlmConnsMap,
            layerStack
        );

        return new Pair<>(
            vlm,
            new VolumeInitMaps(
                vlmConnsMap
            )
        );
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(VolumeData vol) throws SQLException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            errorReporter.logTrace("Creating Volume %s", getId(vol));

            stmt.setString(1, vol.getUuid().toString());
            stmt.setString(2, vol.getResource().getAssignedNode().getName().value);
            stmt.setString(3, vol.getResourceDefinition().getName().value);
            stmt.setInt(4, vol.getVolumeDefinition().getVolumeNumber().value);
            stmt.setString(5, vol.getStorPool(dbCtx).getName().value);
            stmt.setLong(6, vol.getFlags().getFlagsBits(dbCtx));
            stmt.executeUpdate();

            errorReporter.logTrace("Volume created %s", getId(vol));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(VolumeData volume) throws SQLException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
        {
            errorReporter.logTrace("Deleting Volume %s", getId(volume));

            stmt.setString(1, volume.getResource().getAssignedNode().getName().value);
            stmt.setString(2, volume.getResource().getDefinition().getName().value);
            stmt.setInt(3, volume.getVolumeDefinition().getVolumeNumber().value);
            stmt.executeUpdate();

            errorReporter.logTrace("Volume deleted %s", getId(volume));
        }
    }

    @Override
    public StateFlagsPersistence<VolumeData> getStateFlagsPersistence()
    {
        return flagPersistenceDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<VolumeData, StorPool> getStorPoolDriver()
    {
        return storPoolDriver;
    }

    private String getId(VolumeData volume)
    {
        return getVolId(
            volume.getResource().getAssignedNode().getName().displayValue,
            volume.getResource().getDefinition().getName().displayValue,
            volume.getVolumeDefinition().getVolumeNumber()
        );
    }

    private String getVolId(String nodeName, String resName, VolumeNumber volNum)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + " VolNum=" + volNum.value + ")";
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private class VolFlagsPersistence implements StateFlagsPersistence<VolumeData>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(VolumeData volume, long flags)
            throws SQLException
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_FLAGS))
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        VlmFlags.class,
                        volume.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        VlmFlags.class,
                        flags
                    ),
                    ", "
                );

                errorReporter.logTrace(
                    "Updating Volume's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(volume)
                );

                stmt.setLong(1, flags);
                stmt.setString(2, volume.getResource().getAssignedNode().getName().value);
                stmt.setString(3, volume.getResource().getDefinition().getName().value);
                stmt.setInt(4, volume.getVolumeDefinition().getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "Volume's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(volume)
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
    }

    private class StorPoolDriver implements SingleColumnDatabaseDriver<VolumeData, StorPool>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(VolumeData parent, StorPool element) throws SQLException
        {
            try
            {
                errorReporter.logTrace("Updating Volume's StorPool from [%s] to [%s] %s",
                    parent.getStorPool(dbCtx).getName().displayValue,
                    element.getName().displayValue,
                    getId(parent)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_STOR_POOL))
                {
                    stmt.setString(1, element.getName().value);
                    stmt.setString(2, parent.getResource().getAssignedNode().getName().value);
                    stmt.setString(3, parent.getResource().getDefinition().getName().value);
                    stmt.setInt(4, parent.getVolumeDefinition().getVolumeNumber().value);

                    stmt.executeUpdate();
                }
                errorReporter.logTrace("Volume's StorPool updated from [%s] to [%s] %s",
                    parent.getStorPool(dbCtx).getName().displayValue,
                    element.getName().displayValue,
                    getId(parent)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                GenericDbDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    public static class VolPrimaryKey
    {
        private Resource resource;
        private VolumeDefinition volDfn;

        public VolPrimaryKey(Resource rscRef, VolumeDefinition volDfnRef)
        {
            resource = rscRef;
            volDfn = volDfnRef;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((resource == null) ? 0 : resource.hashCode());
            result = prime * result + ((volDfn == null) ? 0 : volDfn.hashCode());
            return result;
        }

        @Override
        // Single exit point exception: Automatically generated code
        @SuppressWarnings("DescendantToken")
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            VolPrimaryKey other = (VolPrimaryKey) obj;
            if (resource == null)
            {
                if (other.resource != null)
                {
                    return false;
                }
            }
            else
            {
                if (!resource.equals(other.resource))
                {
                    return false;
                }
            }
            if (volDfn == null)
            {
                if (other.volDfn != null)
                {
                    return false;
                }
            }
            else
            {
                if (!volDfn.equals(other.volDfn))
                {
                    return false;
                }
            }
            return true;
        }
    }

    private class VolumeInitMaps implements Volume.InitMaps
    {
        private final Map<Volume.Key, VolumeConnection> vlmConnMap;

        VolumeInitMaps(Map<Volume.Key, VolumeConnection> vlmConnMapRef)
        {
            vlmConnMap = vlmConnMapRef;
        }

        @Override
        public Map<Volume.Key, VolumeConnection> getVolumeConnections()
        {
            return vlmConnMap;
        }
    }
}

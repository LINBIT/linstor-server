package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class VolumeGenericDbDriver implements VolumeDatabaseDriver
{
    private static final String TBL_VOL = DbConstants.TBL_VOLUMES;
    private static final String VOL_UUID = DbConstants.UUID;
    private static final String VOL_NODE_NAME = DbConstants.NODE_NAME;
    private static final String VOL_RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String VOL_ID = DbConstants.VLM_NR;
    private static final String VOL_FLAGS = DbConstants.VLM_FLAGS;
    private static final String[] VOL_FIELDS = {
        VOL_UUID,
        VOL_NODE_NAME,
        VOL_RES_NAME,
        VOL_ID,
        VOL_FLAGS
    };

    private static final String SELECT_ALL =
        " SELECT " + StringUtils.join(", ", VOL_FIELDS) +
        " FROM " + TBL_VOL;

    private static final String INSERT =
        " INSERT INTO " + TBL_VOL +
        " (" + StringUtils.join(", ", VOL_FIELDS) + ")" +
        " VALUES (" + StringUtils.repeat("?", ", ", VOL_FIELDS.length) + ")";

    private static final String UPDATE_FLAGS =
        " UPDATE " + TBL_VOL +
        " SET " + VOL_FLAGS + " = ? " +
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

    private final StateFlagsPersistence<Volume> flagPersistenceDriver;

    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public VolumeGenericDbDriver(
        @SystemContext AccessContext privCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        flagPersistenceDriver = new VolFlagsPersistence();
    }

    public Map<Volume, Volume.InitMaps> loadAll(
        Map<Pair<NodeName, ResourceName>, ? extends Resource> rscMap,
        Map<Pair<ResourceName, VolumeNumber>, ? extends VolumeDefinition> vlmDfnMap
    )
        throws DatabaseException
    {
        Map<Volume, Volume.InitMaps> vlmMap = new TreeMap<>();
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
                        VolumeNumber vlmNr = new VolumeNumber(resultSet.getInt(VOL_ID));

                        Pair<Volume, Volume.InitMaps> pair = restoreVlm(
                            resultSet,
                            rscMap.get(new Pair<>(nodeName, rscName)),
                            vlmDfnMap.get(new Pair<>(rscName, vlmNr))
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
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d Volumes", vlmMap.size());
        return vlmMap;
    }

    private Pair<Volume, Volume.InitMaps> restoreVlm(
        ResultSet resultSet,
        Resource rsc,
        VolumeDefinition vlmDfn
    )
        throws DatabaseException
    {
        Map<Volume.Key, VolumeConnection> vlmConnsMap = new TreeMap<>();

        Volume vlm;

        try {
            vlm = new Volume(
                java.util.UUID.fromString(resultSet.getString(VOL_UUID)),
                rsc,
                vlmDfn,
                resultSet.getLong(VOL_FLAGS),
                this,
                vlmConnsMap,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }

        return new Pair<>(
            vlm,
            new VolumeInitMaps(
                vlmConnsMap
            )
        );
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(Volume vol) throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            errorReporter.logTrace("Creating Volume %s", getId(vol));

            stmt.setString(1, vol.getUuid().toString());
            stmt.setString(2, vol.getAbsResource().getNode().getName().value);
            stmt.setString(3, vol.getResourceDefinition().getName().value);
            stmt.setInt(4, vol.getVolumeDefinition().getVolumeNumber().value);
            stmt.setLong(5, vol.getFlags().getFlagsBits(dbCtx));
            stmt.executeUpdate();

            errorReporter.logTrace("Volume created %s", getId(vol));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(Volume volume) throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
        {
            errorReporter.logTrace("Deleting Volume %s", getId(volume));

            stmt.setString(1, volume.getAbsResource().getNode().getName().value);
            stmt.setString(2, volume.getAbsResource().getDefinition().getName().value);
            stmt.setInt(3, volume.getVolumeDefinition().getVolumeNumber().value);
            stmt.executeUpdate();

            errorReporter.logTrace("Volume deleted %s", getId(volume));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public StateFlagsPersistence<Volume> getStateFlagsPersistence()
    {
        return flagPersistenceDriver;
    }

    private String getId(Volume volume)
    {
        return getVolId(
            volume.getAbsResource().getNode().getName().displayValue,
            volume.getAbsResource().getDefinition().getName().displayValue,
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

    private class VolFlagsPersistence implements StateFlagsPersistence<Volume>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(Volume volume, long flags)
            throws DatabaseException
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_FLAGS))
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        Volume.Flags.class,
                        volume.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        Volume.Flags.class,
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
                stmt.setString(2, volume.getAbsResource().getNode().getName().value);
                stmt.setString(3, volume.getAbsResource().getDefinition().getName().value);
                stmt.setInt(4, volume.getVolumeDefinition().getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "Volume's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(volume)
                );
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
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

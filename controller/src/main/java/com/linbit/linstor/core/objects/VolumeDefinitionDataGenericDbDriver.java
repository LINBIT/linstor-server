package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.core.objects.VolumeDefinition.InitMaps;
import com.linbit.linstor.core.objects.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
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
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class VolumeDefinitionDataGenericDbDriver implements VolumeDefinitionDataDatabaseDriver
{
    private static final String TBL_VOL_DFN = DbConstants.TBL_VOLUME_DEFINITIONS;
    private static final String VD_UUID = DbConstants.UUID;
    private static final String VD_RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String VD_ID = DbConstants.VLM_NR;
    private static final String VD_SIZE = DbConstants.VLM_SIZE;
    private static final String VD_FLAGS = DbConstants.VLM_FLAGS;
    private static final String[] VOL_DFN_FIELDS = {
        VD_UUID,
        VD_RES_NAME,
        VD_ID,
        VD_SIZE,
        VD_FLAGS
    };

    private static final String VD_SELECT_ALL =
        " SELECT " + StringUtils.join(", ", VOL_DFN_FIELDS) +
        " FROM " + TBL_VOL_DFN;

    private static final String VD_INSERT =
        " INSERT INTO " + TBL_VOL_DFN +
        " (" + StringUtils.join(", ", VOL_DFN_FIELDS) + ")" +
        "VALUES ( " + StringUtils.repeat("?", ", ", VOL_DFN_FIELDS.length) + " )";

    private static final String VD_UPDATE_FLAGS =
        " UPDATE " + TBL_VOL_DFN +
        " SET " + VD_FLAGS + " = ? " +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";

    private static final String VD_UPDATE_SIZE =
        " UPDATE " + TBL_VOL_DFN +
        " SET " + VD_SIZE + " = ? " +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";

    private static final String VD_DELETE =
        " DELETE FROM " + TBL_VOL_DFN +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final PropsContainerFactory propsContainerFactory;

    private final FlagDriver flagsDriver;
    private final SizeDriver sizeDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeDefinitionDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        flagsDriver = new FlagDriver();
        sizeDriver = new SizeDriver();
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(VolumeDefinitionData volumeDefinition) throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(VD_INSERT))
        {
            errorReporter.logTrace("Creating VolumeDefinition %s", getId(volumeDefinition));

            stmt.setString(1, volumeDefinition.getUuid().toString());
            stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
            stmt.setInt(3, volumeDefinition.getVolumeNumber().value);
            stmt.setLong(4, volumeDefinition.getVolumeSize(dbCtx));
            stmt.setLong(5, volumeDefinition.getFlags().getFlagsBits(dbCtx));

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeDefinition created %s", getId(volumeDefinition));
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

    private Pair<VolumeDefinitionData, InitMaps> restoreVolumeDefinition(
        ResultSet resultSet,
        ResourceDefinition resDfn,
        VolumeNumber volNr
    )
        throws DatabaseException
    {
        Pair<VolumeDefinitionData, InitMaps> retPair;
        try
        {
            errorReporter.logTrace("Restoring VolumeDefinition %s", getId(resDfn, volNr));
            VolumeDefinitionData vlmDfn = null;
            try
            {
                Map<String, Volume> vlmMap = new TreeMap<>();

                vlmDfn = new VolumeDefinitionData(
                    java.util.UUID.fromString(resultSet.getString(VD_UUID)),
                    resDfn,
                    volNr,
                    resultSet.getLong(VD_SIZE),
                    resultSet.getLong(VD_FLAGS),
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    vlmMap,
                    new TreeMap<>()
                );
                retPair = new Pair<>(vlmDfn, new VolumeDefinitionInitMaps(vlmMap));

                errorReporter.logTrace("VolumeDefinition %s created during restore", getId(vlmDfn));
                // restore references
            } catch (MdException mdExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "A VolumeSize of a stored VolumeDefinition in table %s could not be restored. " +
                            "(ResourceName=%s, VolumeNumber=%d, invalid VolumeSize=%d)",
                        TBL_VOL_DFN,
                        resDfn.getName().value,
                        volNr.value,
                        resultSet.getLong(VD_SIZE)
                    ),
                    mdExc
                );
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return retPair;
    }


    public Map<VolumeDefinitionData, VolumeDefinition.InitMaps> loadAll(
        Map<ResourceName, ? extends ResourceDefinition> rscDfnMap
    )
        throws DatabaseException
    {
        errorReporter.logTrace("Loading all VolumeDefinitions");
        Map<VolumeDefinitionData, VolumeDefinition.InitMaps> ret = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(VD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    ResourceName rscName;
                    VolumeNumber volNr;
                    try
                    {
                        rscName = new ResourceName(resultSet.getString(VD_RES_NAME));
                        volNr = new VolumeNumber(resultSet.getInt(VD_ID));
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            TBL_VOL_DFN + " contained invalid resource name: " + exc.invalidName,
                            exc
                        );
                    }
                    catch (ValueOutOfRangeException valueOutOfRangeExc)
                    {
                        throw new LinStorDBRuntimeException(
                            String.format(
                                "A VolumeNumber of a stored VolumeDefinition in table %s could not be restored. " +
                                    "(ResourceName=%s, invalid VolumeNumber=%d)",
                                TBL_VOL_DFN,
                                resultSet.getString(VD_RES_NAME),
                                resultSet.getLong(VD_ID)
                            ),
                            valueOutOfRangeExc
                        );
                    }

                    Pair<VolumeDefinitionData, InitMaps> pair = restoreVolumeDefinition(
                        resultSet,
                        rscDfnMap.get(rscName),
                        volNr
                    );

                    ret.put(pair.objA, pair.objB);

                    errorReporter.logTrace("VolumeDefinition created %s", getId(pair.objA));
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return ret;
    }

    @Override
    public void delete(VolumeDefinitionData volumeDefinition) throws DatabaseException
    {
        errorReporter.logTrace("Deleting VolumeDefinition %s", getId(volumeDefinition));
        try (PreparedStatement stmt = getConnection().prepareStatement(VD_DELETE))
        {
            stmt.setString(1, volumeDefinition.getResourceDefinition().getName().value);
            stmt.setInt(2, volumeDefinition.getVolumeNumber().value);
            stmt.executeUpdate();
            errorReporter.logTrace("VolumeDefinition deleted %s", getId(volumeDefinition));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver()
    {
        return sizeDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(VolumeDefinitionData volDfn)
    {
        return getId(
            volDfn.getResourceDefinition(),
            volDfn.getVolumeNumber()
        );
    }

    private String getId(ResourceDefinition resourceDefinition, VolumeNumber volumeNumber)
    {
        return getId(
            resourceDefinition.getName().displayValue,
            volumeNumber
        );
    }

    private String getId(String resName, VolumeNumber volNum)
    {
        return "(ResName=" + resName + " VolNum=" + volNum.value + ")";
    }

    private class FlagDriver implements StateFlagsPersistence<VolumeDefinitionData>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void persist(VolumeDefinitionData volumeDefinition, long flags)
            throws DatabaseException
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(VD_UPDATE_FLAGS))
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        VlmDfnFlags.class,
                        volumeDefinition.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        VlmDfnFlags.class,
                        flags
                    ),
                    ", "
                );
                errorReporter.logTrace(
                    "Updating VolumeDefinition's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(volumeDefinition)
                );
                stmt.setLong(1, flags);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(volumeDefinition)
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

    private class SizeDriver implements SingleColumnDatabaseDriver<VolumeDefinitionData, Long>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(VolumeDefinitionData volumeDefinition, Long size)
            throws DatabaseException
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(VD_UPDATE_SIZE))
            {
                errorReporter.logTrace(
                    "Updating VolumeDefinition's Size from [%d] to [%d] %s",
                    volumeDefinition.getVolumeSize(dbCtx),
                    size,
                    getId(volumeDefinition)
                );

                stmt.setLong(1, size);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's Size updated from [%d] to [%d] %s",
                    volumeDefinition.getVolumeSize(dbCtx),
                    size,
                    getId(volumeDefinition)
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

    private class VolumeDefinitionInitMaps implements VolumeDefinition.InitMaps
    {
        private final Map<String, Volume> vlmMap;

        VolumeDefinitionInitMaps(Map<String, Volume> vlmMapRef)
        {
            vlmMap = vlmMapRef;
        }

        @Override
        public Map<String, Volume> getVlmMap()
        {
            return vlmMap;
        }
    }
}

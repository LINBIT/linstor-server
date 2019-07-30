package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Triple;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME_SUFFIX;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.SF_VLM_ODATA;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_SWORDFISH_VOLUME_DEFINITIONS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

@Singleton
@SuppressWarnings("checkstyle:magicnumber")
public class SwordfishLayerGenericDbDriver implements SwordfishLayerDatabaseDriver
{
    private static final String VLM_DFN_ALL_FIELDS =
        RESOURCE_NAME + ", " + RESOURCE_NAME_SUFFIX + ", " + VLM_NR + ", " + SF_VLM_ODATA;

    private static final String SELECT_ALL =
        " SELECT " + VLM_DFN_ALL_FIELDS +
        " FROM " + TBL_LAYER_SWORDFISH_VOLUME_DEFINITIONS;

    private static final String INSERT_VLM_DFN =
        " INSERT INTO " + TBL_LAYER_SWORDFISH_VOLUME_DEFINITIONS +
        " ( " + VLM_DFN_ALL_FIELDS + " ) " +
        " VALUES ( " + VLM_DFN_ALL_FIELDS.replaceAll("[^, ]+", "?") + " )";

    private static final String UPDATE_VLM_DFN_ODATA =
        " UPDATE " + TBL_LAYER_SWORDFISH_VOLUME_DEFINITIONS +
        " SET " +   SF_VLM_ODATA         + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    VLM_NR               + " = ?";

    private static final String DELETE_VLM_DFN =
        " DELETE FROM " + TBL_LAYER_SWORDFISH_VOLUME_DEFINITIONS +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    VLM_NR               + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final VlmDfnOdataDriver vlmDfnOdataDriver;

    private Map<Triple<String, String, Integer>, SfVlmDfnData> sfVlmDfnInfoCache;

    @Inject
    public SwordfishLayerGenericDbDriver(
        @SystemContext AccessContext accCtxRef,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtxRef;
        errorReporter = errorReporterRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        vlmDfnOdataDriver = new VlmDfnOdataDriver();
    }

    public void loadLayerData(Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef) throws DatabaseException
    {
        sfVlmDfnInfoCache = new HashMap<>();

        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    String rscNameStr = resultSet.getString(RESOURCE_NAME);
                    String rscNameSuffix = resultSet.getString(RESOURCE_NAME_SUFFIX);
                    int vlmNrInt = resultSet.getInt(VLM_NR);
                    String sfVlmOdataId = resultSet.getString(SF_VLM_ODATA);
                    Triple<String, String, Integer> key = new Triple<>(
                        rscNameStr,
                        rscNameSuffix,
                        vlmNrInt
                    );
                    SfVlmDfnData sfVlmDfnData;

                    ResourceName rscName;
                    try
                    {
                        rscName = new ResourceName(rscNameStr);
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new LinStorDBRuntimeException(
                            "Failed to restore stored resourceName [" + rscNameStr + "]"
                        );
                    }
                    ResourceDefinition rscDfn = tmpRscDfnMapRef.get(rscName);
                    if (rscDfn == null)
                    {
                        throw new LinStorDBRuntimeException(
                            "Loaded swordfish volume definition data for non existent resource definition '" +
                                rscNameStr + "'"
                        );
                    }
                    VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(dbCtx, new VolumeNumber(vlmNrInt));
                    sfVlmDfnData = new SfVlmDfnData(
                        vlmDfn,
                        sfVlmOdataId,
                        rscNameSuffix,
                        this,
                        transObjFactory,
                        transMgrProvider
                    );
                    sfVlmDfnInfoCache.put(key, sfVlmDfnData);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void clearLoadAllCache()
    {
        sfVlmDfnInfoCache.clear();
        sfVlmDfnInfoCache = null;
    }

    public VlmProviderObject load(
        Volume vlmRef,
        StorageRscData rscDataRef,
        DeviceProviderKind kindRef,
        StorPool storPoolRef,
        StorageLayerDatabaseDriver storageLayerDbDriverRef
    )
    {
        SfVlmDfnData sfVlmDfnData = sfVlmDfnInfoCache.get(
            new Triple<>(
                rscDataRef.getResourceName().displayValue,
                rscDataRef.getResourceNameSuffix(),
                vlmRef.getVolumeDefinition().getVolumeNumber().value
            )
        );
        if (sfVlmDfnData == null)
        {
            throw new ImplementationError(
                String.format(
                    "No cached entry for swordfish volume definition! RscId: %d, VlmNr: %d",
                    rscDataRef.getRscLayerId(),
                    vlmRef.getVolumeDefinition().getVolumeNumber().value
                )
            );
        }

        VlmProviderObject vlmData = null;
        switch (kindRef)
        {
            case SWORDFISH_INITIATOR:
                vlmData = new SfInitiatorData(
                    rscDataRef,
                    vlmRef,
                    sfVlmDfnData,
                    storPoolRef,
                    storageLayerDbDriverRef,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case SWORDFISH_TARGET:
                vlmData = new SfTargetData(
                    vlmRef,
                    rscDataRef,
                    sfVlmDfnData,
                    storPoolRef,
                    storageLayerDbDriverRef,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case DISKLESS:
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            case LVM:
            case LVM_THIN:
            case ZFS:
            case ZFS_THIN:
            case FILE:
            case FILE_THIN:
            default:
                throw new LinStorDBRuntimeException("Invalid DeviceProviderKind: '" + kindRef + "'");
        }
        return vlmData;
    }


    @Override
    public void persist(SfVlmDfnData vlmDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Creating SfVlmDfnData %s", getId(vlmDfnData));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM_DFN))
        {
            stmt.setString(1, vlmDfnData.getVolumeDefinition().getResourceDefinition().getName().value);
            stmt.setString(2, vlmDfnData.getRscNameSuffix());
            stmt.setInt(3, vlmDfnData.getVolumeDefinition().getVolumeNumber().value);
            if (vlmDfnData.getVlmOdata() == null)
            {
                stmt.setNull(4, Types.VARCHAR);
            }
            else
            {
                stmt.setString(3, vlmDfnData.getVlmOdata());
            }

            stmt.executeUpdate();
            errorReporter.logTrace("SfVlmDfnData created %s", getId(vlmDfnData));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(SfVlmDfnData vlmDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting SfVlmDfnData %s", getId(vlmDfnData));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM_DFN))
        {
            stmt.setString(1, vlmDfnData.getVolumeDefinition().getResourceDefinition().getName().value);
            stmt.setString(2, vlmDfnData.getRscNameSuffix());
            stmt.setInt(3, vlmDfnData.getVolumeDefinition().getVolumeNumber().value);

            stmt.executeUpdate();
            errorReporter.logTrace("SfVlmDfnData deleted %s", getId(vlmDfnData));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void persist(SfInitiatorData sfInitiatorDataRef)
    {
        // no-op - there is no special database table.
        // this method only exists if SfInitiatorData will get a database table in future.
    }

    @Override
    public void delete(SfInitiatorData sfInitiatorDataRef)
    {
        // no-op - there is no special database table.
        // this method only exists if SfInitiatorData will get a database table in future.
    }

    @Override
    public void persist(SfTargetData sfTargetDataRef)
    {
        // no-op - there is no special database table.
        // this method only exists if SfTargetData will get a database table in future.
    }

    @Override
    public void delete(SfTargetData sfTargetDataRef)
    {
        // no-op - there is no special database table.
        // this method only exists if SfTargetData will get a database table in future.
    }

    @Override
    public SingleColumnDatabaseDriver<SfVlmDfnData, String> getVlmDfnOdataDriver()
    {
        return vlmDfnOdataDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(SfVlmDfnData vlmDfnData)
    {
        return "(SuffResName=" + vlmDfnData.getSuffixedResourceName() +
            ", VlmNr=" + vlmDfnData.getVolumeDefinition().getVolumeNumber().value +
            ")";
    }

    private class VlmDfnOdataDriver implements SingleColumnDatabaseDriver<SfVlmDfnData, String>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(SfVlmDfnData vlmDfnData, String oData)
            throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating SfVlmDfnData's oData from [%s] to [%s] %s",
                vlmDfnData.getVlmOdata(),
                oData,
                getId(vlmDfnData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_VLM_DFN_ODATA))
            {
                stmt.setString(1, oData);
                stmt.setString(2, vlmDfnData.getVolumeDefinition().getResourceDefinition().getName().value);
                stmt.setString(3, vlmDfnData.getRscNameSuffix());
                stmt.setInt(4, vlmDfnData.getVolumeDefinition().getVolumeNumber().value);
                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "SfVlmDfnData's oData updated from [%d] to [%d] %s",
                vlmDfnData.getVlmOdata(),
                oData,
                getId(vlmDfnData)
            );
        }
    }
}

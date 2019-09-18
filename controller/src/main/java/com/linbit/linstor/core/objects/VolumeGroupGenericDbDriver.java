package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_GROUP_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_VOLUME_GROUPS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.UUID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VLM_NR;

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

@Singleton
public class VolumeGroupGenericDbDriver implements VolumeGroupDatabaseDriver
{
    private static final String[] VLM_GRP_FIELDS =
    {
        UUID,
        RESOURCE_GROUP_NAME,
        VLM_NR
    };

    private static final String SELECT_ALL_VLM_GRPS =
        " SELECT " + StringUtils.join(", ", VLM_GRP_FIELDS) +
        " FROM " + TBL_VOLUME_GROUPS;

    private static final String INSERT =
        " INSERT INTO " + TBL_VOLUME_GROUPS +
        " ( " + StringUtils.join(", ", VLM_GRP_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ", ", VLM_GRP_FIELDS.length) + " )";

    private static final String DELETE =
        " DELETE FROM " + TBL_VOLUME_GROUPS +
        " WHERE " + RESOURCE_GROUP_NAME + " = ? AND " +
                    VLM_NR              + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public VolumeGroupGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(VolumeGroup vlmGrp) throws DatabaseException
    {
        errorReporter.logTrace("Creating VolumeGroup %s", getId(vlmGrp));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            stmt.setString(1, vlmGrp.getUuid().toString());
            stmt.setString(2, vlmGrp.getResourceGroup().getName().value);
            stmt.setInt(3, vlmGrp.getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeGroup created %s", getId(vlmGrp));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    public List<VolumeGroup> loadAll(
        Map<ResourceGroupName, ? extends ResourceGroup> rscGrpMap
    )
        throws DatabaseException
    {
        errorReporter.logTrace("Loading all volumeGroups");
        List<VolumeGroup> vlmGrpList = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VLM_GRPS))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    ResourceGroupName rscGrpName;
                    VolumeNumber vlmNr;

                    try
                    {
                        rscGrpName = new ResourceGroupName(resultSet.getString(RESOURCE_GROUP_NAME));
                        vlmNr = new VolumeNumber(resultSet.getInt(VLM_NR));
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            TBL_VOLUME_GROUPS + " contained invalid resource group name: " + exc.invalidName,
                            exc
                        );
                    }
                    catch (ValueOutOfRangeException valueOutOfRangeExc)
                    {
                        throw new LinStorDBRuntimeException(
                            String.format(
                                "A VolumeNumber of a stored VolumeGroup in table %s could not be restored. " +
                                    "(ResourceGroupName=%s, invalid VolumeNumber=%d)",
                                TBL_VOLUME_GROUPS,
                                resultSet.getString(RESOURCE_GROUP_NAME),
                                resultSet.getLong(VLM_NR)
                            ),
                            valueOutOfRangeExc
                        );
                    }

                    vlmGrpList.add(
                        restoreVlmGrp(
                            resultSet,
                            rscGrpMap.get(rscGrpName),
                            vlmNr
                        )
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d VolumeGroup", rscGrpMap.size());
        return vlmGrpList;
    }

    private VolumeGroup restoreVlmGrp(
        ResultSet resultSetRef,
        ResourceGroup resourceGroupRef,
        VolumeNumber vlmNrRef
    )
        throws DatabaseException
    {
        VolumeGroup volumeGroup;
        try
        {
            volumeGroup = new VolumeGroup(
                java.util.UUID.fromString(resultSetRef.getString(UUID)),
                resourceGroupRef,
                vlmNrRef,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
        return volumeGroup;
    }

    @Override
    public void delete(VolumeGroup vlmGrp) throws DatabaseException
    {
        errorReporter.logTrace("Deleting VolumeGroup %s", getId(vlmGrp));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
        {
            stmt.setString(1, vlmGrp.getResourceGroup().getName().value);
            stmt.setInt(2, vlmGrp.getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeGroup deleted %s", getId(vlmGrp));
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

    private String getId(VolumeGroup vlmGrp)
    {
        return getId(vlmGrp.getResourceGroup().getName().displayValue, vlmGrp.getVolumeNumber().value);
    }

    private String getId(String resGrpName, int vlmNr)
    {
        return "(RscGrpName=" + resGrpName + ", VlmNr=" + vlmNr + ")";
    }
}

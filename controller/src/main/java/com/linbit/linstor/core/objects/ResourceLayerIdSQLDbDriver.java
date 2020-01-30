package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgrSQL;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_KIND;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_PARENT_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_SUFFIX;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_SUSPENDED;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_RESOURCE_IDS;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Provider;

@Singleton
public class ResourceLayerIdSQLDbDriver implements ResourceLayerIdCtrlDatabaseDriver
{
    private static final String PK_FIELDS =
        LAYER_RESOURCE_ID;
    private static final String ALL_FIELDS =
        PK_FIELDS + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " + SNAPSHOT_NAME + ", " +
            LAYER_RESOURCE_PARENT_ID + ", " + LAYER_RESOURCE_KIND + ", " + LAYER_RESOURCE_SUFFIX + ", " +
            LAYER_RESOURCE_SUSPENDED;

    private static final String SELECT_ALL =
        " SELECT " + ALL_FIELDS +
        " FROM " + TBL_LAYER_RESOURCE_IDS;

    private static final String INSERT =
        " INSERT INTO " + TBL_LAYER_RESOURCE_IDS +
        " ( " + ALL_FIELDS + " ) " +
        " VALUES ( " + ALL_FIELDS.replaceAll("[^, ]+", "?") + " )";

    private static final String DELETE =
        " DELETE FROM " + TBL_LAYER_RESOURCE_IDS +
        " WHERE " + PK_FIELDS.replaceAll(",", " = ? AND") + " = ?";

    private static final String UPDATE_PARENT_ID =
        " UPDATE " + TBL_LAYER_RESOURCE_IDS +
        " SET " + LAYER_RESOURCE_PARENT_ID + " = ? " +
        " WHERE " + PK_FIELDS.replaceAll(",", " = ? AND") + " = ?";
    private static final String UPDATE_SUSPENDED = " UPDATE " + TBL_LAYER_RESOURCE_IDS +
        " SET " + LAYER_RESOURCE_SUSPENDED + " = ? " +
        " WHERE " + PK_FIELDS.replaceAll(",", " = ? AND") + " = ?";

    private final ErrorReporter errorReporter;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final SingleColumnDatabaseDriver<AbsRscData<?, VlmProviderObject<?>>, AbsRscLayerObject<?>> parentDriver;
    private final SingleColumnDatabaseDriver<AbsRscData<?, VlmProviderObject<?>>, Boolean> suspendDriver;

    @Inject
    public ResourceLayerIdSQLDbDriver(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;
        parentDriver = new ParentDriver();
        suspendDriver = new SuspendDriver();
    }

    @Override
    public List<RscLayerInfo> loadAllResourceIds() throws DatabaseException
    {
        List<RscLayerInfo> ret = new ArrayList<>();

        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Integer parentId = resultSet.getInt(LAYER_RESOURCE_PARENT_ID);
                    if (resultSet.wasNull())
                    {
                        parentId = null;
                    }

                    String snapNameStr = resultSet.getString(SNAPSHOT_NAME);

                    SnapshotName snapshotName;
                    if (snapNameStr == null || snapNameStr.isEmpty())
                    {
                        snapshotName = null;
                    }
                    else
                    {
                        snapshotName = new SnapshotName(snapNameStr);
                    }
                    RscLayerInfo rscInfoData = new RscLayerInfo(
                        new NodeName(resultSet.getString(NODE_NAME)),
                        new ResourceName(resultSet.getString(RESOURCE_NAME)),
                        snapshotName,
                        resultSet.getInt(LAYER_RESOURCE_ID),
                        parentId,
                        DeviceLayerKind.valueOf(resultSet.getString(LAYER_RESOURCE_KIND)),
                        resultSet.getString(LAYER_RESOURCE_SUFFIX),
                        resultSet.getBoolean(LAYER_RESOURCE_SUSPENDED)
                    );

                    ret.add(rscInfoData);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Unrestorable name loaded from the database", exc);
        }
        return ret;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void persist(AbsRscLayerObject<?> rscData) throws DatabaseException
    {
        errorReporter.logTrace("Creating LayerResourceId %s", getId(rscData));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            AbsResource<?> absRsc = rscData.getAbsResource();
            stmt.setInt(1, rscData.getRscLayerId());
            stmt.setString(2, absRsc.getNode().getName().value);
            stmt.setString(3, rscData.getResourceName().value);
            if (absRsc instanceof Resource)
            {
                stmt.setString(4, DFLT_SNAP_NAME_FOR_RSC);
            }
            else
            {
                stmt.setString(4, ((Snapshot) absRsc).getSnapshotName().value);
            }
            if (rscData.getParent() != null)
            {
                stmt.setInt(5, rscData.getParent().getRscLayerId());
            }
            else
            {
                stmt.setNull(5, Types.INTEGER);
            }
            stmt.setString(6, rscData.getLayerKind().name());
            stmt.setString(7, rscData.getResourceNameSuffix());
            stmt.setBoolean(8, rscData.getSuspendIo());

            stmt.executeUpdate();
            errorReporter.logTrace("LayerResourceId created %s", getId(rscData));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(AbsRscLayerObject<?> rscData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting LayerResourceId %s", getId(rscData));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
        {
            AbsResource<?> absRsc = rscData.getAbsResource();
            stmt.setInt(1, rscData.getRscLayerId());

            stmt.executeUpdate();
            errorReporter.logTrace("LayerResourceId deleting %s", getId(rscData));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <
        RSC extends AbsResource<RSC>,
        VLM_TYPE extends VlmProviderObject<RSC>>
        SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> getParentDriver()
    {
        // sorry for this dirty hack :(

        // Java does not allow to cast <?> to <T> for good reasons, but here those reasons are irrelevant as the
        // SingleColumnDatatbaseDriver does not use anything of that T. The reason it still needs to be declared as T
        // is the usage of the implementation of the layer-specific resource data.
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>>) ((Object) parentDriver);
    }

    @Override
    public <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>> SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean> getSuspendDriver()
    {
        // TODO Auto-generated method stub
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean>) ((Object) suspendDriver);
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(AbsRscLayerObject<?> rscData)
    {
        return rscData.getLayerKind().name() +
            " (id: " + rscData.getRscLayerId() +
            ", rscName: " + rscData.getSuffixedResourceName() +
            (rscData.getAbsResource() instanceof Snapshot
                ? ", SnapshotName: " + (((Snapshot) rscData.getAbsResource()).getSnapshotName().displayValue)
                : DFLT_SNAP_NAME_FOR_RSC) +
            ", parent: " + (rscData.getParent() == null ? "-" : rscData.getParent().getRscLayerId()) + ")";
    }

    private String getId(AbsRscData<?, ?> rscData)
    {
        AbsResource<?> absRsc = rscData.getAbsResource();
        String snapName = absRsc instanceof Resource ? DFLT_SNAP_NAME_FOR_RSC
            : ((Snapshot) absRsc).getSnapshotName().displayValue;
        return "(" + rscData.getClass().getSimpleName() + ", " +
            "Node: " + rscData.getAbsResource().getNode().getName().displayValue +
            ", Resource: " + rscData.getResourceName().displayValue +
            (rscData.getAbsResource() instanceof Snapshot
                ? ", SnapshotName: " + (((Snapshot) rscData.getAbsResource()).getSnapshotName().displayValue)
                : DFLT_SNAP_NAME_FOR_RSC) +
            ", Layer Id: " + rscData.getRscLayerId() + ")";
    }

    private class ParentDriver
        implements SingleColumnDatabaseDriver<AbsRscData<?, VlmProviderObject<?>>, AbsRscLayerObject<?>>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(AbsRscData<?, VlmProviderObject<?>> rscData, AbsRscLayerObject<?> newParentData)
            throws DatabaseException
        {
            AbsRscLayerObject<?> oldParentData = rscData.getParent();
            errorReporter.logTrace(
                "Updating %s's parent resource id from [%d] to [%d] %s",
                rscData.getClass().getSimpleName(),
                oldParentData == null ? null : oldParentData.getRscLayerId(),
                newParentData == null ? null : newParentData.getRscLayerId(),
                getId(rscData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_PARENT_ID))
            {
                if (newParentData == null)
                {
                    stmt.setNull(1, Types.INTEGER);
                }
                else
                {
                    stmt.setInt(1, newParentData.getRscLayerId());
                }

                stmt.setInt(2, rscData.getRscLayerId());

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "%s's parent resource id updated from [%d] to [%d] %s",
                rscData.getClass().getSimpleName(),
                oldParentData == null ? null : oldParentData.getRscLayerId(),
                newParentData == null ? null : newParentData.getRscLayerId(),
                getId(rscData)
            );
        }
    }

    private class SuspendDriver implements SingleColumnDatabaseDriver<AbsRscData<?, VlmProviderObject<?>>, Boolean>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(AbsRscData<?, VlmProviderObject<?>> rscData, Boolean newSuspend)
            throws DatabaseException
        {
            boolean oldSuspend = rscData.getSuspendIo();
            errorReporter.logTrace(
                "Updating %s's suspend io from [%s] to [%s] %s",
                rscData.getClass().getSimpleName(),
                Boolean.toString(oldSuspend),
                newSuspend,
                getId(rscData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_SUSPENDED))
            {
                stmt.setBoolean(1, newSuspend);
                stmt.setInt(2, rscData.getRscLayerId());

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "%s's suspended io updated from [%s] to [%s] %s",
                rscData.getClass().getSimpleName(),
                Boolean.toString(oldSuspend),
                newSuspend.toString(),
                getId(rscData)
            );
        }
    }
}

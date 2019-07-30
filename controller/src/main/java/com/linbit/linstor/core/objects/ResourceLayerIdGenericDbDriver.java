package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_KIND;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_PARENT_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_SUFFIX;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME;
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
public class ResourceLayerIdGenericDbDriver implements ResourceLayerIdDatabaseDriver
{
    private static final String PK_FIELDS =
        NODE_NAME + ", " + RESOURCE_NAME + ", " + LAYER_RESOURCE_ID;
    private static final String ALL_FIELDS =
        PK_FIELDS + ", " + LAYER_RESOURCE_PARENT_ID + ", " +
        LAYER_RESOURCE_KIND + ", " + LAYER_RESOURCE_SUFFIX;

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

    private final ErrorReporter errorReporter;
    private final Provider<TransactionMgr> transMgrProvider;

    private final SingleColumnDatabaseDriver<AbsRscData<VlmProviderObject>, RscLayerObject> parentDriver;

    @Inject
    public ResourceLayerIdGenericDbDriver(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;
        parentDriver = new ParentDriver();
    }

    public List<RscLayerInfoData> loadAllResourceIds() throws DatabaseException
    {
        List<RscLayerInfoData> ret = new ArrayList<>();

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
                    RscLayerInfoData rscInfoData = new RscLayerInfoData(
                        new NodeName(resultSet.getString(NODE_NAME)),
                        new ResourceName(resultSet.getString(RESOURCE_NAME)),
                        resultSet.getInt(LAYER_RESOURCE_ID),
                        parentId,
                        DeviceLayerKind.valueOf(resultSet.getString(LAYER_RESOURCE_KIND)),
                        resultSet.getString(LAYER_RESOURCE_SUFFIX)
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
    public void persist(RscLayerObject rscData) throws DatabaseException
    {
        errorReporter.logTrace("Creating LayerResourceId %s", getId(rscData));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            stmt.setString(1, rscData.getResource().getAssignedNode().getName().value);
            stmt.setString(2, rscData.getResourceName().value);
            stmt.setInt(3, rscData.getRscLayerId());
            if (rscData.getParent() != null)
            {
                stmt.setInt(4, rscData.getParent().getRscLayerId());
            }
            else
            {
                stmt.setNull(4, Types.INTEGER);
            }
            stmt.setString(5, rscData.getLayerKind().name());
            stmt.setString(6, rscData.getResourceNameSuffix());

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
    public void delete(RscLayerObject rscData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting LayerResourceId %s", getId(rscData));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
        {
            stmt.setString(1, rscData.getResource().getAssignedNode().getName().value);
            stmt.setString(2, rscData.getResourceName().value);
            stmt.setInt(3, rscData.getRscLayerId());

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
    public <T extends VlmProviderObject> SingleColumnDatabaseDriver<AbsRscData<T>, RscLayerObject> getParentDriver()
    {
        // sorry for this dirty hack :(

        // Java does not allow to cast <?> to <T> for good reasons, but here those reasons are irrelevant as the
        // SingleColumnDatatbaseDriver does not use anything of that T. The reason it still needs to be declared as T
        // is the usage of the implementation of the layer-specific resource data.
        return (SingleColumnDatabaseDriver<AbsRscData<T>, RscLayerObject>) ((Object) parentDriver);
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(RscLayerObject rscData)
    {
        return rscData.getLayerKind().name() +
            " (id: " + rscData.getRscLayerId() +
            ", rscName: " + rscData.getSuffixedResourceName() +
            ", parent: " + (rscData.getParent() == null ? "-" : rscData.getParent().getRscLayerId()) + ")";
    }

    public static class RscLayerInfoData implements RscLayerInfo
    {
        public final NodeName nodeName;
        public final ResourceName resourceName;
        public final int id;
        public final Integer parentId;
        public final DeviceLayerKind kind;
        public final String rscSuffix;

        private RscLayerInfoData(
            NodeName nodeNameRef,
            ResourceName resourceNameRef,
            int idRef,
            Integer parentIdRef,
            DeviceLayerKind kindRef,
            String rscSuffixRef
        )
        {
            super();
            nodeName = nodeNameRef;
            resourceName = resourceNameRef;
            id = idRef;
            parentId = parentIdRef;
            kind = kindRef;
            rscSuffix = rscSuffixRef;
        }

        @Override
        public NodeName getNodeName()
        {
            return nodeName;
        }

        @Override
        public ResourceName getResourceName()
        {
            return resourceName;
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public Integer getParentId()
        {
            return parentId;
        }

        @Override
        public DeviceLayerKind getKind()
        {
            return kind;
        }

        @Override
        public String getRscSuffix()
        {
            return rscSuffix;
        }
    }

    private String getId(AbsRscData<?> rscData)
    {
        return "(" + rscData.getClass().getSimpleName() + ", " +
            "Node: " + rscData.getResource().getAssignedNode().getName().displayValue +
            ", Resource: " + rscData.getResourceName().displayValue +
            ", Layer Id: " + rscData.getRscLayerId() + ")";
    }

    private class ParentDriver implements SingleColumnDatabaseDriver<AbsRscData<VlmProviderObject>, RscLayerObject>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(AbsRscData<VlmProviderObject> rscData, RscLayerObject newParentData) throws DatabaseException
        {
            RscLayerObject oldParentData = rscData.getParent();
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

                stmt.setString(2, rscData.getResource().getAssignedNode().getName().value);
                stmt.setString(3, rscData.getResourceName().value);
                stmt.setInt(4, rscData.getRscLayerId());

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
}

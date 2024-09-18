package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_KIND;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_PARENT_ID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_SUFFIX;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_SUSPENDED;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.SNAPSHOT_NAME;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

@Singleton
public class LayerResourceIdDbDriver extends AbsDatabaseDriver<AbsRscLayerObject<?>, Void, Void>
    implements LayerResourceIdCtrlDatabaseDriver
{
    private static final String IMPL_ERR_TEXT = "This method must not be used after db-loading";

    private final SingleColumnDatabaseDriver<AbsRscLayerObject<?>, AbsRscLayerObject<?>> parentDriver;

    private final SingleColumnDatabaseDriver<AbsRscLayerObject<?>, Boolean> suspendDriver;

    @Inject
    public LayerResourceIdDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.LAYER_RESOURCE_IDS, dbEngineRef, objProtFactoryRef);

        setColumnSetter(LAYER_RESOURCE_ID, rlo -> rlo.getRscLayerId());
        setColumnSetter(LAYER_RESOURCE_KIND, rlo -> rlo.getLayerKind().name());
        setColumnSetter(
            LAYER_RESOURCE_PARENT_ID,
            rlo -> rlo.getParent() == null ? null : rlo.getParent().getRscLayerId()
        );
        setColumnSetter(LAYER_RESOURCE_SUFFIX, rlo -> rlo.getResourceNameSuffix());
        setColumnSetter(LAYER_RESOURCE_SUSPENDED, rlo -> rlo.getShouldSuspendIo());
        setColumnSetter(NODE_NAME, rlo -> rlo.getAbsResource().getNode().getName().value);
        setColumnSetter(RESOURCE_NAME, rlo -> rlo.getAbsResource().getResourceDefinition().getName().value);
        setColumnSetter(SNAPSHOT_NAME, rlo ->
        {
            String snapName;
            AbsResource<?> absResource = rlo.getAbsResource();
            if (absResource instanceof Resource)
            {
                snapName = ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
            }
            else
            {
                snapName = ((Snapshot) absResource).getSnapshotName().value;
            }
            return snapName;
        });

        parentDriver = generateSingleColumnDriver(
            LAYER_RESOURCE_PARENT_ID,
            rlo -> rlo.getParent() == null ? "null" : rlo.getParent().getRscLayerId() + "",
            rlo -> rlo.getParent() == null ? null : rlo.getParent().getRscLayerId()
        );

        suspendDriver = generateSingleColumnDriver(
            LAYER_RESOURCE_SUSPENDED,
            rlo -> Boolean.toString(rlo.getShouldSuspendIo()),
            Function.identity()
        );
    }

    @Override
    protected Pair<AbsRscLayerObject<?>, Void> load(RawParameters raw, Void parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final int rli;
        final Integer parentId;
        final boolean suspended;

        switch (getDbType())
        {
            case SQL:
            case K8S_CRD:
                rli = raw.get(LAYER_RESOURCE_ID);
                parentId = raw.get(LAYER_RESOURCE_PARENT_ID);
                suspended = raw.get(LAYER_RESOURCE_SUSPENDED);
                break;
            case ETCD:
                rli = Integer.parseInt(raw.get(LAYER_RESOURCE_ID));

                String parentIdStr = raw.get(LAYER_RESOURCE_PARENT_ID);
                parentId = parentIdStr == null ? null : Integer.parseInt(parentIdStr);

                String suspendedStr = raw.get(LAYER_RESOURCE_SUSPENDED);
                suspended = suspendedStr == null ? null : Boolean.parseBoolean(suspendedStr);

                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        AbsRscLayerObject<?> ret = new ResourceLayerIdLoadingPojo(
            rli,
            raw.build(LAYER_RESOURCE_KIND, DeviceLayerKind.class),
            parentId,
            raw.get(LAYER_RESOURCE_SUFFIX),
            suspended,
            raw.build(NODE_NAME, NodeName::new),
            raw.build(RESOURCE_NAME, ResourceName::new),
            raw.<String, SnapshotName, InvalidNameException>build(SNAPSHOT_NAME, snapNameStr ->
            {
                SnapshotName snapName = null;
                if (snapNameStr != null && !snapNameStr.equals(ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC))
                {
                    snapName = new SnapshotName(snapNameStr);
                }
                return snapName;
            })
        );
        return new Pair<>(ret, null);
    }

    @Override
    public <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>
        SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> getParentDriver()
    {
        // sorry for this dirty hack :(

        // Java does not allow to cast <?> to <T> for good reasons, but here those reasons are irrelevant as the
        // SingleColumnDatatbaseDriver does not use anything of that T. The reason it still needs to be declared as T
        // is the usage of the implementation of the layer-specific resource data.
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>>) ((Object) parentDriver);
    }

    @Override
    public <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>
        SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean> getSuspendDriver()
    {
        // sorry for this dirty hack :(

        // Java does not allow to cast <?> to <T> for good reasons, but here those reasons are irrelevant as the
        // SingleColumnDatatbaseDriver does not use anything of that T. The reason it still needs to be declared as T
        // is the usage of the implementation of the layer-specific resource data.
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean>) ((Object) suspendDriver);
    }

    @Override
    public String getId(AbsRscLayerObject<?> rlo)
    {
        String snapName = rlo.getSnapName() == null ?
            ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC :
            rlo.getSnapName().displayValue;
        return "(" + rlo.getClass().getSimpleName() + ", " +
            "Node: " + rlo.getNodeName().displayValue +
            ", Resource: " + rlo.getSuffixedResourceName() +
            (snapName.equals(ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC) ?
                ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC :
                ", SnapshotName: " + snapName) +
            ", Layer Id: " + rlo.getRscLayerId() + ")";
    }


    private static class ResourceLayerIdLoadingPojo extends ParentResourceLayerIdLoadingPojo
    {
        private final DeviceLayerKind layerKind;
        private final ParentResourceLayerIdLoadingPojo parent;
        private final String rscNameSuffix;
        private final boolean isSuspended;
        private final NodeName nodeName;
        private final ResourceName rscName;
        private final SnapshotName snapName;

        ResourceLayerIdLoadingPojo(
            int rscLayerIdRef,
            DeviceLayerKind layerKindRef,
            Integer parentRscLayerIdRef,
            String rscNameSuffixRef,
            boolean isSuspendedRef,
            NodeName nodeNameRef,
            ResourceName rscNameRef,
            SnapshotName snapNameRef
        )
        {
            super(rscLayerIdRef);
            parent = parentRscLayerIdRef == null ? null : new ParentResourceLayerIdLoadingPojo(parentRscLayerIdRef);
            layerKind = layerKindRef;
            rscNameSuffix = rscNameSuffixRef;
            isSuspended = isSuspendedRef;
            nodeName = nodeNameRef;
            rscName = rscNameRef;
            snapName = snapNameRef;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return layerKind;
        }

        @Override
        public AbsRscLayerObject<Resource> getParent()
        {
            return parent;
        }

        @Override
        public String getResourceNameSuffix()
        {
            return rscNameSuffix;
        }

        @Override
        public ResourceName getResourceName()
        {
            return rscName;
        }

        @Override
        public NodeName getNodeName()
        {
            return nodeName;
        }

        @Override
        public @Nullable SnapshotName getSnapName()
        {
            return snapName;
        }

        @Override
        public boolean getShouldSuspendIo()
        {
            return isSuspended;
        }
    }

    private static class ParentResourceLayerIdLoadingPojo
        implements AbsRscLayerObject<Resource>
    {
        private final int rscLayerId;

        ParentResourceLayerIdLoadingPojo(int rscLayerIdRef)
        {
            rscLayerId = rscLayerIdRef;
        }

        @Override
        public int getRscLayerId()
        {
            return rscLayerId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rscLayerId);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof ParentResourceLayerIdLoadingPojo))
            {
                return false;
            }
            ParentResourceLayerIdLoadingPojo other = (ParentResourceLayerIdLoadingPojo) obj;
            return rscLayerId == other.rscLayerId;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public AbsRscLayerObject<Resource> getParent()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public String getResourceNameSuffix()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void setConnection(TransactionMgr transMgrRef) throws ImplementationError
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public boolean hasTransMgr()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public boolean isDirtyWithoutTransMgr()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public boolean isDirty()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void rollback()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void commit()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void setParent(AbsRscLayerObject<Resource> parentRscLayerObjectRef) throws DatabaseException
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public Set<AbsRscLayerObject<Resource>> getChildren()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public Resource getAbsResource()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public RscDfnLayerObject getRscDfnLayerObject()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public Map<VolumeNumber, ? extends VlmProviderObject<Resource>> getVlmLayerObjects()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public RscLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void delete(AccessContext accCtxRef) throws AccessDeniedException, DatabaseException
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void remove(AccessContext accCtxRef, VolumeNumber vlmNrRef)
            throws AccessDeniedException, DatabaseException
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public boolean checkFileSystem()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void disableCheckFileSystem()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void setShouldSuspendIo(boolean suspendRef) throws DatabaseException
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public boolean getShouldSuspendIo()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public boolean addIgnoreReasons(LayerIgnoreReason... ignoreReasonsRef) throws DatabaseException
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public Set<LayerIgnoreReason> getIgnoreReasons()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void clearIgnoreReasons() throws DatabaseException
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public boolean exists()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public void setIsSuspended(boolean suspendRef) throws DatabaseException
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }

        @Override
        public Boolean isSuspended()
        {
            throw new ImplementationError(IMPL_ERR_TEXT);
        }
    }
}

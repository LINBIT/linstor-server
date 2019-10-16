package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgrETCD;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_KIND;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_PARENT_ID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.LAYER_RESOURCE_SUFFIX;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerResourceIds.SNAPSHOT_NAME;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class ResourceLayerETCDDriver extends BaseEtcdDriver implements ResourceLayerIdCtrlDatabaseDriver
{
    private final ErrorReporter errorReporter;
    private final SingleColumnDatabaseDriver<AbsRscData<?, VlmProviderObject<?>>, AbsRscLayerObject<?>> parentDriver;

    @Inject
    public ResourceLayerETCDDriver(ErrorReporter errorReporterRef, Provider<TransactionMgrETCD> transMgrProviderRef)
    {
        super(transMgrProviderRef);
        errorReporter = errorReporterRef;

        parentDriver = (rscData, newParentData) ->
        {
            AbsRscLayerObject<?> oldParentData = rscData.getParent();
            errorReporter.logTrace(
                "Updating %s's parent resource id from [%d] to [%d] %s",
                rscData.getClass().getSimpleName(),
                oldParentData == null ? null : oldParentData.getRscLayerId(),
                newParentData == null ? null : newParentData.getRscLayerId(),
                getId(rscData)
            );

            if (newParentData == null)
            {
                namespace(EtcdUtils.buildKey(LAYER_RESOURCE_PARENT_ID, Integer.toString(rscData.getRscLayerId())))
                    .delete(false);
            }
            else
            {
                namespace(GeneratedDatabaseTables.LAYER_RESOURCE_IDS, Integer.toString(rscData.getRscLayerId()))
                    .put(LAYER_RESOURCE_PARENT_ID, Integer.toString(rscData.getParent().getRscLayerId()));
            }
        };
    }

    @Override
    public List<RscLayerInfo> loadAllResourceIds() throws DatabaseException
    {
        List<RscLayerInfo> ret = new ArrayList<>();

        Map<String, String> allIds = namespace(GeneratedDatabaseTables.LAYER_RESOURCE_IDS).get(true);
        Set<String> pks = EtcdUtils.getComposedPkList(allIds);
        try
        {
            for (String layerId : pks)
            {
                String parentIdStr = allIds.get(EtcdUtils.buildKey(LAYER_RESOURCE_PARENT_ID, layerId));
                String snapNameStr = allIds.get(EtcdUtils.buildKey(SNAPSHOT_NAME, layerId));
                SnapshotName snapName = snapNameStr == null ? null : new SnapshotName(snapNameStr);

                ret.add(
                    new RscLayerInfo(
                        new NodeName(allIds.get(EtcdUtils.buildKey(NODE_NAME, layerId))),
                        new ResourceName(allIds.get(EtcdUtils.buildKey(RESOURCE_NAME, layerId))),
                        snapName,
                        Integer.parseInt(layerId),
                        parentIdStr != null && !parentIdStr.isEmpty() ? Integer.parseInt(parentIdStr) : null,
                        DeviceLayerKind.valueOf(allIds.get(EtcdUtils.buildKey(LAYER_RESOURCE_KIND, layerId))),
                        allIds.get(EtcdUtils.buildKey(LAYER_RESOURCE_SUFFIX, layerId))
                    )
                );
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Unrestorable name loaded from the database", exc);
        }
        return ret;
    }

    @Override
    public void persist(AbsRscLayerObject<?> rscData) throws DatabaseException
    {
        errorReporter.logTrace("Creating LayerResourceId %s", getId(rscData));
        FluentLinstorTransaction namespace = namespace(
            GeneratedDatabaseTables.LAYER_RESOURCE_IDS, Integer.toString(rscData.getRscLayerId())
        );
        AbsResource<?> absRsc = rscData.getAbsResource();
        namespace
            .put(NODE_NAME, absRsc.getNode().getName().value)
            .put(RESOURCE_NAME, rscData.getResourceName().value)
            .put(LAYER_RESOURCE_KIND, rscData.getLayerKind().name())
            .put(LAYER_RESOURCE_SUFFIX, rscData.getResourceNameSuffix());
        if (rscData.getParent() != null)
        {
            namespace.put(LAYER_RESOURCE_PARENT_ID, Integer.toString(rscData.getParent().getRscLayerId()));
        }
        if (absRsc instanceof Snapshot)
        {
            namespace
                .put(SNAPSHOT_NAME, ((Snapshot) absRsc).getSnapshotName().value);
        }
    }

    @Override
    public void delete(AbsRscLayerObject<?> rscData) throws DatabaseException
    {
        namespace(GeneratedDatabaseTables.LAYER_RESOURCE_IDS, Integer.toString(rscData.getRscLayerId()))
            .delete(true);
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

    public static String getId(AbsRscLayerObject<?> rscData)
    {
        return rscData.getLayerKind().name() +
            " (id: " + rscData.getRscLayerId() +
            ", rscName: " + rscData.getSuffixedResourceName() +
            ", parent: " + (rscData.getParent() == null ? "-" : rscData.getParent().getRscLayerId()) + ")";
    }

}

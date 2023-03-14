package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.LayerResourceIdsSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class ResourceLayerIdK8sCrdDriver implements ResourceLayerIdCtrlDatabaseDriver
{
    private final ErrorReporter errorReporter;
    private final SingleColumnDatabaseDriver<AbsRscData<?, VlmProviderObject<?>>, ?> updateDriver;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    @Inject
    public ResourceLayerIdK8sCrdDriver(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;

        updateDriver = (rscData, ignored) -> insertOrUpdate(rscData, false);
    }

    @Override
    public List<RscLayerInfo> loadAllResourceIds() throws DatabaseException
    {
        List<RscLayerInfo> ret = new ArrayList<>();

        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        Map<String, LayerResourceIdsSpec> map = tx.getSpec(GeneratedDatabaseTables.LAYER_RESOURCE_IDS);
        try
        {
            for (LayerResourceIdsSpec lriSpec : map.values())
            {
                ret.add(
                    new RscLayerInfo(
                        new NodeName(lriSpec.nodeName),
                        new ResourceName(lriSpec.resourceName),
                        lriSpec.snapshotName == null || lriSpec.snapshotName.isEmpty() ?
                            null :
                            new SnapshotName(lriSpec.snapshotName),
                        lriSpec.layerResourceId,
                        lriSpec.layerResourceParentId == null ? null : lriSpec.layerResourceParentId,
                        DeviceLayerKind.valueOf(lriSpec.layerResourceKind),
                        lriSpec.layerResourceSuffix,
                        lriSpec.layerResourceSuspended
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
        insertOrUpdate(rscData, true);
    }

    private void insertOrUpdate(AbsRscLayerObject<?> rscData, boolean isNew) throws DatabaseException
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        AbsResource<?> absRsc = rscData.getAbsResource();

        GenCrdCurrent.LayerResourceIds val = GenCrdCurrent.createLayerResourceIds(
            rscData.getRscLayerId(),
            absRsc.getNode().getName().value,
            rscData.getResourceName().value,
            absRsc instanceof Snapshot ? ((Snapshot) absRsc).getSnapshotName().value : null,
            rscData.getLayerKind().name(),
            rscData.getParent() != null ? rscData.getParent().getRscLayerId() : null,
            rscData.getResourceNameSuffix(),
            rscData.getShouldSuspendIo()
        );

        tx.createOrReplace(GeneratedDatabaseTables.LAYER_RESOURCE_IDS, val, isNew);
    }

    @Override
    public void delete(AbsRscLayerObject<?> rscData) throws DatabaseException
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_RESOURCE_IDS,
            GenCrdCurrent.createLayerResourceIds(
                rscData.getRscLayerId(),
                null,
                null,
                null,
                null,
                null,
                null,
                false
            )
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>
    SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> getParentDriver()
    {
        // sorry for this dirty hack :(

        // Java does not allow to cast <?> to <T> for good reasons, but here those reasons are irrelevant as the
        // SingleColumnDatatbaseDriver does not use anything of that T. The reason it still needs to be declared as T
        // is the usage of the implementation of the layer-specific resource data.
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>>) ((Object) updateDriver);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean> getSuspendDriver()
    {
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean>) ((Object) updateDriver);
    }
}

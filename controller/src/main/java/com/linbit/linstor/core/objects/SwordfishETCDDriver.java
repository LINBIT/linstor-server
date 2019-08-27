package com.linbit.linstor.core.objects;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerSwordfishVolumeDefinitions;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.transaction.TransactionMgrETCD;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

// TODO: rework this to use the AbsDatabaseDriver
// that also means to split this driver into single-table drivers
@Singleton
public class SwordfishETCDDriver extends BaseEtcdDriver implements SwordfishLayerDatabaseDriver
{
    private static final String DUMMY_VALUE = "intentional-null-value";

    private final ErrorReporter errorReporter;

    private final SingleColumnDatabaseDriver<SfVlmDfnData, String> vlmDfnOdataDriver;

    @Inject
    public SwordfishETCDDriver(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        errorReporter = errorReporterRef;

        vlmDfnOdataDriver = (vlmDfnData, oData) ->
        {
            errorReporter.logTrace(
                "Updating SfVlmDfnData's oData from [%s] to [%s] %s",
                vlmDfnData.getVlmOdata(),
                oData,
                getId(vlmDfnData)
            );
            namespace(
                GeneratedDatabaseTables.LAYER_SWORDFISH_VOLUME_DEFINITIONS,
                vlmDfnData.getVolumeDefinition().getResourceDefinition().getName().value,
                vlmDfnData.getRscNameSuffix(),
                Integer.toString(vlmDfnData.getVolumeDefinition().getVolumeNumber().value)
            )
                .put(LayerSwordfishVolumeDefinitions.VLM_ODATA, oData);
        };
    }

    @Override
    public SingleColumnDatabaseDriver<SfVlmDfnData, String> getVlmDfnOdataDriver()
    {
        return vlmDfnOdataDriver;
    }

    @Override
    public void persist(SfVlmDfnData vlmDfnData) throws DatabaseException
    {
        String odata = vlmDfnData.getVlmOdata();
        if (odata == null)
        {
            odata = DUMMY_VALUE;
        }
        namespace(
            GeneratedDatabaseTables.LAYER_SWORDFISH_VOLUME_DEFINITIONS,
            vlmDfnData.getVolumeDefinition().getResourceDefinition().getName().value,
            vlmDfnData.getRscNameSuffix(),
            Integer.toString(vlmDfnData.getVolumeDefinition().getVolumeNumber().value)
        )
            .put(LayerSwordfishVolumeDefinitions.VLM_ODATA, odata);
    }

    @Override
    public void delete(SfVlmDfnData vlmDfnData) throws DatabaseException
    {
        namespace(
            GeneratedDatabaseTables.LAYER_SWORDFISH_VOLUME_DEFINITIONS,
            vlmDfnData.getVolumeDefinition().getResourceDefinition().getName().value,
            vlmDfnData.getRscNameSuffix(),
            Integer.toString(vlmDfnData.getVolumeDefinition().getVolumeNumber().value)
        )
            .delete(true);
    }

    @Override
    public void persist(SfInitiatorData sfInitiatorDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
      // this method only exists if SfInitiatorData will get a database table in future.
    }

    @Override
    public void delete(SfInitiatorData sfInitiatorDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if SfInitiatorData will get a database table in future.
    }

    @Override
    public void persist(SfTargetData sfTargetDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if SfTargetData will get a database table in future.
    }

    @Override
    public void delete(SfTargetData sfTargetDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if SfTargetData will get a database table in future.
    }

    private String getId(SfVlmDfnData vlmDfnData)
    {
        return "(SuffResName=" + vlmDfnData.getSuffixedResourceName() +
            ", VlmNr=" + vlmDfnData.getVolumeDefinition().getVolumeNumber().value +
            ")";
    }
}

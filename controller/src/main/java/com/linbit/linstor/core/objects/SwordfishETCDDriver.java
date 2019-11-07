package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerSwordfishVolumeDefinitions;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgrETCD;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// TODO: rework this to use the AbsDatabaseDriver
// that also means to split this driver into single-table drivers
@Singleton
public class SwordfishETCDDriver extends BaseEtcdDriver implements SwordfishLayerCtrlDatabaseDriver
{
    private static final String DUMMY_VALUE = "intentional-null-value";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transObjFactory;

    private final SingleColumnDatabaseDriver<SfVlmDfnData, String> vlmDfnOdataDriver;
    private Map<Triple<String, String, Integer>, SfVlmDfnData> sfVlmDfnInfoCache;



    @Inject
    public SwordfishETCDDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        transObjFactory = transObjFactoryRef;

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

    public void clearLoadAllCache()
    {
        sfVlmDfnInfoCache.clear();
        sfVlmDfnInfoCache = null;
    }

    public void loadLayerData(Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef)
    {
        sfVlmDfnInfoCache = new HashMap<>();

        Map<String, String> allSfVlmDfnMap = namespace(
            GeneratedDatabaseTables.LAYER_SWORDFISH_VOLUME_DEFINITIONS
        )
            .get(true);
        Set<String> composedPksSet = EtcdUtils.getComposedPkList(allSfVlmDfnMap);
        try
        {
            for (String composedPk : composedPksSet)
            {
                String[] pks = composedPk.split(EtcdUtils.PK_DELIMITER);

                String rscNameStr = pks[LayerSwordfishVolumeDefinitions.RESOURCE_NAME.getIndex()];
                String rscNameSuffix = pks[LayerSwordfishVolumeDefinitions.RESOURCE_NAME_SUFFIX.getIndex()];
                int vlmNrInt = Integer.parseInt(pks[LayerSwordfishVolumeDefinitions.VLM_NR.getIndex()]);

                String odata = allSfVlmDfnMap.get(
                    EtcdUtils.buildKey(LayerSwordfishVolumeDefinitions.VLM_ODATA, pks)
                );
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
                    odata,
                    rscNameSuffix,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                sfVlmDfnInfoCache.put(key, sfVlmDfnData);
            }
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

    public VlmProviderObject load(
        Volume vlmRef,
        StorageRscData rscDataRef,
        DeviceProviderKind kindRef,
        StorPool storPoolRef,
        StorageLayerETCDDriver storageLayerETCDDriverRef
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
                    storageLayerETCDDriverRef,
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
                    storageLayerETCDDriverRef,
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
            case SPDK:
            default:
                throw new LinStorDBRuntimeException("Invalid DeviceProviderKind: '" + kindRef + "'");
        }
        return vlmData;
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

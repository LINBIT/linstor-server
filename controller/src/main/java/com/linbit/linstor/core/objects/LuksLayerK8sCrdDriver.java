package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.db.utils.K8sCrdUtils;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.LayerLuksVolumesSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class LuksLayerK8sCrdDriver implements LuksLayerCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;

    private final SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> vlmPwDriver;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    private final HashMap<Integer, HashMap<Integer, GenCrdCurrent.LayerLuksVolumesSpec>> vlmSpecCache;

    @Inject
    public LuksLayerK8sCrdDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        vlmPwDriver = (luksVlm, ignored) -> update(luksVlm);

        vlmSpecCache = new HashMap<>();
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public void fetchForLoadAll()
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        Map<String, GenCrdCurrent.LayerLuksVolumesSpec> luksVlmSpecMap = tx.get(
            GeneratedDatabaseTables.LAYER_LUKS_VOLUMES
        );
        for (GenCrdCurrent.LayerLuksVolumesSpec luksVlmSpec : luksVlmSpecMap.values())
        {
            HashMap<Integer, GenCrdCurrent.LayerLuksVolumesSpec> map = vlmSpecCache.get(luksVlmSpec.layerResourceId);
            if (map == null)
            {
                map = new HashMap<>();
                vlmSpecCache.put(luksVlmSpec.layerResourceId, map);
            }
            map.put(luksVlmSpec.vlmNr, luksVlmSpec);
        }
    }

    @Override
    public void clearLoadAllCache()
    {
        vlmSpecCache.clear();
    }

    @Override
    public <RSC extends AbsResource<RSC>> Pair<LuksRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
        throws DatabaseException
    {
        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();

        Map<VolumeNumber, LuksVlmData<RSC>> vlmDataMap = new TreeMap<>();

        LuksRscData<RSC> rscData = new LuksRscData<>(
            id,
            absRsc,
            rscSuffixRef,
            parentRef,
            children,
            vlmDataMap,
            this,
            transObjFactory,
            transMgrProvider
        );

        int vlmNrInt = -1;
        try
        {
            Map<Integer, LayerLuksVolumesSpec> vlmSpecsMap = K8sCrdUtils.getCheckedVlmMap(
                dbCtx,
                absRsc,
                vlmSpecCache,
                id
            );
            for (Entry<Integer, GenCrdCurrent.LayerLuksVolumesSpec> entry : vlmSpecsMap.entrySet())
            {
                LayerLuksVolumesSpec luksVlmSpec = entry.getValue();
                vlmNrInt = entry.getKey();
                VolumeNumber vlmNr;
                vlmNr = new VolumeNumber(vlmNrInt);
                vlmDataMap.put(
                    vlmNr,
                    new LuksVlmData<>(
                        absRsc.getVolume(vlmNr),
                        rscData,
                        Base64.decode(luksVlmSpec.encryptedPassword),
                        this,
                        transObjFactory,
                        transMgrProvider
                    )
                );
            }
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored volume number " + vlmNrInt
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("ApiContext does not have enough privileges");
        }
        return new Pair<>(rscData, children);
    }

    @Override
    public void persist(LuksRscData<?> luksRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if LuksRscData will get a database table in future.
    }

    @Override
    public void delete(LuksRscData<?> luksRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if LuksRscData will get a database table in future.
    }

    @Override
    public void persist(LuksVlmData<?> luksVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating LuksVlmData %s", getId(luksVlmDataRef));
        update(luksVlmDataRef);
    }

    private void update(LuksVlmData<?> luksVlmData)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.update(
            GeneratedDatabaseTables.LAYER_LUKS_VOLUMES,
            GenCrdCurrent.createLayerLuksVolumes(
                luksVlmData.getRscLayerId(),
                luksVlmData.getVlmNr().value,
                Base64.encode(luksVlmData.getEncryptedKey())
            )
        );
    }

    @Override
    public void delete(LuksVlmData<?> luksVlmDataRef) throws DatabaseException
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        errorReporter.logTrace("Deleting LuksVlmData %s", getId(luksVlmDataRef));
        tx.delete(
            GeneratedDatabaseTables.LAYER_LUKS_VOLUMES,
            GenCrdCurrent.createLayerBcacheVolumes(
                luksVlmDataRef.getRscLayerId(),
                luksVlmDataRef.getVlmNr().value,
                null,
                null,
                null
            )
        );
    }

    @Override
    public SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> getVlmEncryptedPasswordDriver()
    {
        return vlmPwDriver;
    }
}

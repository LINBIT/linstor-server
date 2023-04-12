package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Singleton
public class NvmeLayerK8sCrdDriver implements NvmeLayerCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final LayerResourceIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    @Inject
    public NvmeLayerK8sCrdDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        LayerResourceIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    // nothing to fetch for the moment. NvmeVolumes are just created without stored data

    /**
     * Fully loads a {@link NvmeRscData} object including its {@link NvmeVlmData}
     *
     * @param parentRef
     * @return a {@link Pair}, where the first object is the actual NvmeRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     */
    @Override
    public <RSC extends AbsResource<RSC>> Pair<NvmeRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
    {
        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Map<VolumeNumber, NvmeVlmData<RSC>> vlmMap = new TreeMap<>();
        NvmeRscData<RSC> nvmeRscData = new NvmeRscData<>(
            id,
            absRsc,
            parentRef,
            children,
            vlmMap,
            rscSuffixRef,
            this,
            transObjFactory,
            transMgrProvider
        );
        for (AbsVolume<RSC> vlm : absRsc.streamVolumes().collect(Collectors.toList()))
        {
            vlmMap.put(
                vlm.getVolumeNumber(),
                new NvmeVlmData<>(vlm, nvmeRscData, transObjFactory, transMgrProvider)
            );
        }
        return new Pair<>(nvmeRscData, children);
    }

    @Override
    public void create(NvmeRscData<?> nvmeRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if NvmeRscData will get a database table in future.
    }

    @Override
    public void delete(NvmeRscData<?> nvmeRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if NvmeRscData will get a database table in future.
    }

    @Override
    public void persist(NvmeVlmData<?> nvmeVlmDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if NvmeVlmData will get a database table in future.
    }

    @Override
    public void delete(NvmeVlmData<?> nvmeVlmDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if NvmeVlmData will get a database table in future.
    }
}

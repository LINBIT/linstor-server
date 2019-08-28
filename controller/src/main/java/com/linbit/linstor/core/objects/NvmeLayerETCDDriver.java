package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.transaction.TransactionMgrETCD;
import com.linbit.linstor.transaction.TransactionObjectFactory;
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
public class NvmeLayerETCDDriver extends BaseEtcdDriver implements NvmeLayerDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public NvmeLayerETCDDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    /**
     * Fully loads a {@link NvmeRscData} object including its {@link NvmeVlmData}
     *
     * @param parentRef
     * @return a {@link Pair}, where the first object is the actual NvmeRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     */
    @Override
    public Pair<NvmeRscData, Set<RscLayerObject>> load(
        Resource rsc,
        int id,
        String rscSuffixRef,
        RscLayerObject parentRef
    )
    {
        Set<RscLayerObject> children = new HashSet<>();
        Map<VolumeNumber, NvmeVlmData> vlmMap = new TreeMap<>();
        NvmeRscData nvmeRscData = new NvmeRscData(
            id,
            rsc,
            parentRef,
            children,
            vlmMap,
            rscSuffixRef,
            this,
            transObjFactory,
            transMgrProvider
        );
        for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
        {
            vlmMap.put(
                vlm.getVolumeDefinition().getVolumeNumber(),
                new NvmeVlmData(vlm, nvmeRscData, transObjFactory, transMgrProvider)
            );
        }
        return new Pair<>(nvmeRscData, children);
    }

    @Override
    public void create(NvmeRscData drbdRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if NvmeRscData will get a database table in future.
    }

    @Override
    public void delete(NvmeRscData drbdRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if NvmeRscData will get a database table in future.
    }

    @Override
    public void persist(NvmeVlmData drbdVlmDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if NvmeVlmData will get a database table in future.
    }

    @Override
    public void delete(NvmeVlmData drbdVlmDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if NvmeVlmData will get a database table in future.
    }

    private String getId(NvmeRscData nvmeRscData)
    {
        return "(LayerRscId=" + nvmeRscData.getRscLayerId() +
            ", SuffResName=" + nvmeRscData.getSuffixedResourceName() +
            ")";
    }
}

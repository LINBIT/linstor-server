package com.linbit.linstor.core.objects;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerLuksVolumes;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.transaction.TransactionMgrETCD;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class LuksLayerETCDDriver extends BaseEtcdDriver implements LuksLayerDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;

    private final VlmPwDriver vlmPwDriver;

    @Inject
    public LuksLayerETCDDriver(
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

        vlmPwDriver = new VlmPwDriver();
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public Pair<LuksRscData, Set<RscLayerObject>> load(
        Resource rsc,
        int id,
        String rscSuffixRef,
        RscLayerObject parentRef
    )
        throws DatabaseException
    {
        Pair<DrbdRscData, Set<RscLayerObject>> ret;
        Set<RscLayerObject> childrenRscDataList = new HashSet<>();
        Map<VolumeNumber, LuksVlmData> vlmDataMap = new TreeMap<>();
        LuksRscData rscData = new LuksRscData(
            id,
            rsc,
            rscSuffixRef,
            parentRef,
            childrenRscDataList,
            vlmDataMap,
            this,
            transObjFactory,
            transMgrProvider
        );

        Map<String, String> luksVlmByIdMap = namespace(
            GeneratedDatabaseTables.LAYER_LUKS_VOLUMES,
            Integer.toString(id)
        )
            .get(true);
        Set<String> composedKeySet = EtcdUtils.getComposedPkList(luksVlmByIdMap);
        for (String composedKey : composedKeySet)
        {
            String[] pks = composedKey.split(EtcdUtils.PK_DELIMITER);
            VolumeNumber vlmNr;
            try
            {
                vlmNr = new VolumeNumber(Integer.parseInt(pks[LayerLuksVolumes.VLM_NR.getIndex()]));
            }
            catch (ValueOutOfRangeException exc)
            {
                throw new LinStorDBRuntimeException(
                    "Failed to restore stored volume number " + pks[LayerLuksVolumes.VLM_NR.getIndex()]
                );
            }
            vlmDataMap.put(
                vlmNr,
                new LuksVlmData(
                    rsc.getVolume(vlmNr),
                    rscData,
                    Base64.decode(luksVlmByIdMap.get(EtcdUtils.buildKey(LayerLuksVolumes.ENCRYPTED_PASSWORD, pks))),
                    this,
                    transObjFactory,
                    transMgrProvider
                )
            );
        }
        return new Pair<>(rscData, childrenRscDataList);
    }

    @Override
    public void persist(LuksRscData luksRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if LuksRscData will get a database table in future.
    }

    @Override
    public void delete(LuksRscData luksRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if LuksRscData will get a database table in future.
    }

    @Override
    public void persist(LuksVlmData luksVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating LuksVlmData %s", getId(luksVlmDataRef));
        getNamespace(luksVlmDataRef)
            .put(LayerLuksVolumes.ENCRYPTED_PASSWORD, Base64.encode(luksVlmDataRef.getEncryptedKey()));
    }

    @Override
    public void delete(LuksVlmData luksVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting LuksVlmData %s", getId(luksVlmDataRef));
        getNamespace(luksVlmDataRef)
            .delete(true);
    }

    @Override
    public SingleColumnDatabaseDriver<LuksVlmData, byte[]> getVlmEncryptedPasswordDriver()
    {
        return vlmPwDriver;
    }

    private FluentLinstorTransaction getNamespace(LuksVlmData luksVlmDataRef)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_LUKS_VOLUMES,
            Integer.toString(luksVlmDataRef.getRscLayerId()),
            Integer.toString(luksVlmDataRef.getVlmNr().value)
        );
    }

    private String getId(LuksVlmData luksVlmDataRef)
    {
        return "(LayerRscId=" + luksVlmDataRef.getRscLayerId() +
            ", VlmNr=" + luksVlmDataRef.getVlmNr().value +
            ")";
    }

    private class VlmPwDriver implements SingleColumnDatabaseDriver<LuksVlmData, byte[]>
    {

        @Override
        public void update(LuksVlmData luksVlmDataRef, byte[] encryptedPassword) throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating LuksVlmData's encrypted password %s",
                getId(luksVlmDataRef)
            );
            getNamespace(luksVlmDataRef)
                .put(LayerLuksVolumes.ENCRYPTED_PASSWORD, Base64.encode(encryptedPassword));
        }

    }
}

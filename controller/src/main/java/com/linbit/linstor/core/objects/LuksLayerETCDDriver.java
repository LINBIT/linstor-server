package com.linbit.linstor.core.objects;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerLuksVolumes;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;
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
public class LuksLayerETCDDriver extends BaseEtcdDriver implements LuksLayerCtrlDatabaseDriver
{
    private final static int PK_V_LRI_ID_IDX = 0;
    private final static int PK_V_VLM_NR_IDX = 1;

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
    @SuppressWarnings({"checkstyle:magicnumber", "unchecked"})
    public <RSC extends AbsResource<RSC>> Pair<LuksRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
        throws DatabaseException
    {
        Pair<DrbdRscData<RSC>, Set<AbsRscLayerObject<RSC>>> ret;
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
        String etcdKey = EtcdUtils.buildKey(
            GeneratedDatabaseTables.LAYER_LUKS_VOLUMES,
            Integer.toString(rscData.getRscLayerId())
        );
        // we did not specify the full PK, only the rscLayerId. PK should be something like
        // <rscId>:<vlmNr>
        // however, the returned etcdKey is something like "/LINSTOR/<table>/<rscId>/"
        // as we are using the --prefix, we need to cut away the last '/' in order to get all
        // volumes of this <rscId>
        etcdKey = etcdKey.substring(0, etcdKey.length() - EtcdUtils.PATH_DELIMITER.length());

        Map<String, String> luksVlmByIdMap = namespace(etcdKey).get(true);
        Set<String> composedKeySet = EtcdUtils.getComposedPkList(luksVlmByIdMap);
        for (String composedKey : composedKeySet)
        {
            String[] pks = EtcdUtils.splitPks(composedKey, false);
            VolumeNumber vlmNr;
            try
            {
                vlmNr = new VolumeNumber(Integer.parseInt(pks[PK_V_VLM_NR_IDX]));
            }
            catch (ValueOutOfRangeException exc)
            {
                throw new LinStorDBRuntimeException(
                    "Failed to restore stored volume number " + pks[PK_V_VLM_NR_IDX]
                );
            }
            vlmDataMap.put(
                vlmNr,
                new LuksVlmData<>(
                    absRsc.getVolume(vlmNr),
                    rscData,
                    Base64.decode(luksVlmByIdMap.get(EtcdUtils.buildKey(LayerLuksVolumes.ENCRYPTED_PASSWORD, pks))),
                    this,
                    transObjFactory,
                    transMgrProvider
                )
            );
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
        getNamespace(luksVlmDataRef)
            .put(LayerLuksVolumes.ENCRYPTED_PASSWORD, Base64.encode(luksVlmDataRef.getEncryptedKey()));
    }

    @Override
    public void delete(LuksVlmData<?> luksVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting LuksVlmData %s", getId(luksVlmDataRef));
        getNamespace(luksVlmDataRef)
            .delete(true);
    }

    @Override
    public SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> getVlmEncryptedPasswordDriver()
    {
        return vlmPwDriver;
    }

    private FluentLinstorTransaction getNamespace(LuksVlmData<?> luksVlmDataRef)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_LUKS_VOLUMES,
            Integer.toString(luksVlmDataRef.getRscLayerId()),
            Integer.toString(luksVlmDataRef.getVlmNr().value)
        );
    }

    private class VlmPwDriver implements SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]>
    {

        @Override
        public void update(LuksVlmData<?> luksVlmDataRef, byte[] encryptedPassword) throws DatabaseException
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

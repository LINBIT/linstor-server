package com.linbit.linstor.core.objects;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerLuksVolumes;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class LayerLuksVlmDbDriver
    extends AbsLayerVlmDataDbDriver<VlmDfnLayerObject, LuksRscData<?>, LuksVlmData<?>>
    implements LayerLuksVlmDatabaseDriver
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final LayerResourceIdDatabaseDriver rscLayerIdDriver;
    private final SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> encryptedPasswordDriver;

    @Inject
    public LayerLuksVlmDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.LAYER_LUKS_VOLUMES, dbEngineRef);
        rscLayerIdDriver = rscLayerIdDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(LayerLuksVolumes.LAYER_RESOURCE_ID, vlmData -> vlmData.getRscLayerObject().getRscLayerId());
        setColumnSetter(LayerLuksVolumes.VLM_NR, vlmData -> vlmData.getVlmNr().value);
        setColumnSetter(
            LayerLuksVolumes.ENCRYPTED_PASSWORD,
            luksVlm -> Base64.encode(luksVlm.getEncryptedKey())
        );

        encryptedPasswordDriver = generateSingleColumnDriver(
            LayerLuksVolumes.ENCRYPTED_PASSWORD,
            ignored -> MSG_DO_NOT_LOG,
            Base64::encode
        );
    }

    @Override
    protected Pair<LuksVlmData<?>, Void> load(
        RawParameters rawRef,
        VlmParentObjects<VlmDfnLayerObject, LuksRscData<?>, LuksVlmData<?>> parentRef
    )
        throws ValueOutOfRangeException, InvalidNameException, DatabaseException, AccessDeniedException
    {
        int lri = rawRef.getParsed(LayerLuksVolumes.LAYER_RESOURCE_ID);
        VolumeNumber vlmNr = rawRef.buildParsed(LayerLuksVolumes.VLM_NR, VolumeNumber::new);

        byte[] encryptedPassword = rawRef.buildParsed(LayerLuksVolumes.ENCRYPTED_PASSWORD, Base64::decode);

        LuksRscData<?> luksRscData = parentRef.getRscData(lri);
        AbsResource<?> absResource = luksRscData.getAbsResource();
        AbsVolume<?> absVlm = absResource.getVolume(vlmNr);

        LuksVlmData<?> luksVlmData = createAbsVlmData(
            absVlm,
            luksRscData,
            encryptedPassword
        );

        return new Pair<>(luksVlmData, null);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> LuksVlmData<RSC> createAbsVlmData(
        AbsVolume<?> absVlmRef,
        LuksRscData<?> luksRscDataRef,
        byte[] encryptedPasswordRef
    )
    {
        return new LuksVlmData<>(
            (AbsVolume<RSC>) absVlmRef,
            (LuksRscData<RSC>) luksRscDataRef,
            encryptedPasswordRef,
            this,
            transObjFactory,
            transMgrProvider
        );
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return rscLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> getVlmEncryptedPasswordDriver()
    {
        return encryptedPasswordDriver;
    }
}

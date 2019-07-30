package com.linbit.linstor.layer;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.LengthPadding;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.SecretGenerator;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
class LuksLayerHelper extends AbsLayerHelper<LuksRscData, LuksVlmData, RscDfnLayerObject, VlmDfnLayerObject>
{
    private static final int SECRET_KEY_BYTES = 20;

    private final CtrlSecurityObjects secObjs;
    private final LengthPadding cryptoLenPad;

    @Inject
    LuksLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef,
        CtrlSecurityObjects secObjsRef,
        LengthPadding cryptoLenPadRef,
        Provider<CtrlLayerDataHelper> layerHelperProviderRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            LuksRscData.class,
            DeviceLayerKind.LUKS,
            layerHelperProviderRef
        );
        secObjs = secObjsRef;
        cryptoLenPad = cryptoLenPadRef;
    }

    @Override
    protected RscDfnLayerObject createRscDfnData(
        ResourceDefinition rscDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // LuksLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected void mergeRscDfnData(RscDfnLayerObject rscDfnRef, LayerPayload payloadRef)
    {
        // no Luks specific resource-definition, nothing to merge
    }

    @Override
    protected VlmDfnLayerObject createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // LuksLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected void mergeVlmDfnData(VlmDfnLayerObject vlmDfnDataRef, LayerPayload payloadRef)
    {
        // no Luks specific volume-definition, nothing to merge
    }

    @Override
    protected LuksRscData createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        RscLayerObject parentObjectRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        return layerDataFactory.createLuksRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            rscNameSuffixRef,
            parentObjectRef
        );
    }

    @Override
    protected void mergeRscData(LuksRscData rscDataRef, LayerPayload payloadRef)
    {
        // nothing to merge
    }

    @Override
    protected boolean needsChildVlm(RscLayerObject childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
    {
        return true;
    }

    @Override
    protected LuksVlmData createVlmLayerData(
        LuksRscData luksRscData,
        Volume vlm,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException
    {
        byte[] masterKey = secObjs.getCryptKey();
        if (masterKey == null || masterKey.length == 0)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
                    "Unable to create an encrypted volume definition without having a master key")
                .setCause("The masterkey was not initialized yet")
                .setCorrection("Create or enter the master passphrase")
                .build()
            );
        }

        String vlmDfnKeyPlain = SecretGenerator.generateSecretString(SECRET_KEY_BYTES);
        SymmetricKeyCipher cipher;
        cipher = SymmetricKeyCipher.getInstanceWithKey(masterKey);

        byte[] encodedData = cryptoLenPad.conceal(vlmDfnKeyPlain.getBytes());
        byte[] encryptedVlmDfnKey = cipher.encrypt(encodedData);

        return layerDataFactory.createLuksVlmData(
            vlm,
            luksRscData,
            encryptedVlmDfnKey
        );
    }

    @Override
    protected void mergeVlmData(LuksVlmData vlmDataRef, Volume vlmRef, LayerPayload payloadRef)
        throws AccessDeniedException, InvalidKeyException
    {
        // nothing to do
    }

    @Override
    protected void resetStoragePools(RscLayerObject rscDataRef) throws AccessDeniedException, DatabaseException
    {
        // nothing to do
    }
}

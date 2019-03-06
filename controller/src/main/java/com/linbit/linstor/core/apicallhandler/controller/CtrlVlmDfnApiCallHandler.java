package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.drbd.md.GidGenerator;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeDefinition.VlmDfnWtihCreationPayload;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.SecretGenerator;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.Base64;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Singleton
class CtrlVlmDfnApiCallHandler
{
    private static final int SECRET_KEY_BYTES = 20;

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSecurityObjects secObjs;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    CtrlVlmDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSecurityObjects secObjsRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlVlmDfnCrtApiHelper = ctrlVlmDfnCrtApiHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        secObjs = secObjsRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    ApiCallRc createVolumeDefinitions(
        String rscNameStr,
        List<VlmDfnWtihCreationPayload> vlmDfnWithPayloadApiListRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeCreateOperation(),
            "Volume definitions for " + getRscDfnDescriptionInline(rscNameStr),
            "volume definitions for " + getRscDfnDescriptionInline(rscNameStr),
            ApiConsts.MASK_VLM_DFN,
            objRefs
        );

        try
        {
            if (vlmDfnWithPayloadApiListRef.isEmpty())
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.MASK_WARN,
                        "Volume definition list to create is empty."
                    )
                    .setDetails("Volume definition list that should be added to the resource is empty.")
                    .build()
                );
            }

            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

            Iterator<Resource> iterateResource = getRscIterator(rscDfn);
            List<Resource> rscList = new ArrayList<>();
            while (iterateResource.hasNext())
            {
                rscList.add(iterateResource.next());
            }

            List<VolumeDefinitionData> vlmDfnsCreated = createVlmDfns(rscDfn, vlmDfnWithPayloadApiListRef);

            for (VolumeDefinitionData vlmDfn : vlmDfnsCreated)
            {
                for (Resource rsc : rscList)
                {
                    ctrlVlmCrtApiHelper.createVolumeResolvingStorPool(
                        rsc,
                        vlmDfn,
                        null
                    );
                }
            }

            ctrlTransactionHelper.commit();

            for (VolumeDefinition vlmDfn : vlmDfnsCreated)
            {
                responseConverter.addWithOp(responses, context, createVlmDfnCrtSuccessEntry(vlmDfn, rscNameStr));
            }
            responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(rscDfn));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    List<VolumeDefinitionData> createVlmDfns(
        ResourceDefinition rscDfn,
        List<VlmDfnWtihCreationPayload> vlmDfnWithPayloadApiListRef
    )
    {
        List<VolumeDefinitionData> vlmDfns = new ArrayList<>();
        for (VlmDfnWtihCreationPayload vlmDfnApi : vlmDfnWithPayloadApiListRef)
        {
            vlmDfns.add(createVlmDfn(rscDfn, vlmDfnApi));
        }
        return vlmDfns;
    }

    /**
     * Throws contextualized exceptions.
     */
    VolumeDefinitionData createVlmDfn(
        ResourceDefinition rscDfn,
        VlmDfnWtihCreationPayload vlmDfnApiRef
    )
    {
        VolumeNumber volNr = getOrGenerateVlmNr(
            vlmDfnApiRef.getVlmDfn(),
            rscDfn,
            apiCtx
        );

        ResponseContext context = makeVlmDfnContext(
            ApiOperation.makeCreateOperation(),
            rscDfn.getName().displayValue,
            volNr.value
        );

        VolumeDefinitionData vlmDfn;
        try
        {
            long size = vlmDfnApiRef.getVlmDfn().getSize();

            VlmDfnFlags[] vlmDfnInitFlags = VlmDfnFlags.restoreFlags(vlmDfnApiRef.getVlmDfn().getFlags());

            vlmDfn = ctrlVlmDfnCrtApiHelper.createVlmDfnData(
                peerAccCtx.get(),
                rscDfn,
                volNr,
                vlmDfnApiRef.getDrbdMinorNr(),
                size,
                vlmDfnInitFlags
            );
            Map<String, String> propsMap = getVlmDfnProps(vlmDfn).map();

            ctrlPropsHelper.fillProperties(LinStorObject.VOLUME_DEFINITION, vlmDfnApiRef.getVlmDfn().getProps(),
                getVlmDfnProps(vlmDfn), ApiConsts.FAIL_ACC_DENIED_VLM_DFN);

            // Set an initial DRBD current generation identifier for use when creating volumes
            // in a setup that includes thin provisioning storage pools
            propsMap.put(ApiConsts.KEY_DRBD_CURRENT_GI, GidGenerator.generateRandomGid());

            if (Arrays.asList(vlmDfnInitFlags).contains(VlmDfnFlags.ENCRYPTED))
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

                byte[] encryptedVlmDfnKey = cipher.encrypt(vlmDfnKeyPlain.getBytes());

                propsMap.put(
                    ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD,
                    Base64.encode(encryptedVlmDfnKey)
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            throw new ApiRcException(responseConverter.exceptionToResponse(peer.get(), context, exc), exc, true);
        }

        return vlmDfn;
    }

    private Iterator<Resource> getRscIterator(ResourceDefinition rscDfn)
    {
        Iterator<Resource> iterator;
        try
        {
            iterator = rscDfn.iterateResource(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return iterator;
    }

    private VolumeNumber getOrGenerateVlmNr(VlmDfnApi vlmDfnApi, ResourceDefinition rscDfn, AccessContext accCtx)
    {
        VolumeNumber vlmNr;
        try
        {
            vlmNr = CtrlRscDfnApiCallHandler.getVlmNr(vlmDfnApi, rscDfn, accCtx);
        }
        catch (ValueOutOfRangeException valOORangeExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_VLM_NR, String.format(
                "The specified volume number '%d' is invalid. Volume numbers have to be in range of %d - %d.",
                vlmDfnApi.getVolumeNr(),
                VolumeNumber.VOLUME_NR_MIN,
                VolumeNumber.VOLUME_NR_MAX
            )), valOORangeExc);
        }
        catch (LinStorException linStorExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_POOL_EXHAUSTED_VLM_NR,
                "An exception occured during generation of a volume number."
            ), linStorExc);
        }
        return vlmNr;
    }

    private Props getVlmDfnProps(VolumeDefinitionData vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the properties of " + getVlmDfnDescriptionInline(vlmDfn),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return props;
    }

    private ApiCallRcEntry createVlmDfnCrtSuccessEntry(VolumeDefinition vlmDfn, String rscNameStr)
    {
        ApiCallRcEntry vlmDfnCrtSuccessEntry = new ApiCallRcEntry();
        vlmDfnCrtSuccessEntry.setReturnCode(ApiConsts.CREATED);
        String successMessage = String.format(
            "New volume definition with number '%d' of resource definition '%s' created.",
            vlmDfn.getVolumeNumber().value,
            rscNameStr
        );
        vlmDfnCrtSuccessEntry.setMessage(successMessage);
        vlmDfnCrtSuccessEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
        vlmDfnCrtSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

        errorReporter.logInfo(successMessage);
        return vlmDfnCrtSuccessEntry;
    }

    public static String getVlmDfnDescription(String rscName, Integer vlmNr)
    {
        return "Resource definition: " + rscName + ", Volume number: " + vlmNr;
    }

    public static String getVlmDfnDescriptionInline(VolumeDefinition vlmDfn)
    {
        return getVlmDfnDescriptionInline(vlmDfn.getResourceDefinition(), vlmDfn.getVolumeNumber());
    }

    public static String getVlmDfnDescriptionInline(ResourceDefinition rscDfn, VolumeNumber volNr)
    {
        return getVlmDfnDescriptionInline(rscDfn.getName().displayValue, volNr.value);
    }

    public static String getVlmDfnDescriptionInline(String rscName, Integer vlmNr)
    {
        return "volume definition with number '" + vlmNr + "' of resource definition '" + rscName + "'";
    }

    static ResponseContext makeVlmDfnContext(
        ApiOperation operation,
        String rscNameStr,
        int volumeNr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(volumeNr));

        return new ResponseContext(
            operation,
            getVlmDfnDescription(rscNameStr, volumeNr),
            getVlmDfnDescriptionInline(rscNameStr, volumeNr),
            ApiConsts.MASK_VLM_DFN,
            objRefs
        );
    }
}

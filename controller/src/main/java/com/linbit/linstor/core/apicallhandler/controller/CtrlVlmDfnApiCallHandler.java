package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.drbd.md.GidGenerator;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
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
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.Base64;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
class CtrlVlmDfnApiCallHandler
{
    private static final int SECRET_KEY_BYTES = 20;

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelper;
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
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        secObjs = secObjsRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    ApiCallRc createVolumeDefinitions(
        String rscNameStr,
        List<VlmDfnApi> vlmDfnApiList
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
            if (vlmDfnApiList.isEmpty())
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

            List<VolumeDefinitionData> vlmDfnsCreated = createVlmDfns(rscDfn, vlmDfnApiList);

            for (Resource rsc : rscList)
            {
                ctrlVlmDfnCrtApiHelper.adjustRscVolumes(rsc);
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
        List<VlmDfnApi> vlmDfnApis
    )
    {
        List<VolumeDefinitionData> vlmDfns = new ArrayList<>();
        for (VolumeDefinition.VlmDfnApi vlmDfnApi : vlmDfnApis)
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
        VlmDfnApi vlmDfnApi
    )
    {
        VolumeNumber volNr = getOrGenerateVlmNr(
            vlmDfnApi,
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
            long size = vlmDfnApi.getSize();

            VlmDfnFlags[] vlmDfnInitFlags = VlmDfnFlags.restoreFlags(vlmDfnApi.getFlags());

            vlmDfn = ctrlVlmDfnCrtApiHelper.createVlmDfnData(
                peerAccCtx.get(),
                rscDfn,
                volNr,
                vlmDfnApi.getMinorNr(),
                size,
                vlmDfnInitFlags
            );
            Map<String, String> propsMap = getVlmDfnProps(vlmDfn).map();

            ctrlPropsHelper.fillProperties(LinStorObject.VOLUME_DEFINITION, vlmDfnApi.getProps(),
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

    ApiCallRc modifyVlmDfn(
        UUID vlmDfnUuid,
        String rscName,
        int vlmNr,
        Long size,
        Integer minorNr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmDfnContext(
            ApiOperation.makeModifyOperation(),
            rscName,
            vlmNr
        );

        try
        {
            VolumeDefinitionData vlmDfn = loadVlmDfn(rscName, vlmNr);

            if (vlmDfnUuid != null && !vlmDfnUuid.equals(vlmDfn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_VLM_DFN,
                    "UUID check failed. Given UUID: " + vlmDfnUuid + ". Persisted UUID: " + vlmDfn.getUuid()
                ));
            }
            Props props = getVlmDfnProps(vlmDfn);
            Map<String, String> propsMap = props.map();

            ctrlPropsHelper.fillProperties(LinStorObject.VOLUME_DEFINITION, overrideProps,
                getVlmDfnProps(vlmDfn), ApiConsts.FAIL_ACC_DENIED_VLM_DFN);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            if (size != null)
            {
                long vlmDfnSize = getVlmDfnSize(vlmDfn);
                if (size >= vlmDfnSize)
                {
                    setVlmDfnSize(vlmDfn, size);

                    Iterator<Volume> vlmIter = vlmDfn.iterateVolumes(peerAccCtx.get());

                    if (vlmIter.hasNext())
                    {
                        vlmDfn.getFlags().enableFlags(peerAccCtx.get(), VlmDfnFlags.RESIZE);
                    }

                    while (vlmIter.hasNext())
                    {
                        Volume vlm = vlmIter.next();

                        vlm.getFlags().enableFlags(peerAccCtx.get(), Volume.VlmFlags.RESIZE);
                    }
                }
                else
                {
                    if (!hasDeployedVolumes(vlmDfn))
                    {
                        setVlmDfnSize(vlmDfn, size);
                    }
                    else
                    {
                        throw new ApiRcException(ApiCallRcImpl
                            .entryBuilder(
                                ApiConsts.FAIL_INVLD_VLM_SIZE,
                                "Deployed volumes can only grow in size, not shrink."
                            )
                            .setCorrection("If you want to shrink the volume definition, you have to remove all " +
                                "volumes of this volume definition first.")
                            .build()
                        );
                    }
                }
            }

            if (minorNr != null)
            {
                setMinorNr(vlmDfn, minorNr);
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                vlmDfn.getUuid(), getVlmDfnDescriptionInline(vlmDfn)));

            responseConverter.addWithDetail(
                responses, context, ctrlSatelliteUpdater.updateSatellites(vlmDfn.getResourceDefinition()));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    ApiCallRc deleteVolumeDefinition(
        String rscName,
        int vlmNr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmDfnContext(
            ApiOperation.makeDeleteOperation(),
            rscName,
            vlmNr
        );

        try
        {
            VolumeDefinitionData vlmDfn = loadVlmDfn(rscName, vlmNr);
            UUID vlmDfnUuid = vlmDfn.getUuid();
            ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();

            Optional<Resource> rscInUse = rscDfn.anyResourceInUse(apiCtx);
            if (rscInUse.isPresent())
            {
                NodeName nodeName = rscInUse.get().getAssignedNode().getName();
                String rscNameStr = rscDfn.getName().displayValue;
                responses.addEntry(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_DEL | ApiConsts.FAIL_IN_USE,
                        String.format("Resource '%s' on node '%s' is still in use.", rscNameStr, nodeName.displayValue)
                    )
                    .setCause("Resource is mounted/in use.")
                    .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.",
                        rscNameStr,
                        nodeName.displayValue))
                    .build()
                );
            }
            else
            {
                // mark volumes to delete or check if all a 'CLEAN'
                Iterator<Volume> itVolumes = vlmDfn.iterateVolumes(peerAccCtx.get());
                boolean allVlmClean = true;
                while (itVolumes.hasNext())
                {
                    Volume vlm = itVolumes.next();
                    if (vlm.getFlags().isUnset(peerAccCtx.get(), Volume.VlmFlags.CLEAN))
                    {
                        vlm.markDeleted(peerAccCtx.get());
                        allVlmClean = false;
                    }
                }

                String deleteAction;
                if (allVlmClean)
                {
                    vlmDfn.delete(peerAccCtx.get());
                    deleteAction = " was deleted.";
                }
                else
                {
                    vlmDfn.markDeleted(peerAccCtx.get());
                    deleteAction = " marked for deletion.";
                }

                ctrlTransactionHelper.commit();

                responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(rscDfn));

                responseConverter.addWithOp(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.DELETED,
                        firstLetterCaps(getVlmDfnDescriptionInline(rscName, vlmNr)) + deleteAction
                    )
                    .setDetails(firstLetterCaps(getVlmDfnDescriptionInline(rscName, vlmNr)) + " UUID is:" + vlmDfnUuid)
                    .build()
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private VolumeDefinitionData loadVlmDfn(String rscName, int vlmNr)
    {
        ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
        VolumeDefinitionData vlmDfn;
        try
        {
            vlmDfn = (VolumeDefinitionData) rscDfn.getVolumeDfn(peerAccCtx.get(), new VolumeNumber(vlmNr));

            if (vlmDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_VLM_DFN,
                        "Volume definition '" + rscName + "' with volume number '" + vlmNr + "' not found."
                    )
                    .setCause("The specified volume definition '" + rscName +
                        "' with volume number '" + vlmNr + "' could not be found in the database")
                    .setCorrection("Create a volume definition with the name '" + rscName + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load volume definition '" + vlmNr + "' from resource definition '" + rscName + "'",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_VLM_NR,
                "The given volume number '" + vlmNr + "' is invalid."
            ), valueOutOfRangeExc);
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
        try
        {
            vlmDfnCrtSuccessEntry.setReturnCode(ApiConsts.CREATED);
            String successMessage = String.format(
                "New volume definition with number '%d' of resource definition '%s' created.",
                vlmDfn.getVolumeNumber().value,
                rscNameStr
            );
            vlmDfnCrtSuccessEntry.setMessage(successMessage);
            vlmDfnCrtSuccessEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
            vlmDfnCrtSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
            vlmDfnCrtSuccessEntry.putObjRef(
                ApiConsts.KEY_MINOR_NR,
                Integer.toString(vlmDfn.getMinorNr(apiCtx).value)
            );

            errorReporter.logInfo(successMessage);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return vlmDfnCrtSuccessEntry;
    }

    private long getVlmDfnSize(VolumeDefinitionData vlmDfn)
    {
        long volumeSize;
        try
        {
            volumeSize = vlmDfn.getVolumeSize(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access Volume definition's size",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return volumeSize;
    }

    private void setVlmDfnSize(VolumeDefinitionData vlmDfn, Long size)
    {
        try
        {
            vlmDfn.setVolumeSize(peerAccCtx.get(), size);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "update Volume definition's size",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }

    }

    private boolean hasDeployedVolumes(VolumeDefinitionData vlmDfn)
    {
        boolean hasVolumes;
        try
        {
            hasVolumes = vlmDfn.iterateVolumes(peerAccCtx.get()).hasNext();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access volume definition",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return hasVolumes;
    }

    private void setMinorNr(VolumeDefinitionData vlmDfn, Integer minorNr)
    {
        try
        {
            vlmDfn.setMinorNr(
                peerAccCtx.get(),
                new MinorNumber(minorNr)
            );
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_MINOR_NR, String.format(
                "The specified minor number '%d' is invalid.",
                minorNr
            )), exc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set the minor number",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
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

    private static ResponseContext makeVlmDfnContext(
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

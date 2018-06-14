package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.SymmetricKeyCipher;
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
import com.linbit.linstor.VolumeDefinitionDataControllerFactory;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.Base64;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
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

class CtrlVlmDfnApiCallHandler extends CtrlVlmDfnCrtApiCallHandler
{
    private static final int SECRET_KEY_BYTES = 20;
    private String currentRscName;
    private VlmDfnApi currentVlmDfnApi;
    private Integer currentVlmNr;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection rscDfnMapProt;
    private final VolumeDefinitionDataControllerFactory volumeDefinitionDataFactory;
    private final CtrlSecurityObjects secObjs;

    @Inject
    CtrlVlmDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        @ApiContext AccessContext apiCtx,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        @Named(ConfigModule.CONFIG_STOR_POOL_NAME) String defaultStorPoolNameRef,
        CtrlObjectFactories objectFactories,
        VolumeDefinitionDataControllerFactory volumeDefinitionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        CtrlSecurityObjects secObjsRef,
        WhitelistProps whitelistPropsRef
    )
    {
        super(
            errorReporterRef,
            apiCtx,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef,
            defaultStorPoolNameRef,
            volumeDefinitionDataFactoryRef
        );

        rscDfnMap = rscDfnMapRef;
        rscDfnMapProt = rscDfnMapProtRef;
        volumeDefinitionDataFactory = volumeDefinitionDataFactoryRef;
        secObjs = secObjsRef;
    }

    private void updateCurrentKeyNumber(final String key, Integer number)
    {
        String intStringOrNull = null;
        Map<String, String> localVariables = variables.get();
        if (number != null)
        {
            intStringOrNull = number.toString();
            localVariables.put(key, intStringOrNull);
        }
        else
        {
            localVariables.remove(key);
        }
    }

    ApiCallRc createVolumeDefinitions(
        String rscNameStr,
        List<VlmDfnApi> vlmDfnApiList
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                peerAccCtx,
                ApiCallType.CREATE,
                apiCallRc,
                rscNameStr,
                null // no vlmNr
            )
        )
        {
            if (vlmDfnApiList.isEmpty())
            {
                addAnswer(
                    "Volume definition list to create is empty.",
                    null,
                    "Volume definition list that should be added to the resource is empty.",
                    null,
                    ApiConsts.MASK_WARN
                );
                throw new ApiCallHandlerFailedException();
            }

            ResourceDefinition rscDfn = loadRscDfn(rscNameStr, true);

            Iterator<Resource> iterateResource = getRscIterator(rscDfn);
            List<Resource> rscList = new ArrayList<>();
            while (iterateResource.hasNext())
            {
                rscList.add(iterateResource.next());
            }

            List<VolumeDefinitionData> vlmDfnsCreated = createVlmDfns(rscDfn, vlmDfnApiList);

            objRefs.get().remove(ApiConsts.KEY_VLM_NR);
            variables.get().remove(ApiConsts.KEY_VLM_NR);
            variables.get().remove(ApiConsts.KEY_MINOR_NR);

            for (Resource rsc : rscList)
            {
                adjustRscVolumes(rsc);
            }

            commit();

            rscDfnMap.put(rscDfn.getName(), rscDfn);

            for (VolumeDefinition vlmDfn : vlmDfnsCreated)
            {
                apiCallRc.addEntry(createVlmDfnCrtSuccessEntry(vlmDfn, rscNameStr));
            }
            updateSatellites(rscDfn);
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                "a volume definition of resource definition '" + rscNameStr + "'",
                getObjRefs(rscNameStr, null),
                getVariables(rscNameStr, null),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    List<VolumeDefinitionData> createVlmDfns(
        ResourceDefinition rscDfn,
        List<VlmDfnApi> vlmDfnApis
    )
    {
        List<VolumeDefinitionData> createVlmDfns = new ArrayList<>();

        try
        {
            for (VolumeDefinition.VlmDfnApi vlmDfnApi : vlmDfnApis)
            {
                {
                    currentVlmDfnApi = vlmDfnApi;

                    updateCurrentKeyNumber(ApiConsts.KEY_VLM_NR, vlmDfnApi.getVolumeNr());
                    updateCurrentKeyNumber(ApiConsts.KEY_MINOR_NR, vlmDfnApi.getMinorNr());
                }
                VolumeNumber volNr = null;

                volNr = getOrGenerateVlmNr(
                    vlmDfnApi,
                    rscDfn,
                    apiCtx
                );
                updateCurrentKeyNumber(ApiConsts.KEY_VLM_NR, volNr.value);
                currentVlmNr = volNr.value; // set vlmNr for exception error reporting

                long size = vlmDfnApi.getSize();

                VlmDfnFlags[] vlmDfnInitFlags = VlmDfnFlags.restoreFlags(vlmDfnApi.getFlags());

                VolumeDefinitionData vlmDfn = createVlmDfnData(
                    peerAccCtx,
                    rscDfn,
                    volNr,
                    vlmDfnApi.getMinorNr(),
                    size,
                    vlmDfnInitFlags
                );
                Map<String, String> propsMap = getVlmDfnProps(vlmDfn).map();

                fillProperties(vlmDfnApi.getProps(), getVlmDfnProps(vlmDfn), ApiConsts.FAIL_ACC_DENIED_VLM_DFN);

                if (Arrays.asList(vlmDfnInitFlags).contains(VlmDfnFlags.ENCRYPTED))
                {
                    byte[] masterKey = secObjs.getCryptKey();
                    if (masterKey == null || masterKey.length == 0)
                    {
                        throw asExc(
                            null,
                            "Unable to create an encrypted volume definition without having a master key",
                            "The masterkey was not initialized yet",
                            null, // details
                            "Create or enter the master passphrase",
                            ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY
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

                updateCurrentKeyNumber(ApiConsts.KEY_MINOR_NR, vlmDfn.getMinorNr(peerAccCtx).value);

                createVlmDfns.add(vlmDfn);
            }
        }
        catch (LinStorException exc)
        {
            throw asExc(exc, "Unknown exception", ApiConsts.FAIL_UNKNOWN_ERROR);
        }

        return createVlmDfns;
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
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (AbsApiCallHandler basicallyThis = setContext(
            peerAccCtx,
            ApiCallType.MODIFY,
                apiCallRc,
                rscName,
                vlmNr
            )
        )
        {
            VolumeDefinitionData vlmDfn = loadVlmDfn(rscName, vlmNr);

            if (vlmDfnUuid != null && !vlmDfnUuid.equals(vlmDfn.getUuid()))
            {
                throw asExc(
                    null,
                    "UUID check failed. Given UUID: " + vlmDfnUuid + ". Persisted UUID: " + vlmDfn.getUuid(),
                    ApiConsts.FAIL_UUID_VLM_DFN
                );
            }
            Props props = getVlmDfnProps(vlmDfn);
            Map<String, String> propsMap = props.map();

            fillProperties(overrideProps, getVlmDfnProps(vlmDfn), ApiConsts.FAIL_ACC_DENIED_VLM_DFN);

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
                }
                else
                {
                    if (!hasDeployedVolumes(vlmDfn))
                    {
                        setVlmDfnSize(vlmDfn, size);
                    }
                    else
                    {
                        throw asExc(
                            null,
                            "Deployed volumes can only grow in size, not shrink.",
                            null,
                            null,
                            "If you want to shrink the volume definition, you have to remove all volumes of " +
                            "this volume definition first.",
                            ApiConsts.FAIL_INVLD_VLM_SIZE
                        );
                    }
                }
            }

            if (minorNr != null)
            {
                setMinorNr(vlmDfn, minorNr);
            }

            commit();

            reportSuccess(vlmDfn.getUuid());

            updateSatellites(vlmDfn.getResourceDefinition());
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(rscName, vlmNr),
                getObjRefs(rscName, vlmNr),
                getVariables(rscName, vlmNr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    ApiCallRc deleteVolumeDefinition(
        String rscName,
        int vlmNr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                peerAccCtx,
                ApiCallType.DELETE,
                apiCallRc,
                rscName,
                vlmNr
            );
        )
        {
            VolumeDefinitionData vlmDfn = loadVlmDfn(rscName, vlmNr);
            UUID vlmDfnUuid = vlmDfn.getUuid();
            ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();

            Optional<Resource> rscInUse = rscDfn.anyResourceInUse(apiCtx);
            if (rscInUse.isPresent())
            {
                NodeName nodeName = rscInUse.get().getAssignedNode().getName();
                String rscNameStr = rscDfn.getName().displayValue;
                addAnswer(
                    String.format("Resource '%s' on node '%s' is still in use.", rscNameStr, nodeName.displayValue),
                    "Resource is mounted/in use.",
                    null,
                    String.format("Un-mount resource '%s' on the node '%s'.",
                        rscNameStr,
                        nodeName.displayValue),
                    ApiConsts.MASK_ERROR | ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_DEL
                );
            }
            else
            {
                // mark volumes to delete or check if all a 'CLEAN'
                Iterator<Volume> itVolumes = vlmDfn.iterateVolumes(peerAccCtx);
                boolean allVlmClean = true;
                while (itVolumes.hasNext())
                {
                    Volume vlm = itVolumes.next();
                    if (vlm.getFlags().isUnset(peerAccCtx, Volume.VlmFlags.CLEAN))
                    {
                        vlm.markDeleted(peerAccCtx);
                        allVlmClean = false;
                    }
                }

                String deleteAction;
                if (allVlmClean)
                {
                    vlmDfn.delete(peerAccCtx);
                    deleteAction = " was deleted.";
                }
                else
                {
                    vlmDfn.markDeleted(peerAccCtx);
                    deleteAction = " marked for deletion.";
                }

                commit();

                updateSatellites(rscDfn);

                reportSuccess(
                    getObjectDescriptionInlineFirstLetterCaps() + deleteAction,
                    getObjectDescriptionInlineFirstLetterCaps() + " UUID is:" + vlmDfnUuid
                );
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(rscName, vlmNr),
                getObjRefs(rscName, vlmNr),
                getVariables(rscName, vlmNr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    private void ensureRscMapProtAccess(AccessContext accCtx)
    {
        try
        {
            rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "change any existing resource definition",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
    }

    private VolumeDefinitionData loadVlmDfn(String rscName, int vlmNr)
    {
        ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);
        VolumeDefinitionData vlmDfn;
        try
        {
            vlmDfn = volumeDefinitionDataFactory.load(
                peerAccCtx,
                rscDfn,
                new VolumeNumber(vlmNr)
            );

            if (vlmDfn == null)
            {
                throw asExc(
                    null, // throwable
                    "Volume definition '" + rscName + "' with volume number '" + vlmNr + "' not found.",
                    "The specified volume definition '" + rscName +
                        "' with volume number '" + vlmNr + "' could not be found in the database", // cause
                    null, // details
                    "Create a volume definition with the name '" + rscName + "' first.", // correction
                    ApiConsts.FAIL_NOT_FOUND_VLM_DFN
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading volume definition '" + vlmNr + "' from resource definition '" + rscName + "'",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            throw asExc(
                valueOutOfRangeExc,
                "The given volume number '" + vlmNr + "' is invalid.",
                ApiConsts.FAIL_INVLD_VLM_NR
            );
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
            throw asImplError(accDeniedExc);
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
            throw asExc(
                valOORangeExc,
                String.format(
                    "The specified volume number '%d' is invalid. Volume numbers have to be in range of %d - %d.",
                    vlmDfnApi.getVolumeNr(),
                    VolumeNumber.VOLUME_NR_MIN,
                    VolumeNumber.VOLUME_NR_MAX
                ),
                ApiConsts.FAIL_INVLD_VLM_NR
            );
        }
        catch (LinStorException linStorExc)
        {
            throw asExc(
                linStorExc,
                "An exception occured during generation of a volume number.",
                ApiConsts.FAIL_POOL_EXHAUSTED_VLM_NR
            );
        }
        return vlmNr;
    }

    private Props getVlmDfnProps(VolumeDefinitionData vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access the properties of " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return props;
    }

    private boolean isFlagSet(VolumeDefinitionData vlmDfn, VlmDfnFlags flag)
    {
        boolean isSet;

        try
        {
            isSet = vlmDfn.getFlags().isSet(peerAccCtx, flag);
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "access volume definitions flags",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }

        return isSet;
    }

    private ApiCallRcEntry createVlmDfnCrtSuccessEntry(VolumeDefinition vlmDfn, String rscNameStr)
    {
        ApiCallRcEntry vlmDfnCrtSuccessEntry = new ApiCallRcEntry();
        try
        {
            vlmDfnCrtSuccessEntry.setReturnCode(ApiConsts.MASK_VLM_DFN | ApiConsts.MASK_CRT | ApiConsts.CREATED);
            String successMessage = String.format(
                "New volume definition with number '%d' of resource definition '%s' created.",
                vlmDfn.getVolumeNumber().value,
                rscNameStr
            );
            vlmDfnCrtSuccessEntry.setMessageFormat(successMessage);
            vlmDfnCrtSuccessEntry.putVariable(
                ApiConsts.KEY_RSC_DFN,
                vlmDfn.getResourceDefinition().getName().displayValue
            );
            vlmDfnCrtSuccessEntry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
            vlmDfnCrtSuccessEntry.putVariable(
                ApiConsts.KEY_MINOR_NR,
                Integer.toString(vlmDfn.getMinorNr(apiCtx).value)
            );
            vlmDfnCrtSuccessEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
            vlmDfnCrtSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

            errorReporter.logInfo(successMessage);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return vlmDfnCrtSuccessEntry;
    }

    private long getVlmDfnSize(VolumeDefinitionData vlmDfn)
    {
        long volumeSize;
        try
        {
            volumeSize = vlmDfn.getVolumeSize(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing Volume definition's size",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return volumeSize;
    }

    private void setVlmDfnSize(VolumeDefinitionData vlmDfn, Long size)
    {
        try
        {
            vlmDfn.setVolumeSize(peerAccCtx, size);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "updating Volume definition's size",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(sqlExc, "updating volume definition's size");
        }

    }

    private boolean hasDeployedVolumes(VolumeDefinitionData vlmDfn)
    {
        boolean hasVolumes;
        try
        {
            hasVolumes = vlmDfn.iterateVolumes(peerAccCtx).hasNext();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing volume definition",
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
                peerAccCtx,
                new MinorNumber(minorNr)
            );
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            throw asExc(
                exc,
                String.format(
                    "The specified minor number '%d' is invalid.",
                    minorNr
                ),
                ApiConsts.FAIL_INVLD_MINOR_NR
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "setting the minor number",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(sqlExc, "updating minor number");
        }
    }

    private AbsApiCallHandler setContext(
        AccessContext accCtx,
        ApiCallType apiCallType,
        ApiCallRcImpl apiCallRc,
        String rscNameStr,
        Integer vlmNr
    )
    {
        super.setContext(
            apiCallType,
            apiCallRc,
            true, // autoClose
            getObjRefs(rscNameStr, vlmNr),
            getVariables(rscNameStr, vlmNr)
        );

        ensureRscMapProtAccess(accCtx);

        currentRscName = rscNameStr;
        currentVlmDfnApi = null;
        currentVlmNr = vlmNr;

        return this;
    }

    private Map<String, String> getObjRefs(String rscName, Integer vlmNr)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscName);
        if (vlmNr != null)
        {
            objRefs.put(ApiConsts.KEY_VLM_NR, vlmNr.toString());
        }
        return objRefs;
    }

    private Map<String, String> getVariables(String rscName, Integer vlmNr)
    {
        Map<String, String> vars = new TreeMap<>();
        vars.put(ApiConsts.KEY_RSC_NAME, rscName);
        if (vlmNr != null)
        {
            vars.put(ApiConsts.KEY_VLM_NR, vlmNr.toString());
        }
        return vars;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Resource definition: " + currentRscName + ", Volume number: " + currentVlmNr;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentRscName, currentVlmNr);
    }

    public static String getObjectDescriptionInline(String rscName, Integer vlmNr)
    {
        return "volume definition with number '" + vlmNr + "' of resource definition '" + rscName + "'";
    }
}

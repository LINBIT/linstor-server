package com.linbit.linstor.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

class CtrlVlmDfnApiCallHandler extends AbsApiCallHandler
{
    private final CtrlSerializer<Resource> rscSerializer;

    private final ThreadLocal<String> currentRscNameStr = new ThreadLocal<>();
    private final ThreadLocal<VlmDfnApi> currentVlmDfnApi = new ThreadLocal<>();
    private final ThreadLocal<Integer> currentVlmNr = new ThreadLocal<>();

    CtrlVlmDfnApiCallHandler(
        Controller controller,
        CtrlSerializer<Resource> rscSerializer,
        AccessContext apiCtx
    )
    {
        super(controller, apiCtx, ApiConsts.MASK_VLM_DFN);
        super.setNullOnAutoClose(
            currentRscNameStr,
            currentVlmDfnApi,
            currentVlmNr
        );
        this.rscSerializer = rscSerializer;
    }

    ApiCallRc createVolumeDefinitions(
        AccessContext accCtx,
        Peer client,
        String rscNameStr,
        List<VlmDfnApi> vlmDfnApiList
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setCurrent(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                null, // create new transMgr
                rscNameStr,
                null // no vlmNr
            );
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

            ResourceDefinition rscDfn = loadRscDfn(rscNameStr);

            if (rscDfn == null)
            {
                addAnswer(
                        String.format("Resource definition '%s' not found.", rscNameStr),
                        null,
                        "Volume definition couldn't be created because the " +
                        "specified resource definition was not found.",
                        "Create or use an already existing resource definition.",
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN
                    );
                throw new ApiCallHandlerFailedException();
            }

            Iterator<Resource> iterateResource = getRscIterator(rscDfn);
            List<Resource> rscList = new ArrayList<>();
            while (iterateResource.hasNext())
            {
                rscList.add(iterateResource.next());
            }

            List<VolumeDefinition> vlmDfnsCreated = new ArrayList<>();
            for (VolumeDefinition.VlmDfnApi vlmDfnApi : vlmDfnApiList)
            {
                currentVlmDfnApi.set(vlmDfnApi);
                currentObjRefs.get().put(
                    ApiConsts.KEY_VLM_NR,
                    vlmDfnApi.getVolumeNr() + "" // possibly null, DO NOT change to Integer.toString
                );
                currentVariables.get().put(
                    ApiConsts.KEY_VLM_NR,
                    vlmDfnApi.getVolumeNr() + "" // possibly null, DO NOT change to Integer.toString
                );
                currentVariables.get().put(
                    ApiConsts.KEY_MINOR_NR,
                    vlmDfnApi.getMinorNr() + ""  // possibly null, DO NOT change to Integer.toString
                );

                currentObjRefs.get().put(ApiConsts.KEY_VLM_NR, vlmDfnApi.getVolumeNr() + "");

                VolumeNumber volNr = null;
                MinorNumber minorNr = null;

                volNr = getOrGenerateVlmNr(
                    vlmDfnApi,
                    rscDfn,
                    apiCtx
                );
                minorNr = getOrGenerateMinorNr(vlmDfnApi);

                long size = vlmDfnApi.getSize();

                VlmDfnFlags[] vlmDfnInitFlags = null;

                VolumeDefinitionData vlmDfn = createVlmDfnData(
                    accCtx,
                    rscDfn,
                    volNr,
                    minorNr,
                    size,
                    vlmDfnInitFlags
                );
                getVlmDfnProps(vlmDfn).map().putAll(vlmDfnApi.getProps());

                vlmDfnsCreated.add(vlmDfn);
            }
            currentObjRefs.get().remove(ApiConsts.KEY_VLM_NR);
            currentVariables.get().remove(ApiConsts.KEY_VLM_NR);
            currentVariables.get().remove(ApiConsts.KEY_MINOR_NR);

            for (Resource rsc : rscList)
            {
                adjustRscVolumes(rsc);
            }

            commit();

            controller.rscDfnMap.put(rscDfn.getName(), rscDfn);

            for (VolumeDefinition vlmDfn : vlmDfnsCreated)
            {
                apiCallRc.addEntry(createVlmDfnCrtSuccessEntry(vlmDfn, rscNameStr));
            }
            notifySatellites(rscDfn);
        }
        catch (ApiCallHandlerFailedException e)
        {
            // exception already reported and apiCallRc created.
            // nothing to do here.
            // the only reason we have this catch block here is for flow-control
        }
        catch (Exception exc)
        {
            Map<String, String> objRefs = new TreeMap<>();
            Map<String, String> variables = new TreeMap<>();

            fillMaps(objRefs, variables, rscNameStr, null);

            reportStatic(
                exc,
                "Volume definition could not be created due to an unknown exception.",
                null, // causeMsg
                null, // detailsMsg
                null, // correctionMsg,
                ApiConsts.RC_VLM_DFN_CRT_FAIL_UNKNOWN_ERROR,
                objRefs,
                variables,
                apiCallRc,
                controller,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    ApiCallRc modifyVlmDfn(
        AccessContext accCtx,
        Peer client,
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

        try (AbsApiCallHandler basicallyThis = setCurrent(
                accCtx,
                client,
                ApiCallType.MODIFY,
                apiCallRc,
                null, // create new transMgr
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

            System.out.println("putting all: " + overrideProps);
            propsMap.putAll(overrideProps);

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

            reportSuccess("Volume definition '" + vlmNr + "' on resource definition '" + rscName + "' modified.");

            notifySatellites(vlmDfn.getResourceDefinition());
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // exception already reported and apiCallRc created.
            // nothing to do here.
            // the only reason we have this catch block here is for flow-control
        }
        catch (Exception exc)
        {
            Map<String, String> objRefs = new TreeMap<>();
            Map<String, String> variables = new TreeMap<>();
            fillMaps(objRefs, variables, rscName, vlmNr);
            reportStatic(
                exc,
                "Volume definition could not be created due to an unknown exception.",
                null, // causeMsg
                null, // detailsMsg
                null, // correctionMsg
                ApiConsts.RC_VLM_DFN_MOD_FAIL_UNKNOWN_ERROR,
                objRefs,
                variables,
                apiCallRc,
                controller,
                accCtx,
                client
            );
        }
        catch (ImplementationError implErr)
        {
            Map<String, String> objRefs = new TreeMap<>();
            Map<String, String> variables = new TreeMap<>();
            fillMaps(objRefs, variables, rscName, vlmNr);
            reportStatic(
                implErr,
                "Volume definition could not be created due to an implementation error.",
                null, // causeMsg
                null, // detailsMsg
                null, // correctionMsg
                ApiConsts.RC_VLM_DFN_MOD_FAIL_IMPL_ERROR,
                objRefs,
                variables,
                apiCallRc,
                controller,
                accCtx,
                client
            );
        }


        return apiCallRc;
    }

    @Override
    protected TransactionMgr createNewTransMgr() throws ApiCallHandlerFailedException
    {
        try
        {
            return new TransactionMgr(controller.dbConnPool.getConnection());
        }
        catch (SQLException sqlExc)
        {
            handleSqlExc(sqlExc, ApiConsts.RC_VLM_DFN_CRT_FAIL_SQL);
            throw new ApiCallHandlerFailedException();
        }
    }

    private void ensureRscMapProtAccess(AccessContext accCtx)
    {
        try
        {
            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleAccDeniedExc(
                accDeniedExc,
                "change any existing resource definition",
                ApiConsts.RC_VLM_DFN_CRT_FAIL_ACC_DENIED_RSC_DFN
            );
            throw new ApiCallHandlerFailedException();
        }
    }


    private VolumeDefinitionData loadVlmDfn(String rscName, int vlmNr)
    {
        ResourceDefinitionData rscDfn = loadRscDfn(rscName);
        try
        {
            return VolumeDefinitionData.getInstance(
                currentAccCtx.get(),
                rscDfn,
                new VolumeNumber(vlmNr),
                null, // minor
                null, // volSize
                null, // initFlags
                currentTransMgr.get(),
                false,
                false
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading volume definition '" + vlmNr + "' from resource definition '"+ rscName + "'",
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
        catch (LinStorDataAlreadyExistsException | MdException implError)
        {
            throw asImplError(implError);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "loading " + getObjectDescriptionInline()
            );
        }
    }

    private Iterator<Resource> getRscIterator(ResourceDefinition rscDfn)
    {
        try
        {
            return rscDfn.iterateResource(apiCtx);
        }
        catch (AccessDeniedException e)
        {
            handleImplError(e);
            throw new ApiCallHandlerFailedException();
        }
    }

    private VolumeNumber getOrGenerateVlmNr(VlmDfnApi vlmDfnApi, ResourceDefinition rscDfn, AccessContext accCtx)
    {
        try
        {
            return CtrlRscDfnApiCallHandler.getVlmNr(vlmDfnApi, rscDfn, accCtx);
        }
        catch (ValueOutOfRangeException valOORangeExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage = String.format(
                "The specified volume number '%d' is invalid. Volume numbers have to be in range of %d - %d.",
                vlmDfnApi.getVolumeNr(),
                VolumeNumber.VOLUME_NR_MIN,
                VolumeNumber.VOLUME_NR_MAX
            );
            entry.setReturnCodeBit(ApiConsts.RC_VLM_DFN_CRT_FAIL_INVLD_VLM_NR);

            controller.getErrorReporter().reportError(
                valOORangeExc,
                accCtx,
                currentPeer.get(),
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            String vlmNrStr = vlmDfnApi.getVolumeNr() + "";// nullable, DO NOT change to Integer.toString
            String rscNameStr = rscDfn.getName().displayValue;
            entry.putVariable(ApiConsts.KEY_VLM_NR, vlmNrStr);
            entry.putVariable(ApiConsts.KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(ApiConsts.KEY_VLM_NR, vlmNrStr);

            currentApiCallRc.get().addEntry(entry);

            throw new ApiCallHandlerFailedException();
        }
    }

    private MinorNumber getOrGenerateMinorNr(VlmDfnApi vlmDfnApi)
    {
        try
        {
            return CtrlRscDfnApiCallHandler.getMinor(vlmDfnApi);
        }
        catch (ValueOutOfRangeException valOORangeExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage = String.format(
                "The specified minor number '%d' is invalid. Minor numbers have to be in range of %d - %d.",
                vlmDfnApi.getMinorNr(),
                MinorNumber.MINOR_NR_MIN,
                MinorNumber.MINOR_NR_MAX
            );
            entry.setReturnCodeBit(ApiConsts.RC_VLM_DFN_CRT_FAIL_INVLD_MINOR_NR);

            controller.getErrorReporter().reportError(
                valOORangeExc,
                currentAccCtx.get(),
                currentPeer.get(),
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            String vlmNrStr = vlmDfnApi.getVolumeNr() + "";// nullable, DO NOT change to Integer.toString
            String rscNameStr = currentRscNameStr.get();
            entry.putVariable(ApiConsts.KEY_VLM_NR, vlmNrStr);
            entry.putVariable(ApiConsts.KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(ApiConsts.KEY_VLM_NR, vlmNrStr);

            currentApiCallRc.get().addEntry(entry);
            throw new ApiCallHandlerFailedException();
        }
    }

    private VolumeDefinitionData createVlmDfnData(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        VolumeNumber volNr,
        MinorNumber minorNr,
        long size,
        VlmDfnFlags[] vlmDfnInitFlags
    )
    {
        try
        {
            return VolumeDefinitionData.getInstance(
                accCtx,
                rscDfn,
                volNr,
                minorNr,
                size,
                vlmDfnInitFlags,
                currentTransMgr.get(),
                true, // persist this entry
                true // throw exception if the entry exists
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleAccDeniedExc(
                accDeniedExc,
                String.format(
                    "create a new volume definition with number '%d' in resource definition '%s'.",
                    volNr.value,
                    rscDfn.getName().displayValue
                ),
                ApiConsts.RC_VLM_DFN_CRT_FAIL_ACC_DENIED_VLM_DFN
            );
            throw new ApiCallHandlerFailedException();
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            handleDataExists(dataAlreadyExistsExc);
            throw new ApiCallHandlerFailedException();
        }
        catch (SQLException sqlExc)
        {
            handleSqlExc(sqlExc, ApiConsts.RC_VLM_DFN_CRT_FAIL_SQL);
            throw new ApiCallHandlerFailedException();
        }
        catch (MdException mdExc)
        {
            handleMdExc(mdExc);
            throw new ApiCallHandlerFailedException();
        }
    }

    private Props getVlmDfnProps(VolumeDefinitionData vlmDfn)
    {
        try
        {
            return vlmDfn.getProps(currentAccCtx.get());
        }
        catch (AccessDeniedException e)
        {
            handleAccDeniedExc(
                e,
                String.format(
                    "access the properties of the volume definition with number '%d' in resource " +
                        "definition '%s'.",
                    vlmDfn.getVolumeNumber().value,
                    vlmDfn.getResourceDefinition().getName().displayValue
                ),
                ApiConsts.RC_VLM_DFN_CRT_FAIL_ACC_DENIED_VLM_DFN);
            throw new ApiCallHandlerFailedException();
        }
    }

    private void adjustRscVolumes(Resource rsc)
    {
        try
        {
            rsc.adjustVolumes(apiCtx, currentTransMgr.get(), controller.getDefaultStorPoolName());
        }
        catch (InvalidNameException invalidNameExc)
        {
            handleInvalidStorPoolNameExc(invalidNameExc, ApiConsts.RC_VLM_DFN_CRT_FAIL_INVLD_STOR_POOL_NAME);
            throw new ApiCallHandlerFailedException();
        }
        catch (LinStorException e)
        {
            throw asExc(
                e,
                "An exception occured while adjusting resources.",
                ApiConsts.FAIL_UNKNOWN_ERROR // TODO somehow find out if the exception is caused
                // by a missing storpool (not deployed yet?), and return a more meaningful RC
            );
        }
    }

    private ApiCallRcEntry createVlmDfnCrtSuccessEntry(VolumeDefinition vlmDfn, String rscNameStr)
    {
        ApiCallRcEntry vlmDfnCrtSuccessEntry = new ApiCallRcEntry();
        try
        {
            vlmDfnCrtSuccessEntry.setReturnCode(ApiConsts.RC_VLM_DFN_CREATED);
            String successMessage = String.format(
                "Volume definition with number '%d' successfully " +
                    " created in resource definition '%s'.",
                vlmDfn.getVolumeNumber().value,
                rscNameStr
            );
            vlmDfnCrtSuccessEntry.setMessageFormat(successMessage);
            vlmDfnCrtSuccessEntry.putVariable(ApiConsts.KEY_RSC_DFN, vlmDfn.getResourceDefinition().getName().displayValue);
            vlmDfnCrtSuccessEntry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
            vlmDfnCrtSuccessEntry.putVariable(ApiConsts.KEY_MINOR_NR, Integer.toString(vlmDfn.getMinorNr(apiCtx).value));
            vlmDfnCrtSuccessEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
            vlmDfnCrtSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

            controller.getErrorReporter().logInfo(successMessage);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleImplError(accDeniedExc);
            throw new ApiCallHandlerFailedException();
        }
        return vlmDfnCrtSuccessEntry;
    }

    private long getVlmDfnSize(VolumeDefinitionData vlmDfn)
    {
        try
        {
            return vlmDfn.getVolumeSize(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing Volume definition's size",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
    }

    private void setVlmDfnSize(VolumeDefinitionData vlmDfn, Long size)
    {
        try
        {
            vlmDfn.setVolumeSize(currentAccCtx.get(), size);
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
        try
        {
            return vlmDfn.iterateVolumes(currentAccCtx.get()).hasNext();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing volume definition",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
    }

    private void setMinorNr(VolumeDefinitionData vlmDfn, Integer minorNr)
    {
        try
        {
            vlmDfn.setMinorNr(
                currentAccCtx.get(),
                toMinorNr(minorNr)
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

    private MinorNumber toMinorNr(Integer minorNr)
    {
        try
        {
            return new MinorNumber(minorNr);
        }
        catch (ValueOutOfRangeException valOutOfRangeExc)
        {
            throw asExc(
                valOutOfRangeExc,
                "The given minor number '" + minorNr + "' is invalid.",
                ApiConsts.FAIL_INVLD_MINOR_NR
            );
        }
    }

    private void notifySatellites(ResourceDefinition rscDfn)
    {
        try
        {
            Iterator<Resource> iterateResource = rscDfn.iterateResource(apiCtx);
            while (iterateResource.hasNext())
            {
                Resource rsc = iterateResource.next();
                Peer peer = rsc.getAssignedNode().getPeer(apiCtx);

                if (peer.isConnected())
                {
                    Message msg = peer.createMessage();
                    msg.setData(rscSerializer.getChangedMessage(rsc));
                    peer.sendMessage(msg);
                }
                else
                {
                    String nodeName = rsc.getAssignedNode().getName().displayValue;
                    addAnswer(
                        "No active connection to satellite '" + nodeName + "'",
                        null,
                        "The satellite was added and the controller tries to (re-) establish connection to it." +
                        "The controller stored the new volume definition and as soon the satellite is connected, it will " +
                        "receive this update.",
                        null,
                        ApiConsts.WARN_NOT_CONNECTED
                    );
                }
            }
        }
        catch (IllegalMessageStateException illegalMsgStateExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "VlmDfnApi could not send a message properly to a satellite",
                    illegalMsgStateExc
                )
            );
            throw new ApiCallHandlerFailedException();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "ApiCtx does not have enough privileges",
                    accDeniedExc
                )
            );
            throw new ApiCallHandlerFailedException();
        }
    }

    /*
     * exception handlers
     */

    private void handleSqlExc(SQLException sqlExc, long apiRcCode)
    {
        String errorMessage;
        VlmDfnApi vlmDfnApi = currentVlmDfnApi.get();
        if (vlmDfnApi != null)
        {
            errorMessage = String.format(
                "A database error occured while creating the volume definition with the number '%d' " +
                    "in resource definition '%s'.",
                vlmDfnApi.getVolumeNr(),
                currentRscNameStr
            );
        }
        else
        {
            errorMessage = String.format(
                "A database error occured while creating a volume definition " +
                    "in resource definition '%s'.",
                currentRscNameStr
            );
        }
        asExc(sqlExc, errorMessage, apiRcCode);
    }

    private void handleImplError(Exception exc)
    {
        String errorMessage = null;
        long apiRc = 0;
        VlmDfnApi vlmDfnApi = currentVlmDfnApi.get();
        if (currentApiCallType.get().equals(ApiCallType.CREATE))
        {
            apiRc = ApiConsts.RC_VLM_DFN_CRT_FAIL_IMPL_ERROR;
            if (vlmDfnApi != null)
            {
                errorMessage = String.format(
                    "The volume definition with number '%d' could not be added to resource definition " +
                        "'%s' due to an implementation error",
                    vlmDfnApi.getVolumeNr(),
                    currentRscNameStr.get()
                );
            }
            else
            {
                errorMessage = String.format(
                    "A volume definition could not be added to the resource definition '%s' "+
                        "due to an implementation error.",
                    currentRscNameStr.get()
                );
            }
        }
        asExc(exc, errorMessage, apiRc);
    }

    private void handleAccDeniedExc(AccessDeniedException accDeniedExc, String action, long apiRcCode)
    {
        AccessContext accCtx = currentAccCtx.get();
        asAccDeniedExc(
            accDeniedExc,
            String.format(
                "The access context (user: '%s', role: '%s') has no permission to %s",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                action
            ),
            apiRcCode
        );
    }

    private void handleDataExists(LinStorDataAlreadyExistsException dataAlreadyExistsExc)
    {
        asExc(
            dataAlreadyExistsExc,
            String.format(
                "A volume definition with the numer %d already exists in resource definition '%s'.",
                currentVlmDfnApi.get().getVolumeNr(),
                currentRscNameStr.get()
            ),
            ApiConsts.RC_VLM_DFN_CRT_FAIL_EXISTS_VLM_DFN
        );
    }

    private void handleInvalidRscNameExc(
        InvalidNameException invalidNameExc,
        long retCode
    )
    {
        asExc(
            invalidNameExc,
            String.format(
                "The given resource name '%s' is invalid.",
                invalidNameExc.invalidName
            ),
            retCode
        );
    }

    private void handleInvalidStorPoolNameExc(InvalidNameException invalidNameExc, long retCode)
    {
        asExc(
            invalidNameExc,
            String.format(
                "The given stor pool name '%s' is invalid",
                invalidNameExc.invalidName
            ),
            retCode
        );
    }

    private void handleMdExc(MdException mdExc)
    {
        asExc(
            mdExc,
            String.format(
                "The volume definition with number '%d' for resource definition '%s' has an invalid size of '%d'. " +
                    "Valid sizes range from %d to %d.",
                currentVlmDfnApi.get().getVolumeNr(),
                currentRscNameStr.get(),
                MetaData.DRBD_MIN_NET_kiB,
                MetaData.DRBD_MAX_kiB
            ),
            ApiConsts.RC_VLM_DFN_CRT_FAIL_INVLD_VLM_SIZE
        );
    }

    private AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer client,
        ApiCallType apiCallType,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String rscNameStr,
        Integer vlmNr
    )
    {
        super.setCurrent(accCtx, client, apiCallType, apiCallRc, transMgr);

        ensureRscMapProtAccess(accCtx);

        currentRscNameStr.set(rscNameStr);
        currentVlmDfnApi.set(null);
        currentVlmNr.set(vlmNr);

        Map<String, String> objRefs = currentObjRefs.get();
        Map<String, String> vars = currentVariables.get();
        fillMaps(objRefs, vars, rscNameStr, vlmNr);
        return this;
    }

    private void fillMaps(Map<String, String> objRefs, Map<String, String> vars, String rscNameStr, Integer vlmNr)
    {
        objRefs.clear();
        vars.clear();

        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        vars.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        if (vlmNr != null)
        {
            objRefs.put(ApiConsts.KEY_VLM_NR, vlmNr.toString());
            vars.put(ApiConsts.KEY_VLM_NR, vlmNr.toString());
        }
    }

    @Override
    protected String getObjectDescription()
    {
        return "Resource definition: " + currentRscNameStr.get() + ", Volume number: " + currentVlmNr.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return "volume definition with number '" + currentVlmNr.get() + "' of resource definition '" + currentRscNameStr.get() + "'";
    }
}

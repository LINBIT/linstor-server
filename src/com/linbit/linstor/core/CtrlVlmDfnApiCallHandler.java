package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlSerializer;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

class CtrlVlmDfnApiCallHandler extends AbsApiCallHandler
{
    private CtrlSerializer<Resource> rscSerializer;

    private ThreadLocal<String> currentRscNameStr = new ThreadLocal<>();
    private ThreadLocal<VlmDfnApi> currentVlmDfnApi = new ThreadLocal<>();

    CtrlVlmDfnApiCallHandler(
        Controller controller,
        CtrlSerializer<Resource> rscSerializer,
        AccessContext apiCtx
    )
    {
        super(controller, apiCtx, ApiConsts.MASK_VLM_DFN);
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
            AbsApiCallHandler basicallyThis = setCurrentImpl(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                null, // create new transMgr
                rscNameStr
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
                        ApiConsts.WARN_NOT_CONNECTED
                    );
                throw new CtrlVlmDfnApiCallHandlerFailedException();
            }

            ensureRscMapProtAccess(accCtx);

            ResourceDefinition rscDfn = getRscDfn(accCtx, rscNameStr);

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
                    KEY_VLM_NR,
                    vlmDfnApi.getVolumeNr() + "" // possibly null, DO NOT change to Integer.toString
                );
                currentVariables.get().put(
                    KEY_VLM_NR,
                    vlmDfnApi.getVolumeNr() + "" // possibly null, DO NOT change to Integer.toString
                );
                currentVariables.get().put(
                    KEY_MINOR_NR,
                    vlmDfnApi.getMinorNr() + ""  // possibly null, DO NOT change to Integer.toString
                );

                currentObjRefs.get().put(KEY_VLM_NR, vlmDfnApi.getVolumeNr() + "");

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
                getVlmDfnProps(accCtx, vlmDfn).map().putAll(vlmDfnApi.getProps());

                vlmDfnsCreated.add(vlmDfn);
            }
            currentObjRefs.get().remove(KEY_VLM_NR);
            currentVariables.get().remove(KEY_VLM_NR);
            currentVariables.get().remove(KEY_MINOR_NR);

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
            notifySatellites(rscList);
        }
        catch (CtrlVlmDfnApiCallHandlerFailedException e)
        {
            // exception already reported and apiCallRc created.
            // nothing to do here.
            // the only reason we have this catch block here is for flow-control
        }
        catch (Exception exc)
        {
            exc(
                exc,
                "Volume definition could not be created due to an unknown exception.",
                ApiConsts.RC_VLM_DFN_CRT_FAIL_UNKNOWN_ERROR
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
            throw new CtrlVlmDfnApiCallHandlerFailedException();
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
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
    }

    private ResourceDefinition getRscDfn(AccessContext accCtx, String rscNameStr)
    {
        try
        {
            return ResourceDefinitionData.getInstance(
                accCtx,
                new ResourceName(rscNameStr), // invalidNameExc1
                null, // tcpPortNumber only needed when we want to persist this object
                null, // flags         only needed when we want to persist this object
                null, // secret        only needed when we want to persist this object
                currentTransMgr.get(),
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleAccDeniedExc(
                accDeniedExc,
                "access the resource definition '" + rscNameStr + "'",
                ApiConsts.RC_VLM_DFN_CRT_FAIL_ACC_DENIED_RSC_DFN
            );
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            handleImplError(dataAlreadyExistsExc);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
        catch (SQLException sqlExc)
        {
            handleSqlExc(sqlExc, ApiConsts.RC_VLM_DFN_CRT_FAIL_SQL);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
        catch (InvalidNameException invalidNameExc)
        {
            handleInvalidRscNameExc(invalidNameExc, ApiConsts.RC_VLM_DFN_CRT_FAIL_INVLD_RSC_NAME);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
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
            throw new CtrlVlmDfnApiCallHandlerFailedException();
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
            entry.putVariable(KEY_VLM_NR, vlmNrStr);
            entry.putVariable(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, vlmNrStr);

            currentApiCallRc.get().addEntry(entry);

            throw new CtrlVlmDfnApiCallHandlerFailedException();
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
            entry.putVariable(KEY_VLM_NR, vlmNrStr);
            entry.putVariable(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, vlmNrStr);

            currentApiCallRc.get().addEntry(entry);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
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
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            handleDataExists(dataAlreadyExistsExc);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
        catch (SQLException sqlExc)
        {
            handleSqlExc(sqlExc, ApiConsts.RC_VLM_DFN_CRT_FAIL_SQL);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
        catch (MdException mdExc)
        {
            handleMdExc(mdExc);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
    }

    private Props getVlmDfnProps(AccessContext accCtx, VolumeDefinitionData vlmDfn)
    {
        try
        {
            return vlmDfn.getProps(accCtx);
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
            throw new CtrlVlmDfnApiCallHandlerFailedException();
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
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
    }

    private void commit()
    {
        try
        {
            currentTransMgr.get().commit();
        }
        catch (SQLException sqlExc)
        {
            handleSqlExc(sqlExc, ApiConsts.RC_VLM_DFN_CRT_FAIL_SQL);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
    }

    private ApiCallRcEntry createVlmDfnCrtSuccessEntry(VolumeDefinition vlmDfn, String rscNameStr)
    {
        ApiCallRcEntry vlmDfnCrtSuccessEntry = new ApiCallRcEntry();
        try
        {
            vlmDfnCrtSuccessEntry.setReturnCode(RC_VLM_DFN_CREATED);
            String successMessage = String.format(
                "Volume definition with number '%d' successfully " +
                    " created in resource definition '%s'.",
                vlmDfn.getVolumeNumber().value,
                rscNameStr
            );
            vlmDfnCrtSuccessEntry.setMessageFormat(successMessage);
            vlmDfnCrtSuccessEntry.putVariable(KEY_RSC_DFN, vlmDfn.getResourceDefinition().getName().displayValue);
            vlmDfnCrtSuccessEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
            vlmDfnCrtSuccessEntry.putVariable(KEY_MINOR_NR, Integer.toString(vlmDfn.getMinorNr(apiCtx).value));
            vlmDfnCrtSuccessEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
            vlmDfnCrtSuccessEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

            controller.getErrorReporter().logInfo(successMessage);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleImplError(accDeniedExc);
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
        return vlmDfnCrtSuccessEntry;
    }

    private void notifySatellites(List<Resource> rscList)
    {
        try
        {
            for (Resource rsc : rscList)
            {
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
            throw new CtrlVlmDfnApiCallHandlerFailedException();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "ApiCtx does not have enough privileges",
                    accDeniedExc
                )
            );
            throw new CtrlVlmDfnApiCallHandlerFailedException();
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
        exc(sqlExc, errorMessage, apiRcCode);
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
        exc(exc, errorMessage, apiRc);
    }

    private void handleAccDeniedExc(AccessDeniedException accDeniedExc, String action, long apiRcCode)
    {
        AccessContext accCtx = currentAccCtx.get();
        accDeniedExc(
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
        exc(
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
        exc(
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
        exc(
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
        exc(
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

    private AbsApiCallHandler setCurrentImpl(
        AccessContext accCtx,
        Peer client,
        ApiCallType apiCallType,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String rscNameStr
    )
    {
        super.setCurrent(accCtx, client, apiCallType, apiCallRc, transMgr);

        this.currentRscNameStr.set(rscNameStr);
        currentVlmDfnApi.set(null);


        Map<String, String> objRefs = currentObjRefs.get();
        objRefs.clear();
        objRefs.put(KEY_RSC_DFN, currentRscNameStr.get());
        Map<String, String> vars = currentVariables.get();
        vars.clear();
        vars.put(KEY_RSC_NAME, currentRscNameStr.get());
        return this;
    }

    @Override
    protected void rollbackIfDirty()
    {
        TransactionMgr transMgr = currentTransMgr.get();
        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException sqlExc)
                {
                    exc(sqlExc, String.format(
                        "A database error occured while trying to rollback the creation of "
                            + "Volume definitions for Resource definition '%s'.",
                            // TODO: improve this error message - maybe mention the vlmNrs
                            currentRscNameStr.get()
                        ),
                        RC_STOR_POOL_DEL_FAIL_SQL_ROLLBACK);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
    }

    private static class CtrlVlmDfnApiCallHandlerFailedException extends RuntimeException
    {
        private static final long serialVersionUID = 3922462817645686356L;
    }
}

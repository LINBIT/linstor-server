package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.KEY_MINOR_NR;
import static com.linbit.linstor.api.ApiConsts.KEY_RSC_DFN;
import static com.linbit.linstor.api.ApiConsts.KEY_VLM_NR;
import static com.linbit.linstor.api.ApiConsts.RC_STOR_POOL_DEL_FAIL_SQL_ROLLBACK;
import static com.linbit.linstor.api.ApiConsts.RC_VLM_DFN_CREATED;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.DrbdDataAlreadyExistsException;
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
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.interfaces.Serializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

public class CtrlVlmDfnApiCallHandler
{
    private Controller controller;
    private Serializer<Resource> rscSerializer;
    private AccessContext apiCtx;

    CtrlVlmDfnApiCallHandler(
        Controller controller,
        Serializer<Resource> rscSerializer,
        AccessContext apiCtx
    )
    {
        this.controller = controller;
        this.rscSerializer = rscSerializer;
        this.apiCtx = apiCtx;
    }

    public ApiCallRc createVolumeDefinitions(
        AccessContext accCtx,
        Peer client,
        String rscNameStr,
        List<VlmDfnApi> vlmDfnApiList
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        ResourceDefinition rscDfn = null;
        TransactionMgr transMgr = null;

        VolumeNumber volNr = null;
        MinorNumber minorNr = null;
        VolumeDefinition.VlmDfnApi currentVlmDfnApi = null;

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection()); // sqlExc1
            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE); // accDeniedExc1

            rscDfn = ResourceDefinitionData.getInstance( // sqlExc2, accDeniedExc1 (same as last line), alreadyExistsExc1
                accCtx,
                new ResourceName(rscNameStr), // invalidNameExc1
                null, // tcpPortNumber only needed when we want to persist this object
                null, // flags         only needed when we want to persist this object
                null, // secret        only needed when we want to persist this object
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            rscDfn.setConnection(transMgr); // the vlmDfns will inject themselves here

            Iterator<Resource> iterateResource = rscDfn.iterateResource(apiCtx);
            List<Resource> rscList = new ArrayList<>();
            while (iterateResource.hasNext())
            {
                rscList.add(iterateResource.next());
            }

            List<VolumeDefinition> vlmDfnsCreated = new ArrayList<>();
            for (VolumeDefinition.VlmDfnApi vlmDfnApi : vlmDfnApiList)
            {
                currentVlmDfnApi = vlmDfnApi;

                volNr = null;
                minorNr = null;

                volNr = new VolumeNumber(
                    CtrlRscDfnApiCallHandler.getVlmNr(
                        vlmDfnApi,
                        rscDfn,
                        apiCtx
                    )
                ); // valOORangeExc2
                minorNr = new MinorNumber(
                    CtrlRscDfnApiCallHandler.getMinor(vlmDfnApi)
                ); // valOORangeExc3

                long size = vlmDfnApi.getSize();

                VlmDfnFlags[] vlmDfnInitFlags = null;

                VolumeDefinitionData vlmDfn = VolumeDefinitionData.getInstance( // mdExc2, sqlExc3, accDeniedExc2, alreadyExistsExc2
                    accCtx,
                    rscDfn,
                    volNr,
                    minorNr,
                    size,
                    vlmDfnInitFlags,
                    transMgr,
                    true, // persist this entry
                    true // throw exception if the entry exists
                );
                vlmDfn.setConnection(transMgr);
                vlmDfn.getProps(accCtx).map().putAll(vlmDfnApi.getProps());

                vlmDfnsCreated.add(vlmDfn);
            }
            for (Resource rsc : rscList)
            {
                rsc.adjustVolumes(apiCtx, transMgr, controller.getDefaultStorPoolName());
            }

            transMgr.commit(); // sqlExc4

            controller.rscDfnMap.put(rscDfn.getName(), rscDfn);

            for (VolumeDefinition vlmDfn : vlmDfnsCreated)
            {
                ApiCallRcEntry volSuccessEntry = new ApiCallRcEntry();
                volSuccessEntry.setReturnCode(RC_VLM_DFN_CREATED);
                String successMessage = String.format(
                    "Volume definition with number '%d' successfully " +
                        " created in resource definition '%s'.",
                    vlmDfn.getVolumeNumber().value,
                    rscNameStr
                );
                volSuccessEntry.setMessageFormat(successMessage);
                volSuccessEntry.putVariable(KEY_RSC_DFN, vlmDfn.getResourceDefinition().getName().displayValue);
                volSuccessEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
                volSuccessEntry.putVariable(KEY_MINOR_NR, Integer.toString(vlmDfn.getMinorNr(apiCtx).value));
                volSuccessEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                volSuccessEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

                apiCallRc.addEntry(volSuccessEntry);

                controller.getErrorReporter().logInfo(successMessage);
            }
            notifySatellites(rscList);
        }
        catch (SQLException sqlExc)
        {
            // TODO
            controller.getErrorReporter().reportError(sqlExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // TODO
            controller.getErrorReporter().reportError(accDeniedExc);
        }
        catch (DrbdDataAlreadyExistsException e)
        {
            // TODO
            controller.getErrorReporter().reportError(e);
        }
        catch (InvalidNameException e)
        {
            // TODO
            controller.getErrorReporter().reportError(e);
        }
        catch (ValueOutOfRangeException e)
        {
            // TODO
            controller.getErrorReporter().reportError(e);
        }
        catch (MdException e)
        {
            // TODO
            controller.getErrorReporter().reportError(e);
        }

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
                    String errorMessage = String.format(
                        "A database error occured while trying to rollback the creation of "
                            + "Volume definitions for Resource definition '%s'.",
                        // TODO: improve this error message - maybe mention the vlmNrs
                        rscNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_STOR_POOL_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
                    entry.putVariable(ApiConsts.KEY_RSC_NAME, rscNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

    private void notifySatellites(List<Resource> rscList) throws AccessDeniedException
    {
        try
        {
            for (Resource rsc : rscList)
            {
                Peer peer = rsc.getAssignedNode().getPeer(apiCtx);
                Message msg = peer.createMessage();
                msg.setData(rscSerializer.getChangedMessage(rsc));
                peer.sendMessage(msg);
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
        }
    }
}

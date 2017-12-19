package com.linbit.linstor.core;


import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

class CtrlStorPoolDfnApiCallHandler extends AbsApiCallHandler
{
    private final CtrlListSerializer<StorPoolDefinitionData.StorPoolDfnApi> listSerializer;
    private final ThreadLocal<String> currentStorPoolNameStr = new ThreadLocal<>();

    CtrlStorPoolDfnApiCallHandler(
        Controller controllerRef,
        CtrlListSerializer<StorPoolDefinitionData.StorPoolDfnApi> storPoolDfnSerializer
    )
    {
        super(
            controllerRef,
            null, // apiCtx
            ApiConsts.MASK_STOR_POOL_DFN
        );
        super.setNullOnAutoClose(currentStorPoolNameStr);
        listSerializer = storPoolDfnSerializer;
    }

    public ApiCallRc createStorPoolDfn(
        AccessContext accCtx,
        Peer client,
        String storPoolNameStr,
        Map<String, String> storPoolDfnProps
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        TransactionMgr transMgr = null;
        StorPoolName storPoolName = null;

        try
        {
            controller.storPoolDfnMapProt.requireAccess(accCtx, AccessType.CHANGE);
            transMgr = new TransactionMgr(controller.dbConnPool);

            storPoolName = new StorPoolName(storPoolNameStr);

            StorPoolDefinitionData storPoolDfn = StorPoolDefinitionData.getInstance(
                accCtx,
                storPoolName,
                transMgr,
                true, // persist this entry
                true // throw exception if the entry exists
            );
            storPoolDfn.setConnection(transMgr);
            storPoolDfn.getProps(accCtx).map().putAll(storPoolDfnProps);

            transMgr.commit();

            String successMessage = String.format(
                "Storage pool definition '%s' successfully created",
                storPoolNameStr
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_CREATED);
            entry.setMessageFormat(successMessage);
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
            controller.storPoolDfnMap.put(storPoolName, storPoolDfn);
            controller.getErrorReporter().logInfo(successMessage);
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while creating the storage pool definition '%s'.",
                storPoolNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            String errorMessage = String.format(
                "The access context (user: '%s', role: '%s') has no permission to create storage pool " +
                    "definition '%s'.",
                    accCtx.subjectId.name.displayValue,
                    accCtx.subjectRole.name.displayValue,
                    storPoolNameStr
                );
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_CRT_FAIL_ACC_DENIED_STOR_POOL_DFN);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            String errorMessage = String.format(
                "The specified storage pool name '%s' is not a valid.",
                storPoolNameStr
            );
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_CRT_FAIL_INVLD_STOR_POOL_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            String errorMsg = String.format(
                "A storage pool definition with the name '%s' already exists.",
                storPoolNameStr
            );
            controller.getErrorReporter().reportError(
                alreadyExistsExc,
                accCtx,
                client,
                errorMsg
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_CRT_FAIL_EXISTS_STOR_POOL_DFN);
            entry.setMessageFormat(errorMsg);
            entry.setCauseFormat(alreadyExistsExc.getMessage());
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while creating a storage pool definition '%s'. ",
                storPoolNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_CRT_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
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
                            + "storage pool definition '%s'.",
                        storPoolNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
                    entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
        return apiCallRc;
    }

    public ApiCallRc modifyStorPoolDfn(
        AccessContext accCtx,
        Peer client,
        UUID storPoolDfnUuid,
        String storPoolNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setCurrent(
                accCtx,
                client,
                ApiCallType.MODIFY,
                apiCallRc,
                null,
                storPoolNameStr
            );
        )
        {
            requireStorPoolDfnChangeAccess();
            StorPoolDefinitionData storPoolDfn = loadStorPoolDfn(storPoolNameStr);

            if (storPoolDfnUuid != null && !storPoolDfnUuid.equals(storPoolDfn.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    ApiConsts.FAIL_UUID_STOR_POOL_DFN
                );
                throw new ApiCallHandlerFailedException();
            }

            Props props = getProps(storPoolDfn);
            Map<String, String> propsMap = props.map();

            propsMap.putAll(overrideProps);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            updateSatellites(storPoolDfn);
            reportSuccess("Storage pool definition '" + storPoolNameStr + "' updated.");
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // failure was reported and added to returning apiCallRc
            // this is only for flow-control.
        }
        catch (Exception exc)
        {
            asExc(
                exc,
                "Modifying storage pool definition '" + storPoolNameStr + "' failed due to an unknown exception.",
                ApiConsts.FAIL_UNKNOWN_ERROR
            );
        }
        catch (ImplementationError implErr)
        {
            asImplError(implErr);
        }

        return apiCallRc;
    }

    private void requireStorPoolDfnChangeAccess()
    {
        try
        {
            controller.storPoolDfnMapProt.requireAccess(
                currentAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "change any storage definitions pools.",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
    }

    public ApiCallRc deleteStorPoolDfn(AccessContext accCtx, Peer client, String storPoolNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        TransactionMgr transMgr = null;
        StorPoolName storPoolName = null;
        try
        {
            controller.storPoolDfnMapProt.requireAccess(accCtx, AccessType.CHANGE);
            transMgr = new TransactionMgr(controller.dbConnPool);

            storPoolName = new StorPoolName(storPoolNameStr);

            StorPoolDefinitionData storPoolDefinitionData = StorPoolDefinitionData.getInstance(
                accCtx,
                storPoolName,
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );

            if (storPoolDefinitionData == null)
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_DEL_WARN_NOT_FOUND);
                entry.setMessageFormat(
                    String.format(
                        "Storage pool definition '%s' was not deleted as it was not found",
                        storPoolNameStr
                    )
                );
                entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
                entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
                apiCallRc.addEntry(entry);
            }
            else
            {
                storPoolDefinitionData.setConnection(transMgr);
//                storPoolDefinitionData.markDeleted(accCtx);
                // TODO: check if there are still storPools open, if not, delete this storPoolDefinition
                // client has to manually remove storPools prior removing storPoolDefinition
                transMgr.commit();

                String successMessage = String.format(
                    "Storage pool definition '%s' marked to be deleted",
                    storPoolNameStr
                );
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_DELETED);
                entry.setMessageFormat(successMessage);
                entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
                entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

                apiCallRc.addEntry(entry);
                controller.getErrorReporter().logInfo(successMessage);
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while deleting the storage pool definition '%s'.",
                storPoolNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            String errorMessage = String.format(
                "The access context (user: '%s', role: '%s') has no permission to delete storage pool " +
                    "definition '%s'.",
                    accCtx.subjectId.name.displayValue,
                    accCtx.subjectRole.name.displayValue,
                    storPoolNameStr
                );
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_DEL_FAIL_ACC_DENIED_STOR_POOL_DFN);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            String errorMessage = String.format(
                "The specified storage pool name '%s' is not a valid.",
                storPoolNameStr
            );
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_DEL_FAIL_INVLD_STOR_POOL_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    String.format(
                        ".getInstance was called with failIfExists=false, still threw an AlreadyExistsException "+
                            "(storage pool name: '%s')",
                        storPoolNameStr
                    ),
                    dataAlreadyExistsExc
                )
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_DEL_FAIL_IMPL_ERROR);
            entry.setMessageFormat(
                String.format(
                    "Failed to delete the storage pool definition '%s' due to an implementation error.",
                    storPoolNameStr
                )
            );
            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while deleting a storage pool definition '%s'. ",
                storPoolNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_DEL_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
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
                        "A database error occured while trying to rollback the deletion of "+
                            "storage pool definition '%s'.",
                        storPoolNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(ApiConsts.RC_STOR_POOL_DFN_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
                    entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
        return apiCallRc;
    }

    byte[] listStorPoolDefinitions(int msgId, AccessContext accCtx, Peer client)
    {
        ArrayList<StorPoolDefinitionData.StorPoolDfnApi> storPoolDfns = new ArrayList<>();
        try
        {
            controller.storPoolDfnMapProt.requireAccess(accCtx, AccessType.VIEW);// accDeniedExc1
            for (StorPoolDefinition storPoolDfn : controller.storPoolDfnMap.values())
            {
                try
                {
                    storPoolDfns.add(storPoolDfn.getApiData(accCtx));
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add storpooldfn without access
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        try
        {
            return listSerializer.getListMessage(msgId, storPoolDfns);
        }
        catch (IOException e)
        {
            controller.getErrorReporter().reportError(
                e,
                null,
                client,
                "Could not complete list message due to an IOException"
            );
        }

        return null;
    }

    protected AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String storPoolNameStr
    )
    {
        super.setCurrent(accCtx, peer, type, apiCallRc, transMgr);

        currentStorPoolNameStr.set(storPoolNameStr);

        Map<String, String> objRefs = currentObjRefs.get();
        objRefs.clear();
        objRefs.put(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
        Map<String, String> vars = currentVariables.get();
        vars.clear();
        vars.put(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);

        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Storage pool definition: " + currentStorPoolNameStr.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return "storage pool definition '" + currentStorPoolNameStr.get() + "'";
    }

    private Props getProps(StorPoolDefinitionData storPoolDfn)
    {
        try
        {
            return storPoolDfn.getProps(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing properties of storage pool definition '" + storPoolDfn.getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
    }

    private void updateSatellites(StorPoolDefinitionData storPoolDfn)
    {
        try
        {
            Iterator<StorPool> iterateStorPools = storPoolDfn.iterateStorPools(apiCtx);
            while (iterateStorPools.hasNext())
            {
                StorPool storPool = iterateStorPools.next();
                updateSatellite(storPool);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }
}

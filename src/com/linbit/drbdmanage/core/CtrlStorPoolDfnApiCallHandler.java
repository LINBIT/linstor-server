package com.linbit.drbdmanage.core;

import static com.linbit.drbdmanage.ApiCallRcConstants.*;
import static com.linbit.drbdmanage.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiCallRcImpl;
import com.linbit.drbdmanage.StorPoolDefinitionData;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.DrbdDataAlreadyExistsException;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;

class CtrlStorPoolDfnApiCallHandler
{
    private Controller controller;

    CtrlStorPoolDfnApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
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
                true,
                true
            );

            transMgr.commit();

            String successMessage = String.format(
                "Storage pool definition '%s' successfully created",
                storPoolNameStr
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_CREATED);
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
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

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
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_CRT_FAIL_ACC_DENIED_STOR_POOL_DFN);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

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
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_CRT_FAIL_INVLD_STOR_POOL_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException alreadyExistsExc)
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
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_CRT_FAIL_EXISTS_STOR_POOL_DFN);
            entry.setMessageFormat(errorMsg);
            entry.setCauseFormat(alreadyExistsExc.getMessage());
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);
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
                    entry.setReturnCodeBit(RC_STOR_POOL_DFN_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
                    entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
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
                false,
                false
            );

            if (storPoolDefinitionData == null)
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_STOR_POOL_DFN_DEL_NOT_FOUND);
                entry.setMessageFormat(
                    String.format(
                        "Storage pool definition '%s' was not deleted as it was not found",
                        storPoolNameStr
                    )
                );
                entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);
                entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                apiCallRc.addEntry(entry);
            }
            else
            {
                storPoolDefinitionData.setConnection(transMgr);
//                storPoolDefinitionData.markDeleted(accCtx);
                // TODO: check if there are still storpools open, if not, delete this storpooldefinition
                transMgr.commit();

                String successMessage = String.format(
                    "Storage pool definition '%s' marked to be deleted",
                    storPoolNameStr
                );
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_STOR_POOL_DFN_DELETED);
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
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

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
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_DEL_FAIL_ACC_DENIED_STOR_POOL_DFN);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

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
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_DEL_FAIL_INVLD_STOR_POOL_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException dataAlreadyExistsExc)
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
            entry.setReturnCodeBit(RC_STOR_POOL_DFN_DEL_FAIL_IMPL_ERROR);
            entry.setMessageFormat(
                String.format(
                    "Failed to delete the storage pool definition '%s' due to an implementation error.",
                    storPoolNameStr
                )
            );
            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);

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
                    entry.setReturnCodeBit(RC_STOR_POOL_DFN_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                    entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }
}

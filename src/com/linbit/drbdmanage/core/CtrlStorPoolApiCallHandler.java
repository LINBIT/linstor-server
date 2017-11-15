package com.linbit.drbdmanage.core;

import static com.linbit.drbdmanage.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.DrbdDataAlreadyExistsException;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.StorPoolDefinitionData;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.api.ApiCallRc;
import com.linbit.drbdmanage.api.ApiCallRcImpl;
import com.linbit.drbdmanage.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.interfaces.Serializer;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

class CtrlStorPoolApiCallHandler
{
    private final Controller controller;
    private final Serializer<StorPool> serializer;
    private final AccessContext apiCtx;

    CtrlStorPoolApiCallHandler(
        Controller controllerRef,
        Serializer<StorPool> serializerRef,
        AccessContext apiCtxRef
    )
    {
        controller = controllerRef;
        serializer = serializerRef;
        apiCtx = apiCtxRef;
    }

    public ApiCallRc createStorPool(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String storPoolNameStr,
        String driver,
        Map<String, String> storPoolPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;

        NodeName nodeName = null;
        StorPoolName storPoolName = null;

        NodeData node = null;
        StorPoolDefinitionData storPoolDfn = null;

        try
        {
            boolean fail = false;

            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName = new NodeName(nodeNameStr); // invalidNameExc1
            storPoolName = new StorPoolName(storPoolNameStr); // invalidNameExc2

            node = NodeData.getInstance( // accDeniedExc1, dataAlreadyExistsExc0
                accCtx,
                nodeName,
                null, // type  is  only relevant if we want to persist
                null, // flags are only relevant if we want to persist
                transMgr,
                false, // do not persist
                false  // do not throw exception if exists
            );

            storPoolDfn = StorPoolDefinitionData.getInstance( // accDeniedExc2, dataAlreadyExistsExc0
                accCtx,
                storPoolName,
                transMgr,
                false, // do not persist
                false  // do not throw exception if exists
            );

            if (node == null)
            {
                ApiCallRcEntry nodeNotFoundEntry = new ApiCallRcEntry();
                nodeNotFoundEntry.setReturnCode(RC_STOR_POOL_CRT_FAIL_NOT_FOUND_NODE);
                nodeNotFoundEntry.setCauseFormat(String.format(
                    "The specified node '%s' could not be found in the database",
                    nodeNameStr
                ));
                nodeNotFoundEntry.setCorrectionFormat(
                    String.format(
                        "Create a node with the name '%s' first.",
                        nodeNameStr
                    )
                );
                nodeNotFoundEntry.putVariable(KEY_NODE_NAME, nodeNameStr);
                nodeNotFoundEntry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                nodeNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

                apiCallRc.addEntry(nodeNotFoundEntry);
                fail = true;
            }
            if (storPoolDfn == null)
            {
                ApiCallRcEntry nodeNotFoundEntry = new ApiCallRcEntry();
                nodeNotFoundEntry.setReturnCode(RC_STOR_POOL_CRT_FAIL_NOT_FOUND_STOR_POOL_DFN);
                nodeNotFoundEntry.setCauseFormat(String.format(
                    "The specified storage pool definition '%s' could not be found in the database",
                    storPoolNameStr
                ));
                nodeNotFoundEntry.setCorrectionFormat(
                    String.format(
                        "Create a storage pool definition with the name '%s' first.",
                        storPoolNameStr
                    )
                );
                nodeNotFoundEntry.putVariable(KEY_NODE_NAME, nodeNameStr);
                nodeNotFoundEntry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                nodeNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

                apiCallRc.addEntry(nodeNotFoundEntry);
                fail = true;
            }
            if (!fail)
            {
                StorPoolData storPool = StorPoolData.getInstance( // accDeniedExc3, dataAlreadyExistsExc1
                    accCtx,
                    node,
                    storPoolDfn,
                    driver,
                    transMgr,
                    true,  // persist to db
                    true   // throw exception if already exists
                );
                storPool.setConnection(transMgr);
                storPool.getConfiguration(accCtx).map().putAll(storPoolPropsMap);
                transMgr.commit();

                String successMessage = String.format(
                    "Storage pool '%s' on node '%s' created successfully",
                    storPoolNameStr,
                    nodeNameStr
                );
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_STOR_POOL_CREATED);
                entry.setMessageFormat(successMessage);
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                entry.putObjRef(KEY_NODE, nodeNameStr);
                entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

                apiCallRc.addEntry(entry);
                controller.getErrorReporter().logInfo(successMessage);

                notifySatellite(storPool, apiCallRc);
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while creating the storage pool '%s' on node '%s'.",
                storPoolNameStr,
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            String action;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (node == null)
            { // accDeniedExc1
                action = String.format("access the node '%s'", nodeNameStr);
                entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_ACC_DENIED_NODE);
            }
            else
            if (storPoolDfn == null)
            { // accDeniedExc2
                action = String.format("access the node '%s'", nodeNameStr);
                entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_ACC_DENIED_STOR_POOL_DFN);
            }
            else
            { // accDeniedExc3
                action = String.format(
                    "create the storage pool '%s' on node '%s'",
                    storPoolNameStr,
                    nodeNameStr
                );
                entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_ACC_DENIED_STOR_POOL);
            }

            String errorMessage = String.format(
                "The access context (user: '%s', role: '%s') has no permission to %s",
                    accCtx.subjectId.name.displayValue,
                    accCtx.subjectRole.name.displayValue,
                    action
            );
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (nodeName == null)
            { // invalidNameExc1
                errorMessage = String.format(
                    "The specified node name '%s' is not a valid.",
                    nodeNameStr
                );
                entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_INVLD_NODE_NAME);
            }
            else
            { // invalidNameExc2
                errorMessage = String.format(
                    "The specified storage pool name '%s' is not a valid.",
                    storPoolNameStr
                );
                entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_INVLD_STOR_POOL_NAME);
            }
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException dataAlreadyExistsExc)
        {
            // dataAlreadyExistsExc0 cannot happen
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage = String.format(
                "The specified storage pool '%s' on node '%s' already exists.",
                storPoolNameStr,
                nodeNameStr
            );
            entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_EXISTS_STOR_POOL);
            controller.getErrorReporter().reportError(
                dataAlreadyExistsExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException implErrExc)
        {
            String errorMessage = String.format(
                "A dynamic class instantiation related exception occured while this method shoud "+
                    "not create such instances (Node name: '%s', storage pool name: '%s')",
                nodeNameStr,
                storPoolNameStr
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    errorMessage,
                    implErrExc
                )
            );
            entry.setMessageFormat(String.format(
                "Failed to create the storage pool '%s' on node '%s' due to an implementation error.",
                    storPoolNameStr,
                    nodeNameStr
                )
            );
            entry.setReturnCode(RC_STOR_POOL_CRT_FAIL_IMPL_ERROR);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while creating a storage pool '%s' on node '%s'. ",
                storPoolNameStr,
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
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
                            + "storage pool '%s' on node '%s'.",
                        storPoolNameStr,
                        nodeNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_STOR_POOL_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
                    entry.putObjRef(ApiConsts.KEY_NODE, nodeNameStr);
                    entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
                    entry.putVariable(ApiConsts.KEY_NODE_NAME, nodeNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

    private void notifySatellite(StorPoolData storPool, ApiCallRcImpl apiCallRc)
    {
        try
        {
            Peer satellitePeer = storPool.getNode().getPeer(apiCtx);
            if (satellitePeer.isConnected())
            {
                Message msg = satellitePeer.createMessage();
                byte[] data = serializer.getChangedMessage(storPool);
                msg.setData(data);
                satellitePeer.sendMessage(msg);
            }
            else
            {
                ApiCallRcEntry notConnected = new ApiCallRcEntry();
                notConnected.setReturnCode(RC_STOR_POOL_CRT_WARN_NOT_CONNECTED);
                notConnected.setMessageFormat("No active connection to satellite '" + storPool.getNode().getName().displayValue + "'");
                notConnected.setDetailsFormat(
                    "The satellite was added and the controller tries to (re-) establish connection to it." +
                    "The controller stored the new StorPool and as soon the satellite is connected, it will " +
                    "receive this update."
                );
                notConnected.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPool.getName().displayValue);
                notConnected.putObjRef(ApiConsts.KEY_NODE, storPool.getNode().getName().displayValue);
                notConnected.putVariable(ApiConsts.KEY_NODE_NAME, storPool.getNode().getName().displayValue);
                apiCallRc.addEntry(notConnected);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to contact all satellites about a resource change",
                    accDeniedExc
                )
            );
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller could not send send a message to target node",
                    illegalMessageStateExc
                )
            );
        }
    }

    public ApiCallRc deleteStorPool(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;

        NodeName nodeName = null;
        StorPoolName storPoolName = null;

        NodeData node = null;
        StorPoolDefinitionData storPoolDfn = null;
        StorPoolData storPool = null;

        try
        {
            boolean fail = false;
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName = new NodeName(nodeNameStr);
            storPoolName = new StorPoolName(storPoolNameStr);

            node = NodeData.getInstance( // accDeniedExc1, dataAlreadyExistsExc0
                accCtx,
                nodeName,
                null, // type  is  only relevant if we want to persist
                null, // flags are only relevant if we want to persist
                transMgr,
                false, // do not persist
                false  // do not throw exception if exists
            );
            storPoolDfn = StorPoolDefinitionData.getInstance( // accDeniedExc2, dataAlreadyExistsExc0
                accCtx,
                storPoolName,
                transMgr,
                false, // do not persist
                false  // do not throw exception if exists
            );
            storPool = StorPoolData.getInstance( // accDeniedExc0, dataAlreadyExistsExc1
                accCtx,
                node,
                storPoolDfn,
                null,  // driver only needed for creation
                transMgr,
                false, // do not persist
                false  // do not throw exception if exists
            );
            if (node == null)
            {
                ApiCallRcEntry nodeNotFoundEntry = new ApiCallRcEntry();
                nodeNotFoundEntry.setReturnCode(RC_STOR_POOL_DEL_FAIL_NOT_FOUND_NODE);
                nodeNotFoundEntry.setCauseFormat(String.format(
                    "The specified node '%s' could not be found in the database",
                    nodeNameStr
                ));
                nodeNotFoundEntry.putVariable(KEY_NODE_NAME, nodeNameStr);
                nodeNotFoundEntry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                nodeNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

                apiCallRc.addEntry(nodeNotFoundEntry);
                fail = true;
            }
            if (storPoolDfn == null)
            {
                ApiCallRcEntry storPoolDfnNotFoundEntry = new ApiCallRcEntry();
                storPoolDfnNotFoundEntry.setReturnCode(RC_STOR_POOL_DEL_FAIL_NOT_FOUND_STOR_POOL_DFN);
                storPoolDfnNotFoundEntry.setCauseFormat(String.format(
                    "The specified storage pool definition '%s' could not be found in the database",
                    storPoolNameStr
                ));
                storPoolDfnNotFoundEntry.putVariable(KEY_NODE_NAME, nodeNameStr);
                storPoolDfnNotFoundEntry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                storPoolDfnNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                storPoolDfnNotFoundEntry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

                apiCallRc.addEntry(storPoolDfnNotFoundEntry);
                fail = true;
            }
            Collection<Volume> volumes = storPool.getVolumes(accCtx); // accDeniedExc0
            if (!volumes.isEmpty())
            {
                StringBuilder volListSb = new StringBuilder();
                for (Volume vol : volumes)
                {
                    volListSb.append("\n   Node name: '")
                             .append(vol.getResource().getAssignedNode().getName().displayValue)
                             .append("', resource name: '")
                             .append(vol.getResource().getDefinition().getName().displayValue)
                             .append("', volume number: ")
                             .append(vol.getVolumeDefinition().getVolumeNumber().value);
                }

                ApiCallRcEntry storPoolInUse = new ApiCallRcEntry();
                storPoolInUse.setReturnCode(RC_STOR_POOL_DEL_FAIL_IN_USE);
                storPoolInUse.setCauseFormat(String.format(
                    "The specified storage pool '%s' on node '%s' can not be deleted as the following " +
                        "volumes are still using it: %s",
                    storPoolNameStr,
                    nodeNameStr,
                    volListSb.toString()
                ));
                storPoolInUse.setCorrectionFormat(
                    volumes.size() == 1 ?
                        "Delete the listed volume first." :
                        "Delete the listed volumes first."
                );
                storPoolInUse.putVariable(KEY_NODE_NAME, nodeNameStr);
                storPoolInUse.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                storPoolInUse.putObjRef(KEY_NODE, nodeNameStr);
                storPoolInUse.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

                apiCallRc.addEntry(storPoolInUse);

                fail = true;
            }


            if (!fail)
            {
                storPool.setConnection(transMgr);
                storPool.delete(accCtx);

                transMgr.commit();

                String successMessage = String.format(
                    "Storage pool '%s' on node '%s' successfully deleted.",
                    storPoolNameStr,
                    nodeNameStr
                );
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_STOR_POOL_DELETED);
                entry.setMessageFormat(successMessage);
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                entry.putObjRef(KEY_NODE, nodeNameStr);
                entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

                apiCallRc.addEntry(entry);
                controller.getErrorReporter().logInfo(successMessage);
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while deleting the storage pool '%s' on node '%s'.",
                storPoolNameStr,
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_STOR_POOL_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            String action;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (node == null)
            { // accDeniedExc1
                action = String.format("access the node '%s'", nodeNameStr);
                entry.setReturnCodeBit(RC_STOR_POOL_DEL_FAIL_ACC_DENIED_NODE);
            }
            else
            if (storPoolDfn == null)
            { // accDeniedExc2
                action = String.format("access the node '%s'", nodeNameStr);
                entry.setReturnCodeBit(RC_STOR_POOL_DEL_FAIL_ACC_DENIED_STOR_POOL_DFN);
            }
            else
            { // accDeniedExc3 & accDeniedExc4
                action = String.format(
                    "delete the storage pool '%s' on node '%s'",
                    storPoolNameStr,
                    nodeNameStr
                );
                entry.setReturnCodeBit(RC_STOR_POOL_DEL_FAIL_ACC_DENIED_STOR_POOL);
            }

            String errorMessage = String.format(
                "The access context (user: '%s', role: '%s') has no permission to %s",
                    accCtx.subjectId.name.displayValue,
                    accCtx.subjectRole.name.displayValue,
                    action
            );
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (nodeName == null)
            { // invalidNameExc1
                errorMessage = String.format(
                    "The specified node name '%s' is not a valid.",
                    nodeNameStr
                );
                entry.setReturnCodeBit(RC_STOR_POOL_DEL_FAIL_INVLD_NODE_NAME);
            }
            else
            { // invalidNameExc2
                errorMessage = String.format(
                    "The specified storage pool name '%s' is not a valid.",
                    storPoolNameStr
                );
                entry.setReturnCodeBit(RC_STOR_POOL_DEL_FAIL_INVLD_STOR_POOL_NAME);
            }
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (ClassNotFoundException | InstantiationException |
            IllegalAccessException | DrbdDataAlreadyExistsException implErrExc)
        {
            String errorMessage;
            if (implErrExc instanceof DrbdDataAlreadyExistsException)
            { // dataAlreadyExistsExc0 cannot happen
                errorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an AlreadyExistsException "+
                        "(node name: '%s', storage pool name: '%s')",
                    nodeNameStr,
                    storPoolNameStr
                );
            }
            else
            {
                errorMessage= String.format(
                    "A dynamic class instantiation related exception occured while this method shoud "+
                        "not create such instances (Node name: '%s', storage pool name: '%s')",
                    nodeNameStr,
                    storPoolNameStr
                );
            }
            ApiCallRcEntry entry = new ApiCallRcEntry();
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    errorMessage,
                    implErrExc
                )
            );
            entry.setMessageFormat(String.format(
                "Failed to create the storage pool '%s' on node '%s' due to an implementation error.",
                    storPoolNameStr,
                    nodeNameStr
                )
            );
            entry.setReturnCode(RC_STOR_POOL_DEL_FAIL_IMPL_ERROR);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while deleting a storage pool '%s' on node '%s'. ",
                storPoolNameStr,
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_STOR_POOL_DEL_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
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
                            + "storage pool '%s' on node '%s'.",
                        storPoolNameStr,
                        nodeNameStr
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
                    entry.putObjRef(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);
                    entry.putObjRef(ApiConsts.KEY_NODE, nodeNameStr);
                    entry.putVariable(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr);
                    entry.putVariable(ApiConsts.KEY_NODE_NAME, nodeNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

    public void respondStorPool(String storPoolNameStr, UUID storPoolUuid, int msgId, Peer satellitePeer)
    {
        try
        {
            StorPoolName storPoolName = new StorPoolName(storPoolNameStr);

            StorPool storPool = satellitePeer.getNode().getStorPool(apiCtx, storPoolName);
            // TODO: check if the storPool has the same uuid as storPoolUuid
            if (storPool != null)
            {
                byte[] data = serializer.getDataMessage(msgId, storPool);

                Message response = satellitePeer.createMessage();
                response.setData(data);
                satellitePeer.sendMessage(response);
            }
            else
            {
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        String.format(
                            "A requested storpool name '%s' with the uuid '%s' was not found "+
                                "in the controllers list of stor pools",
                                storPoolName,
                                storPoolUuid.toString()
                            ),
                        null
                    )
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite requested data for invalid storpool name '" + storPoolNameStr + "'.",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested storpool data.",
                    accDeniedExc
                )
            );
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to respond to storpool data request",
                    illegalMessageStateExc
                )
            );
        }
    }

}

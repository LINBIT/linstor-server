package com.linbit.drbdmanage.core;

import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CREATED;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_ACC_DENIED_RSC_DFN;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_ACC_DENIED_VLM_DFN;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_EXISTS_RSC_DFN;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_EXISTS_VLM_DFN;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_INVLD_MINOR_NR;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_INVLD_RSC_NAME;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_INVLD_VLM_NR;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_INVLD_VLM_SIZE;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_SQL;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_CRT_FAIL_SQL_ROLLBACK;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_DELETED;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_DEL_FAIL_ACC_DENIED_RSC_DFN;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_DEL_FAIL_EXISTS_IMPL_ERROR;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_DEL_FAIL_INVLD_RSC_NAME;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_DEL_FAIL_SQL;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_DEL_FAIL_SQL_ROLLBACK;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_RSC_DFN_DEL_NOT_FOUND;
import static com.linbit.drbdmanage.ApiCallRcConstants.RC_VLM_DFN_CREATED;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_AL_SIZE;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_AL_STRIPES;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_ID;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_MINOR_NR;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_PEER_COUNT;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_ROLE;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_RSC_DFN;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_RSC_NAME;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_VLM_NR;
import static com.linbit.drbdmanage.api.ApiConsts.KEY_VLM_SIZE;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiCallRcImpl;
import com.linbit.drbdmanage.DrbdDataAlreadyExistsException;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.ResourceDefinition.RscDfnFlags;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnApi;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;

class CtrlRscDfnApiCallHandler
{
    private Controller controller;

    CtrlRscDfnApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
    }

    public ApiCallRc createResourceDefinition(
        AccessContext accCtx,
        Peer client,
        String resourceName,
        Map<String, String> props,
        List<VlmDfnApi> volDescrMap
    )
    {
        /*
         * Usually its better to handle exceptions "close" to their appearance.
         * However, as in this method almost every other line throws an exception,
         * the code would get completely unreadable; thus, unmaintainable.
         *
         * For that reason there is (almost) only one try block with many catches, and
         * those catch blocks handle the different cases (commented as <some>Exc<count> in
         * the try block and a matching "handle <some>Exc<count>" in the catch block)
         */

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        ResourceDefinition rscDfn = null;
        TransactionMgr transMgr = null;

        VolumeNumber volNr = null;
        MinorNumber minorNr = null;
        VolumeDefinition.VlmDfnApi currentVolCrtData = null;

        short peerCount = getAsShort(props, KEY_PEER_COUNT, controller.getDefaultPeerCount());
        int alStripes = getAsInt(props, KEY_AL_STRIPES, controller.getDefaultAlStripes());
        long alStripeSize = getAsLong(props, KEY_AL_SIZE, controller.getDefaultAlSize());

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection()); // sqlExc1

            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE); // accDeniedExc1

            RscDfnFlags[] rscDfnInitFlags = null;

            rscDfn = ResourceDefinitionData.getInstance( // sqlExc2, accDeniedExc1 (same as last line), alreadyExistsExc1
                accCtx,
                new ResourceName(resourceName), // invalidNameExc1
                rscDfnInitFlags,
                transMgr,
                true,
                true
            );

            for (VolumeDefinition.VlmDfnApi volCrtData : volDescrMap)
            {
                currentVolCrtData = volCrtData;

                volNr = null;
                minorNr = null;

                volNr = new VolumeNumber(volCrtData.getVolumeNr()); // valOORangeExc1
                minorNr = new MinorNumber(volCrtData.getMinorNr()); // valOORangeExc2

                long size = volCrtData.getSize();

                // getGrossSize performs check and throws exception when something is invalid
                controller.getMetaDataApi().getGrossSize(size, peerCount, alStripes, alStripeSize);
                // mdExc1

                VlmDfnFlags[] vlmDfnInitFlags = null;

                VolumeDefinitionData.getInstance( // mdExc2, sqlExc3, accDeniedExc2, alreadyExistsExc2
                    accCtx,
                    rscDfn,
                    volNr,
                    minorNr,
                    size,
                    vlmDfnInitFlags,
                    transMgr,
                    true,
                    true
                );
            }

            transMgr.commit(); // sqlExc4

            controller.rscDfnMap.put(rscDfn.getName(), rscDfn);

            for (VolumeDefinition.VlmDfnApi volCrtData : volDescrMap)
            {
                ApiCallRcEntry volSuccessEntry = new ApiCallRcEntry();
                volSuccessEntry.setReturnCode(RC_VLM_DFN_CREATED);
                volSuccessEntry.setMessageFormat(
                    String.format(
                        "Volume definition with number %d and minor number %d successfully " +
                            " created in resource definition '%s'.",
                        volCrtData.getVolumeNr(),
                        volCrtData.getMinorNr(),
                        resourceName
                    )
                );
                volSuccessEntry.putVariable(KEY_RSC_DFN, resourceName);
                volSuccessEntry.putVariable(KEY_VLM_NR, Integer.toString(volCrtData.getVolumeNr()));
                volSuccessEntry.putVariable(KEY_MINOR_NR, Integer.toString(volCrtData.getMinorNr()));
                volSuccessEntry.putObjRef(KEY_RSC_DFN, resourceName);
                volSuccessEntry.putObjRef(KEY_VLM_NR, Integer.toString(volCrtData.getVolumeNr()));

                apiCallRc.addEntry(volSuccessEntry);
            }

            ApiCallRcEntry successEntry = new ApiCallRcEntry();

            String successMsg = String.format(
                "Resource definition '%s' successfully created.",
                resourceName
            );
            successEntry.setReturnCode(RC_RSC_DFN_CREATED);
            successEntry.setMessageFormat(successMsg);
            successEntry.putVariable(KEY_RSC_NAME, resourceName);
            successEntry.putVariable(KEY_PEER_COUNT, Short.toString(peerCount));
            successEntry.putVariable(KEY_AL_STRIPES, Integer.toString(alStripes));
            successEntry.putVariable(KEY_AL_SIZE, Long.toString(alStripeSize));
            successEntry.putObjRef(KEY_RSC_DFN, resourceName);

            apiCallRc.addEntry(successEntry);
            controller.getErrorReporter().logInfo(successMsg);
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while creating the resource definition '%s'.",
                resourceName
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, resourceName);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String action;
            if (rscDfn == null)
            { // handle accDeniedExc1

                action = "create a new resource definition.";
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_ACC_DENIED_RSC_DFN);
            }
            else
            { // handle accDeniedExc2
                action = String.format(
                    "create a new volume definition for resource definition '%s'.",
                    resourceName
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_ACC_DENIED_VLM_DFN);
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVolCrtData.getMinorNr()));
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
            }
            String errorMessage = String.format(
                "The access context (user: %s, role: %s) has no permission to %s",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                action
            );
            controller.getErrorReporter().reportError(
                accExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accExc.getMessage());
            entry.putVariable(KEY_ID, accCtx.subjectId.name.displayValue);
            entry.putVariable(KEY_ROLE, accCtx.subjectRole.name.displayValue);
            entry.putObjRef(KEY_RSC_DFN, resourceName);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException nameExc)
        {
            // handle invalidNameExc1

            String errorMessage = String.format(
                "The specified resource name '%s' is not a valid.",
                resourceName
            );
            controller.getErrorReporter().reportError(
                nameExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_RSC_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(errorMessage);
            entry.putVariable(KEY_RSC_NAME, resourceName);
            entry.putObjRef(KEY_RSC_DFN, resourceName);

            apiCallRc.addEntry(entry);
        }
        catch (ValueOutOfRangeException valOORangeExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage;
            if (volNr == null)
            { // handle valOORangeExc1
                errorMessage = String.format(
                    "The specified volume number %d is invalid.",
                    currentVolCrtData.getVolumeNr()
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_VLM_NR);
            }
            else
            { // handle valOORangeExc2
                errorMessage = String.format(
                    "The specified minor number %d is invalid.",
                    currentVolCrtData.getMinorNr()
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_MINOR_NR);
                entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
            }
            controller.getErrorReporter().reportError(
                valOORangeExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.putVariable(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
            entry.putVariable(KEY_RSC_DFN, resourceName);
            entry.putObjRef(KEY_RSC_DFN, resourceName);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));

            apiCallRc.addEntry(entry);
        }
        catch (MdException metaDataExc)
        {
            // handle mdExc1 and mdExc2
            String errorMessage = String.format(
                "The specified volume size %d is invalid.",
                currentVolCrtData.getSize()
            );
            controller.getErrorReporter().reportError(
                metaDataExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_VLM_SIZE);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(metaDataExc.getMessage());
            entry.putVariable(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
            entry.putVariable(KEY_RSC_DFN, resourceName);
            entry.putVariable(KEY_VLM_SIZE, Long.toString(currentVolCrtData.getSize()));
            entry.putObjRef(KEY_RSC_DFN, resourceName);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException alreadyExistsExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMsg;
            if (rscDfn == null)
            {
                // handle alreadyExists1
                errorMsg = String.format(
                    "A resource definition with the name '%s' already exists.",
                    resourceName
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_EXISTS_RSC_DFN);
            }
            else
            {
                // handle alreadyExists2
                errorMsg = String.format(
                    "A volume definition with the numer %d already exists in resource definition '%s'.",
                    currentVolCrtData.getVolumeNr(),
                    resourceName
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_EXISTS_VLM_DFN);
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVolCrtData.getMinorNr()));
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
            }
            controller.getErrorReporter().reportError(
                alreadyExistsExc,
                accCtx,
                client,
                errorMsg
            );
            entry.setMessageFormat(errorMsg);
            entry.setCauseFormat(alreadyExistsExc.getMessage());
            entry.putVariable(KEY_RSC_NAME, resourceName);
            entry.putObjRef(KEY_RSC_DFN, resourceName);
            apiCallRc.addEntry(entry);
        }

        if (transMgr != null && transMgr.isDirty())
        {
            // not committed -> error occurred
            try
            {
                transMgr.rollback();
            }
            catch (SQLException sqlExc)
            {
                String errorMessage = String.format(
                    "A database error occured while trying to rollback the creation of " +
                        "resource definition '%s'.",
                    resourceName
                );
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_SQL_ROLLBACK);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
        }
        return apiCallRc;
    }

    public ApiCallRc deleteResourceDefinition(AccessContext accCtx, Peer client, String resNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;
        ResourceName resName = null;
        ResourceDefinitionData resDfn = null;

        try
        {
            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE); // accDeniedExc1
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection()); // sqlExc1

            resName = new ResourceName(resNameStr); // invalidNameExc1
            resDfn = ResourceDefinitionData.getInstance( // accDeniedExc2, sqlExc2, dataAlreadyExistsExc1
                accCtx,
                resName,
                null,
                transMgr,
                false,
                false
            );

            if (resDfn != null)
            {
                resDfn.setConnection(transMgr);
                resDfn.markDeleted(accCtx); // accDeniedExc3, sqlExc3

                transMgr.commit(); // sqlExc4

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_DELETED);
                entry.setMessageFormat(
                    String.format(
                        "Resource definition '%s' successfully deleted",
                        resNameStr
                    )
                );
                entry.putObjRef(KEY_RSC_DFN, resNameStr);
                entry.putVariable(KEY_RSC_NAME, resNameStr);
                apiCallRc.addEntry(entry);

                // TODO: tell satellites to remove all the corresponding resources
                // TODO: if satellites are finished (or no satellite had such a resource deployed)
                //       remove the rscDfn from the DB
                controller.getErrorReporter().logInfo(
                    "Resource definition '%s' marked to be deleted",
                    resNameStr
                );
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_DEL_NOT_FOUND);
                entry.setMessageFormat(
                    String.format(
                        "Resource definition '%s' was not deleted as it was not found",
                        resNameStr
                    )
                );
                entry.putObjRef(KEY_RSC_DFN, resNameStr);
                entry.putVariable(KEY_RSC_NAME, resNameStr);
                apiCallRc.addEntry(entry);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            String errorMessage = String.format(
                "The access context (user: %s, role: %s) has no permission to " +
                    "delete the resource definition '%s'.",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                resNameStr
            );
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_ACC_DENIED_RSC_DFN);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, resNameStr);
            entry.putVariable(KEY_RSC_NAME, resNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (SQLException sqlExc)
        {
            String errorMessge = String.format(
                "A database error occured while deleting the resource definition '%s'.",
                resNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessge
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessge);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, resNameStr);
            entry.putVariable(KEY_RSC_DFN, resNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        { // handle invalidNameExc1
            String errorMessage = String.format(
                "The given resource name '%s' is invalid.",
                resNameStr
            );
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_INVLD_RSC_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putObjRef(KEY_RSC_DFN, resNameStr);
            entry.putVariable(KEY_RSC_NAME, resNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException dataAlreadyExistsExc)
        { // handle drbdAlreadyExistsExc1
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    String.format(
                        ".getInstance was called with failIfExists=false, still threw an AlreadyExistsException "+
                            "(Resource name: %s)",
                        resNameStr
                    ),
                    dataAlreadyExistsExc
                )
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_EXISTS_IMPL_ERROR);
            entry.setMessageFormat(
                String.format(
                    "Failed to delete the resource definition '%s' due to an implementation error.",
                    resNameStr
                )
            );
            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, resNameStr);
            entry.putVariable(KEY_RSC_NAME, resNameStr);

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
                            "resource definition '%s'.",
                        resNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_RSC_DFN, resNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }


    private short getAsShort(Map<String, String> props, String key, short defaultValue)
    {
        short ret = defaultValue;
        String value = props.get(key);
        if (value != null)
        {
            try
            {
                ret = Short.parseShort(value);
            }
            catch (NumberFormatException numberFormatExc)
            {
                // ignore and return the default value
            }
        }
        return ret;
    }

    private int getAsInt(Map<String, String> props, String key, int defaultValue)
    {
        int ret = defaultValue;
        String value = props.get(key);
        if (value != null)
        {
            try
            {
                ret = Integer.parseInt(value);
            }
            catch (NumberFormatException numberFormatExc)
            {
                // ignore and return the default value
            }
        }
        return ret;
    }

    private long getAsLong(Map<String, String> props, String key, long defaultValue)
    {
        long ret = defaultValue;
        String value = props.get(key);
        if (value != null)
        {
            try
            {
                ret = Long.parseLong(value);
            }
            catch (NumberFormatException numberFormatExc)
            {
                // ignore and return the default value
            }
        }
        return ret;
    }
}

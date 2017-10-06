package com.linbit.drbdmanage.core;

import static com.linbit.drbdmanage.api.ApiConsts.*;
import static com.linbit.drbdmanage.ApiCallRcConstants.*;

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
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeId;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.Volume.VlmApi;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnApi;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.ResourceDefinition.RscDfnFlags;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;

class CtrlResourceApiCallHandler
{
    private final Controller controller;

    CtrlResourceApiCallHandler(Controller controllerRef)
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
        VolumeDefinition lastVolDfn = null;

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

                lastVolDfn = null;
                volNr = null;
                minorNr = null;

                volNr = new VolumeNumber(volCrtData.getVolumeNr()); // valOORangeExc1
                minorNr = new MinorNumber(volCrtData.getMinorNr()); // valOORangeExc2

                long size = volCrtData.getSize();

                // getGrossSize performs check and throws exception when something is invalid
                controller.getMetaDataApi().getGrossSize(size, peerCount, alStripes, alStripeSize);
                // mdExc1

                VlmDfnFlags[] vlmDfnInitFlags = null;

                lastVolDfn = VolumeDefinitionData.getInstance( // mdExc2, sqlExc3, accDeniedExc2, alreadyExistsExc2
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
                        "Volume Definition with number ${%s} and minor number ${%s} successfully created",
                        KEY_VLM_NR,
                        KEY_MINOR_NR
                    )
                );
                volSuccessEntry.putVariable(KEY_VLM_NR, Integer.toString(volCrtData.getVolumeNr()));
                volSuccessEntry.putVariable(KEY_MINOR_NR, Integer.toString(volCrtData.getMinorNr()));
                volSuccessEntry.putObjRef(KEY_RSC_DFN, resourceName);
                volSuccessEntry.putObjRef(KEY_VLM_NR, Integer.toString(volCrtData.getVolumeNr()));

                apiCallRc.addEntry(volSuccessEntry);
            }

            ApiCallRcEntry successEntry = new ApiCallRcEntry();

            successEntry.setReturnCode(RC_RSC_DFN_CREATED);
            successEntry.setMessageFormat("Resource definition '${" + KEY_RSC_NAME + "}' successfully created.");
            successEntry.putVariable(KEY_RSC_NAME, resourceName);
            successEntry.putVariable(KEY_PEER_COUNT, Short.toString(peerCount));
            successEntry.putVariable(KEY_AL_STRIPES, Integer.toString(alStripes));
            successEntry.putVariable(KEY_AL_SIZE, Long.toString(alStripeSize));
            successEntry.putObjRef(KEY_RSC_DFN, resourceName);

            apiCallRc.addEntry(successEntry);
            controller.getErrorReporter().logInfo(
                "Resource definition [%s] successfully created",
                resourceName
            );
        }
        catch (SQLException sqlExc)
        {
            if (transMgr == null)
            { // handle sqlExc1
                String errorMessage = "A database error occured while trying to create a new transaction.";
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
            else
            if (rscDfn == null)
            { // handle sqlExc2
                String errorMessage = "A database error occured while trying to create a new resource definition. " +
                    "Resource name: " + resourceName;
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
            else
            if (lastVolDfn == null)
            { // handle sqlExc3
                String errorMessage = "A database error occured while trying to create a new volume definition for resource definition: " +
                    resourceName + ".";
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());
                if (currentVolCrtData != null)
                {
                    entry.putVariable(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                    entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVolCrtData.getMinorNr()));
                    entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                }
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
            else
            if (transMgr.isDirty())
            { // handle sqlExc4
                String errorMessage = "A database error occured while trying to commit the transaction.";
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
        }
        catch (AccessDeniedException accExc)
        {
            if (rscDfn == null)
            { // handle accDeniedExc1

                String errorMessage = "The given access context has no permission to create a new resource definition";
                controller.getErrorReporter().reportError(
                    accExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(accExc.getMessage());
                entry.setDetailsFormat("The access-context (user: ${" + KEY_ID + " }, role: ${" + KEY_ROLE + "}) "
                    + "has not enough rights to create a new resource definition");
                entry.putVariable(KEY_ID, accCtx.subjectId.name.displayValue);
                entry.putVariable(KEY_ROLE, accCtx.subjectRole.name.displayValue);
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
            else
            if (lastVolDfn == null)
            { // handle accDeniedExc2
                String errorMessage = "The given access context has no permission to create a resource definition";
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        "Could not create volume definition for a newly created resource definition",
                        accExc
                    ),
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(accExc.getMessage());
                entry.setDetailsFormat("The access-context (user: ${" + KEY_ID + " }, role: ${" + KEY_ROLE + "}) "
                    + "has not enough rights to create a new resource definition");
                entry.putVariable(KEY_ID, accCtx.subjectId.name.displayValue);
                entry.putVariable(KEY_ROLE, accCtx.subjectRole.name.displayValue);
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
        }
        catch (InvalidNameException nameExc)
        {
            // handle invalidNameExc1

            String errorMessage = "The specified name is not valid for use as a resource name.";
            controller.getErrorReporter().reportError(
                nameExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat("The given resource name '${" + KEY_RSC_NAME + "}'is invalid");
            entry.putVariable(KEY_RSC_NAME, resourceName);
            entry.putObjRef(KEY_RSC_DFN, resourceName);

            apiCallRc.addEntry(entry);
        }
        catch (ValueOutOfRangeException valOORangeExc)
        {
            if (volNr == null)
            { // handle valOORangeExc1

                String errorMessage = "The specified volume number is invalid.";
                controller.getErrorReporter().reportError(
                    valOORangeExc,
                    accCtx,
                    client,
                    errorMessage
                );
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat("Given volume number ${" + KEY_VLM_NR + "} was invalid");
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                entry.putObjRef(KEY_RSC_DFN, resourceName);
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));

                apiCallRc.addEntry(entry);
            }
            else
            if (minorNr == null)
            { // handle valOORangeExc2
                String errorMessage = "The specified minor number is invalid.";
                controller.getErrorReporter().reportError(
                    valOORangeExc,
                    accCtx,
                    client,
                    errorMessage
                );
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat("Given minor number ${" + KEY_MINOR_NR + "} was invalid");
                entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                entry.putObjRef(KEY_RSC_DFN, resourceName);
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));

                apiCallRc.addEntry(entry);
            }
        }
        catch (MdException metaDataExc)
        {
            // handle mdExc1 and mdExc2

            String errorMessage = "The specified volume size is invalid.";
            controller.getErrorReporter().reportError(
                metaDataExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat("Given volume size ${" + KEY_VLM_SIZE + "} was invalid");
            entry.putVariable(KEY_VLM_SIZE, Long.toString(currentVolCrtData.getSize()));
            entry.putObjRef(KEY_RSC_DFN, resourceName);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException alreadyExistsExc)
        {
            if (rscDfn == null)
            {
                // handle alreadyExists1
                controller.getErrorReporter().reportError(
                    alreadyExistsExc,
                    accCtx,
                    client,
                    String.format(
                        "The resource definition '%s' be created already exists.",
                        resourceName
                    )
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat("The resource definition '${" + KEY_RSC_NAME + "}' already exists");
                entry.setCauseFormat(alreadyExistsExc.getMessage());
                entry.putVariable(KEY_RSC_NAME, resourceName);
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
            else
            {
                // handle alreadyExists2
                controller.getErrorReporter().reportError(
                    alreadyExistsExc,
                    accCtx,
                    client,
                    "A volume definition which should be created already exists"
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat("A volume definition already exists in resource definition '${" + KEY_RSC_NAME + "}'");
                entry.setCauseFormat(alreadyExistsExc.getMessage());
                if (currentVolCrtData != null)
                {
                    entry.putVariable(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                    entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVolCrtData.getMinorNr()));
                    entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                }
                entry.putVariable(KEY_RSC_NAME, resourceName);
                entry.putObjRef(KEY_RSC_DFN, resourceName);

                apiCallRc.addEntry(entry);
            }
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
                String errorMessage = "A database error occured while trying to rollback the transaction.";
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
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

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_DELETED);
                entry.setMessageFormat("Resource definition ${" + KEY_RSC_NAME + "} successfully deleted");
                entry.putObjRef(KEY_RSC_DFN, resNameStr);
                entry.putVariable(KEY_RSC_NAME, resNameStr);
                apiCallRc.addEntry(entry);

                transMgr.commit(); // sqlExc4

                // TODO: tell satellites to remove all the corresponding resources
                // TODO: if satellites are finished (or no satellite had such a resource deployed)
                //       remove the rscDfn from the DB
                controller.getErrorReporter().logInfo(
                    "Resource definition [%s] marked to be deleted",
                    resNameStr
                );
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_NOT_FOUND);
                entry.setMessageFormat("Resource definition ${" + KEY_RSC_NAME + "} was not deleted as it was not found");
                entry.putObjRef(KEY_RSC_DFN, resNameStr);
                entry.putVariable(KEY_RSC_NAME, resNameStr);
                apiCallRc.addEntry(entry);

                controller.getErrorReporter().logInfo(
                    "Non existing reource definition [%s] could not be deleted",
                    resNameStr
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        { // handle accDeniedExc1 && accDeniedExc2 && accDeniedExc3
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                "The given access context has no permission to create a new resource definition"
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DELETION_FAILED);
            entry.setMessageFormat(
                "The given access context has no permission to delete the resource definition ${" +
                    KEY_NODE_NAME + "}."
            );
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, resNameStr);
            entry.putVariable(KEY_RSC_NAME, resNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (SQLException sqlExc)
        {
            if (transMgr == null)
            { // handle sqlExc1
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    "A database error occured while trying to create a new transaction."
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_DELETION_FAILED);
                entry.setMessageFormat("Failed to create database transaction");
                entry.setCauseFormat(sqlExc.getMessage());
                entry.putObjRef(KEY_RSC_DFN, resNameStr);

                apiCallRc.addEntry(entry);
            }
            else
            if (resDfn == null)
            { // handle sqlExc2
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    String.format(
                        "A database error occured while trying to load the resource definition '%s'.",
                        resNameStr
                    )
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DFN_CREATION_FAILED);
                entry.setMessageFormat("Failed to load resource definition ${" + KEY_RSC_NAME + "} for deletion.");
                entry.setCauseFormat(sqlExc.getMessage());
                entry.putObjRef(KEY_RSC_DFN, resNameStr);
                entry.putVariable(KEY_RSC_NAME, resNameStr);

                apiCallRc.addEntry(entry);
            }
            else
            {
                Throwable ex = null;
                try
                {
                    if (resDfn.getFlags().isSet(accCtx, RscDfnFlags.DELETE))
                    { // handle sqlExc3
                        ex = sqlExc;
                    }
                }
                catch (AccessDeniedException accDeniedExc)
                { // handle sqlExc3's accDeniedExc
                    ex = new ImplementationError(
                        "Mark delete was authorized (sqlExc, not accDeniedExc is thrown), but check mark deleted (getFlags) was not authorized",
                        sqlExc
                    );
                }
                if (ex != null)
                {
                    controller.getErrorReporter().reportError(
                        ex,
                        accCtx,
                        client,
                        "A database error occured while trying to mark the resource definition to be deleted."
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_DFN_DELETION_FAILED);
                    entry.setMessageFormat("Failed to mark the resource definition ${" + KEY_RSC_NAME + "} to be deleted.");
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_RSC_DFN, resNameStr);
                    entry.putVariable(KEY_RSC_NAME, resNameStr);

                    apiCallRc.addEntry(entry);

                }
                else
                { // handle sqlExc4
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        "A database error occured while trying to commit the transaction."
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_DFN_DELETION_FAILED);
                    entry.setMessageFormat("Failed to commit transaction");
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_RSC_DFN, resNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
        }
        catch (InvalidNameException invalidNameExc)
        { // handle invalidNameExc1
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                "The given name for the resource definition is invalid"
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DELETION_FAILED);
            entry.setMessageFormat("The given resource definition name '${" + KEY_RSC_NAME + "}' is invalid");
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putObjRef(KEY_RSC_DFN, resNameStr);
            entry.putVariable(KEY_RSC_NAME, resNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException dataAlreadyExistsExc)
        { // handle drbdAlreadyExistsExc1
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    ".getInstance was called with failIfExists=false, still threw an AlreadyExistsException",
                    dataAlreadyExistsExc
                )
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DELETION_FAILED);
            entry.setMessageFormat("Failed to delete the resource definition ${" + KEY_RSC_NAME + "} due to an implementation error.");
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
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        "A database error occured while trying to rollback the transaction."
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_DFN_DELETION_FAILED);
                    entry.setMessageFormat("Failed to rollback database transaction");
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_RSC_DFN, resNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

    public ApiCallRc createResource(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr,
        int nodeIdRaw,
        Map<String, String> rscProps,
        List<VlmApi> vlmApiList
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        TransactionMgr transMgr = null;

        NodeName nodeName = null;
        ResourceName rscName = null;

        ResourceDefinitionData rscDfn = null;
        NodeData node = null;

        NodeId nodeId = null;

        ResourceData rsc = null;
        VlmApi currentVlmApi = null;
        VolumeNumber volNr = null;
        VolumeDefinition vlmDfn = null;

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName = new NodeName(nodeNameStr);
            rscName = new ResourceName(rscNameStr);

            node = NodeData.getInstance( // accDeniedExc1, dataAlreadyExistsExc0
                accCtx,
                nodeName,
                null,
                null,
                transMgr,
                false,
                false
            );
            rscDfn = ResourceDefinitionData.getInstance( // accDeniedExc2, dataAlreadyExistsExc0
                accCtx,
                rscName,
                null,
                transMgr,
                false,
                false
            );

            if (node == null)
            {
                ApiCallRcEntry nodeNotFoundEntry = new ApiCallRcEntry();
                nodeNotFoundEntry.setReturnCode(RC_RSC_CRT_FAIL_NODE_NOT_FOUND);
                nodeNotFoundEntry.setCauseFormat("The specified node '${" + KEY_NODE_NAME + "}' " +
                    "could not be found in the database");
                nodeNotFoundEntry.setCorrectionFormat("Create a node with the name "+
                    "'${" + KEY_NODE_NAME + "}' first.");
                nodeNotFoundEntry.putVariable(KEY_NODE_NAME, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
            if (rscDfn == null)
            {
                ApiCallRcEntry rscNotFoundEntry = new ApiCallRcEntry();
                rscNotFoundEntry.setReturnCode(RC_RSC_CRT_FAIL_RSC_DFN_NOT_FOUND);
                rscNotFoundEntry.setCauseFormat("The specified resource '${" + KEY_RSC_NAME + "}' " +
                    "could not be found in the database");
                rscNotFoundEntry.setCorrectionFormat("Create a resource definition with the name "+
                    "'${" + KEY_RSC_NAME + "}' first.");
                rscNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                rscNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                rscNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                apiCallRc.addEntry(rscNotFoundEntry);
            }
            else
            {
                nodeId = new NodeId(nodeIdRaw);

                RscFlags[] initFlags = null;

                ApiCallRcImpl successApiCallRc = new ApiCallRcImpl();

                rsc = ResourceData.getInstance( // accDeniedExc3, dataAlreadyExistsExc1
                    accCtx,
                    rscDfn,
                    node,
                    nodeId,
                    initFlags,
                    transMgr,
                    true,
                    true
                );

                ApiCallRcEntry rscSuccess = new ApiCallRcEntry();
                rscSuccess.setMessageFormat("Resource '"+ rscNameStr + "' " +
                    "created successfully on node '${" + KEY_NODE_NAME + "}'");
                rscSuccess.putObjRef(KEY_NODE, nodeNameStr);
                rscSuccess.putObjRef(KEY_RSC_DFN, rscNameStr);
                rscSuccess.putVariable(KEY_NODE_NAME, nodeNameStr);
                rscSuccess.putVariable(KEY_RSC_NAME, rscNameStr);

                successApiCallRc.addEntry(rscSuccess);

                for (VlmApi vlmApi : vlmApiList)
                {
                    currentVlmApi = vlmApi;

                    volNr = null;
                    vlmDfn = null;
                    volNr = new VolumeNumber(vlmApi.getVlmNr());
                    vlmDfn = rscDfn.getVolumeDfn(accCtx, volNr); // accDeniedExc4

                    VlmFlags[] vlmFlags = null;

                    VolumeData.getInstance( // accDeniedExc5, dataAlreadyExistsExc2
                        accCtx,
                        rsc,
                        vlmDfn,
                        vlmApi.getBlockDevice(),
                        vlmApi.getMetaDisk(),
                        vlmFlags,
                        transMgr,
                        true,
                        true
                    );
                    ApiCallRcEntry vlmSuccess = new ApiCallRcEntry();
                    vlmSuccess.setMessageFormat("Volume with number '${" + KEY_VLM_NR + "}' " +
                        "created successfully on node '${" + KEY_NODE_NAME + "}' " +
                        "for resource '${" + KEY_RSC_NAME + "}'.");
                    vlmSuccess.putVariable(KEY_NODE_NAME, nodeNameStr);
                    vlmSuccess.putVariable(KEY_RSC_NAME, rscNameStr);
                    vlmSuccess.putVariable(KEY_VLM_NR, Integer.toString(vlmApi.getVlmNr()));
                    vlmSuccess.putObjRef(KEY_NODE, nodeNameStr);
                    vlmSuccess.putObjRef(KEY_RSC_DFN, rscNameStr);
                    vlmSuccess.putObjRef(KEY_VLM_NR, Integer.toString(vlmApi.getVlmNr()));

                    successApiCallRc.addEntry(vlmSuccess);
                }

                transMgr.commit();

                // if everything worked fine, just replace the returned rcApiCall with the
                // already filled successApiCallRc. otherwise, this line does not get executed anyways
                apiCallRc = successApiCallRc;
                controller.getErrorReporter().logInfo(
                    "Resource '%s' on node '%s' saved to database",
                    rscName,
                    nodeName
                );

                // TODO: tell satellite(s) to do their job
                // TODO: if a satellite confirms creation, also log it to controller.info
            }
        }
        catch (SQLException sqlExc)
        {
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                "A database error occured while trying to create a new resource. " +
                    "(Node name: " + nodeNameStr + ", resource name: " + rscNameStr +")"
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_CRT_FAIL_SQL);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setCauseFormat(invalidNameExc.getMessage());
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            if (nodeName == null)
            {
                controller.getErrorReporter().reportError(
                    invalidNameExc,
                    accCtx,
                    client,
                    "Given node name '" + nodeNameStr + "' is invalid"
                );
                entry.setMessageFormat("Given node name '${" + KEY_NODE_NAME + "}' is invalid");
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_INVALID_NODE_NAME);
            }
            else
            {
                controller.getErrorReporter().reportError(
                    invalidNameExc,
                    accCtx,
                    client,
                    "Given resource name '" + rscNameStr + "' is invalid"
                );
                entry.setMessageFormat("Given node name '${" + KEY_RSC_NAME + "}' is invalid");
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_INVALID_RSC_NAME);
            }

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String action = "Given user has no permission to ";
            if (node == null)
            { // accDeniedExc1
                action += "access the node '" + nodeNameStr + "'";
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_NODE);
            }
            else
            if (rscDfn == null)
            { // accDeniedExc2
                action += "access the resource definition '" + rscNameStr + "'";
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_RSC_DFN);
            }
            else
            if (rsc == null)
            { // accDeniedExc3
                action += "access the resource '"+ rscNameStr + "' on node '" + nodeNameStr + "'";
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_RSC);
            }
            else
            if (vlmDfn == null)
            { // accDeniedExc4
                action += "access the volume definition with volume number " + currentVlmApi.getVlmNr() + " on resource '" +
                    rscNameStr + "' on node '" + nodeNameStr + "'";
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_VLM_DFN);
            }
            else
            { // accDeniedExc5
                action += "create a new volume with volume number " + currentVlmApi.getVlmNr() + " on resource '"+
                    rscNameStr + "' on node '" + nodeNameStr + "'";
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_VLM);
            }
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                action
            );
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.setMessageFormat(action);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException dataAlreadyExistsExc)
        {
            String errorMsgFormat;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            // dataAlreadyExistsExc0 cannot happen
            if (rsc == null)
            { // dataAlreadyExistsExc1
                errorMsgFormat = "Resource '" + rscNameStr + "' could not be created as it already exists on " +
                    "node '" + nodeNameStr + "'.";
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_RSC_EXISTS);
            }
            else
            { // dataAlreadyExistsExc2
                errorMsgFormat = "Volume with volume number " + currentVlmApi.getVlmNr() + " could not be created " +
                    "as it already exists on resource '" + rscNameStr + "' on node '" + nodeNameStr + "'.";
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_NODE_EXISTS);
            }

            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.setMessageFormat(errorMsgFormat);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            String errorMsgFormat;
            ApiCallRcEntry entry = new ApiCallRcEntry();

            if (nodeId == null)
            {
                errorMsgFormat = "Node id's value (" + nodeIdRaw + ") is out of its valid range (" +
                    NodeId.NODE_ID_MIN + " - " + NodeId.NODE_ID_MAX + ")";
                entry.putVariable(KEY_NODE_ID, Integer.toString(nodeIdRaw));
                entry.setReturnCode(RC_RSC_CRT_FAIL_INVALID_NODE_ID);
            }
            else
            {
                errorMsgFormat = "Volume number (" + currentVlmApi.getVlmNr() + ") is out of its valid range (" +
                    VolumeNumber.VOLUME_NR_MIN + " - " + VolumeNumber.VOLUME_NR_MAX + ")";
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.setReturnCode(RC_RSC_CRT_FAIL_INVALID_VLM_NR);
            }
            entry.setCauseFormat(valueOutOfRangeExc.getMessage());
            entry.setMessageFormat(errorMsgFormat);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
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
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        "A database error occured while trying to rollback a resource creation. " +
                            "(Node name: " + nodeNameStr + ", resource name: " + rscNameStr +")"
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_CRT_FAIL_SQL_ROLLBACK);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_NODE, nodeNameStr);
                    entry.putObjRef(KEY_RSC_DFN, rscNameStr);

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

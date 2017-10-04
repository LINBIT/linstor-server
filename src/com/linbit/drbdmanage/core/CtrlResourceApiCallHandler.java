package com.linbit.drbdmanage.core;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiCallRcConstants;
import com.linbit.drbdmanage.ApiCallRcImpl;
import com.linbit.drbdmanage.DrbdDataAlreadyExistsException;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnApiData;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.api.ApiConsts;
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
        List<VlmDfnApiData> volDescrMap
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
        VolumeDefinition.VlmDfnApiData currentVolCrtData = null;
        VolumeDefinition lastVolDfn = null;

        short peerCount = getAsShort(props, ApiConsts.KEY_PEER_COUNT, controller.getDefaultPeerCount());
        int alStripes = getAsInt(props, ApiConsts.KEY_AL_STRIPES, controller.getDefaultAlStripes());
        long alStripeSize = getAsLong(props, ApiConsts.KEY_AL_SIZE, controller.getDefaultAlSize());

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection()); // sqlExc1

            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE); // accDeniedExc1
            rscDfn = ResourceDefinitionData.getInstance( // sqlExc2, accDeniedExc1 (same as last line), alreadyExistsExc1
                accCtx,
                new ResourceName(resourceName), // invalidNameExc1
                null, // init flags
                transMgr,
                true,
                true
            );

            for (VolumeDefinition.VlmDfnApiData volCrtData : volDescrMap)
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

                lastVolDfn = VolumeDefinitionData.getInstance( // mdExc2, sqlExc3, accDeniedExc2, alreadyExistsExc2
                    accCtx,
                    rscDfn,
                    volNr,
                    minorNr,
                    size,
                    null, // init flags
                    transMgr,
                    true,
                    true
                );
            }

            transMgr.commit(); // sqlExc4

            controller.rscDfnMap.put(rscDfn.getName(), rscDfn);

            for (VolumeDefinition.VlmDfnApiData volCrtData : volDescrMap)
            {
                ApiCallRcEntry volSuccessEntry = new ApiCallRcEntry();
                volSuccessEntry.setReturnCode(ApiCallRcConstants.RC_VOLUME_DEFINITION_CREATED);
                volSuccessEntry.setMessageFormat(
                    String.format(
                        "Volume Definition with number ${%s} and minor number ${%s} successfully created",
                        ApiConsts.KEY_VLM_NR,
                        ApiConsts.KEY_MINOR_NR
                    )
                );
                volSuccessEntry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(volCrtData.getVolumeNr()));
                volSuccessEntry.putVariable(ApiConsts.KEY_MINOR_NR, Integer.toString(volCrtData.getMinorNr()));

                apiCallRc.addEntry(volSuccessEntry);
            }

            ApiCallRcEntry successEntry = new ApiCallRcEntry();

            successEntry.setReturnCode(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATED);
            successEntry.setMessageFormat("Resource definition '${" + ApiConsts.KEY_RSC_NAME + "}' successfully created.");
            successEntry.putVariable(ApiConsts.KEY_RSC_NAME, resourceName);
            successEntry.putVariable(ApiConsts.KEY_PEER_COUNT, Short.toString(peerCount));
            successEntry.putVariable(ApiConsts.KEY_AL_STRIPES, Integer.toString(alStripes));
            successEntry.putVariable(ApiConsts.KEY_AL_SIZE, Long.toString(alStripeSize));

            apiCallRc.addEntry(successEntry);

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
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
            else
            if (rscDfn == null)
            { // handle sqlExc2
                String errorMessage = "A database error occured while trying to create a new resource definition.";
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
            else
            if (lastVolDfn == null)
            { // handle sqlExc3
                String errorMessage = "A database error occured while trying to create a new volume definition.";
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());
                if (currentVolCrtData != null)
                {
                    entry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                    entry.putVariable(ApiConsts.KEY_MINOR_NR, Integer.toString(currentVolCrtData.getMinorNr()));
                }
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
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
        }
        catch (AccessDeniedException accExc)
        {
            if (rscDfn == null)
            { // handle accDeniedExc1

                String errorMessage = "The given access context has no permission to create a resource definition";
                controller.getErrorReporter().reportError(
                    accExc,
                    accCtx,
                    client,
                    errorMessage
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(accExc.getMessage());
                entry.setDetailsFormat("The access-context (user: ${acUser}, role: ${acRole}) "
                    + "requires more rights to create a new resource definition ");
                entry.putVariable(ApiConsts.KEY_ID, accCtx.subjectId.name.displayValue);
                entry.putVariable(ApiConsts.KEY_ROLE, accCtx.subjectRole.name.displayValue);

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
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(accExc.getMessage());
                entry.setDetailsFormat("The access-context (user: ${acUser}, role: ${acRole}) "
                    + "requires more rights to create a new resource definition ");
                entry.putVariable(ApiConsts.KEY_ID, accCtx.subjectId.name.displayValue);
                entry.putVariable(ApiConsts.KEY_ROLE, accCtx.subjectRole.name.displayValue);

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
            entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat("The given resource name '${" + ApiConsts.KEY_RSC_NAME + "}'is invalid");
            entry.putVariable(ApiConsts.KEY_RSC_NAME, resourceName);

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
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat("Given volume number ${" + ApiConsts.KEY_VLM_NR + "} was invalid");
                entry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));

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
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat("Given minor number ${" + ApiConsts.KEY_MINOR_NR + "} was invalid");
                entry.putVariable(ApiConsts.KEY_MINOR_NR, Integer.toString(currentVolCrtData.getVolumeNr()));

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
            entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat("Given volume size ${" + ApiConsts.KEY_VLM_SIZE + "} was invalid");
            entry.putVariable(ApiConsts.KEY_VLM_SIZE, Long.toString(currentVolCrtData.getSize()));

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
                    "The ResourceDefinition which should be created already exists"
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat("The ResourceDefinition already exists");
                entry.setCauseFormat(alreadyExistsExc.getMessage());

                apiCallRc.addEntry(entry);
            }
            else
            {
                // handle alreadyExists2
                controller.getErrorReporter().reportError(
                    alreadyExistsExc,
                    accCtx,
                    client,
                    "A VolumeDefinition which should be created already exists"
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat("A VolumeDefinition already exists");
                entry.setCauseFormat(alreadyExistsExc.getMessage());
                if (currentVolCrtData != null)
                {
                    entry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(currentVolCrtData.getVolumeNr()));
                    entry.putVariable(ApiConsts.KEY_MINOR_NR, Integer.toString(currentVolCrtData.getMinorNr()));
                }

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
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATION_FAILED);
                entry.setMessageFormat(errorMessage);
                entry.setCauseFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
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

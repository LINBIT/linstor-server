package com.linbit.drbdmanage.core;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.event.Level;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiCallRcConstants;
import com.linbit.drbdmanage.ApiCallRcImpl;
import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.dbcp.DbConnectionPool;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;

class CtrlApiCallHandler
{
    private static final String RESOURCE_DEFINITION_PEER_COUNT_KEY = "rscDfnPeerCountKey";
    private static final String RESOURCE_DEFINITION_AL_SIZE_KEY = "rscDfnAlSizeKey";
    private static final String RESOURCE_DEFINITION_AL_STRIPES_KEY = "rscDfnAlStripesKey";

    private final Controller controller;
    private final DbConnectionPool dbConnPool;
    private final ObjectProtection rscDfnMapProt;
    private final Map<ResourceName, ResourceDefinition> rscDfnMap;

    CtrlApiCallHandler(
        Controller controllerRef,
        DbConnectionPool dbConnPoolRef,
        ObjectProtection rscDfnMapProtRef,
        Map<ResourceName, ResourceDefinition> rscDfnMapRef
    )
    {
        controller = controllerRef;
        dbConnPool = dbConnPoolRef;
        rscDfnMapProt = rscDfnMapProtRef;
        rscDfnMap = rscDfnMapRef;
    }


    public ApiCallRc createNode(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        Map<String, String> props
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

        TransactionMgr transMgr = null;
        Node node = null;
        try
        {
            transMgr = new TransactionMgr(dbConnPool.getConnection()); // sqlExc1
            NodeName nodeName = new NodeName(nodeNameStr); // invalidNameExc1

            NodeType type = null;
            // TODO: parse types from props
            NodeFlag[] flags = null;
            // TODO: parse flags from props
            node = NodeData.getInstance( // sqlExc2, accDeniedExc1
                accCtx,
                nodeName,
                type,
                flags,
                controller.getRootSerialGenerator(),
                transMgr,
                true
            );
        }
        catch (SQLException sqlExc)
        {
            if (transMgr == null)
            { // handle sqlExc1
                controller.getErrorReporter().reportError(
                    sqlExc,
                    null,
                    null,
                    "A database error occured while trying to create a new transaction."
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRc.MASK_ERROR);
                // TODO: set additional bits for further describing the problem
                entry.setMessageFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
            else
            if (node == null)
            { // handle sqlExc2
                // TODO implement error reporting
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                "The given name for the node is invalid"
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiCallRc.MASK_ERROR);
            // TODO: set additional bits for further describing the problem
            entry.setMessageFormat(invalidNameExc.getMessage());

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // handle accDeniedExc1

            // TODO implement error reporting
        }
        return apiCallRc;
    }

    public ApiCallRc createResourceDefinition(
        AccessContext accCtx,
        Peer client,
        String resourceName,
        Map<String, String> props,
        List<VolumeDefinition.CreationData> volDescrMap
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
        VolumeDefinition.CreationData currentVolCrtData = null;
        VolumeDefinition lastVolDfn = null;

        short peerCount = getAsShort(props, RESOURCE_DEFINITION_PEER_COUNT_KEY, controller.getDefaultPeerCount());
        int alStripes = getAsInt(props, RESOURCE_DEFINITION_AL_STRIPES_KEY, controller.getDefaultAlStripes());
        long alStripeSize = getAsLong(props, RESOURCE_DEFINITION_AL_SIZE_KEY, controller.getDefaultAlSize());

        try
        {
            transMgr = new TransactionMgr(dbConnPool.getConnection()); // sqlExc1

            rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE); // accDeniedExc1
            rscDfn = ResourceDefinitionData.getInstance( // sqlExc2, accDeniedExc1 (same as last line)
                accCtx,
                new ResourceName(resourceName), // invalidNameExc1
                null, // init flags
                controller.getRootSerialGenerator(),
                transMgr,
                true
            );
            // TODO: Read optional ConnectionDefinitions from the properties map
            // TODO: Read optional TcpPortNumbers for ConnectionDefinitions
            //       from the properties map, or allocate a free TcpPortNumber

            for (VolumeDefinition.CreationData volCrtData : volDescrMap)
            {
                currentVolCrtData = volCrtData;

                lastVolDfn = null;
                volNr = null;
                minorNr = null;

                volNr = new VolumeNumber(volCrtData.getId()); // valOORangeExc1
                minorNr = new MinorNumber(volCrtData.getMinorNr()); // valOORangeExc2

                long size = volCrtData.getSize();

                // getGrossSize performs check and throws exception when something is invalid
                controller.getMetaDataApi().getGrossSize(size, peerCount, alStripes, alStripeSize);
                // mdExc2

                lastVolDfn = VolumeDefinitionData.getInstance( // mdExc1, sqlExc3, accDeniedExc2
                    accCtx,
                    rscDfn,
                    volNr,
                    minorNr,
                    size,
                    null, // init flags
                    controller.getRootSerialGenerator(),
                    transMgr,
                    true
                );
            }

            transMgr.commit(); // sqlExc4

            rscDfnMap.put(rscDfn.getName(), rscDfn);

            for (VolumeDefinition.CreationData volCrtData : volDescrMap)
            {
                ApiCallRcEntry volSuccessEntry = new ApiCallRcEntry();
                volSuccessEntry.setReturnCode(ApiCallRcConstants.RC_VOLUME_DEFINITION_CREATED);
                volSuccessEntry.setMessageFormat("Volume Definition with number ${volNr} and minor number ${minorNr} successfully created");
                volSuccessEntry.putVariable("volNr", Integer.toString(volCrtData.getId()));
                volSuccessEntry.putVariable("minorNr", Integer.toString(volCrtData.getMinorNr()));

                apiCallRc.addEntry(volSuccessEntry);
            }

            ApiCallRcEntry successEntry = new ApiCallRcEntry();

            successEntry.setReturnCode(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATED);
            successEntry.setMessageFormat("Resource definition '${resName}' successfully created");
            successEntry.putVariable("resName", resourceName);

            apiCallRc.addEntry(successEntry);

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
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_FAILED);
                entry.setMessageFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
            else
            if (rscDfn == null)
            { // handle sqlExc2
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    "A database error occured while trying to create a new resource definition."
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_RESOURCE_DEFINITION_FAILED);
                entry.setMessageFormat(sqlExc.getMessage());
                entry.setCauseFormat("Persisting the resource definition resulted in an SQL Exception");

                apiCallRc.addEntry(entry);
            }
            else
            if (lastVolDfn == null)
            { // handle sqlExc3
                // TODO implement error reporting
            }
            else
            if (transMgr.isDirty())
            { // handle sqlExc4
                // TODO implement error reporting
            }
        }
        catch (AccessDeniedException accExc)
        {
            if (rscDfn == null)
            { // handle accDeniedExc1

                // TODO: Generate a problem report with less debug information
                controller.getErrorReporter().reportProblem(
                    Level.ERROR, accExc, accCtx, client,
                    "createResourceDefinition" // TOOD: Provide useful context information
                );
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRc.MASK_ERROR);
                // TODO: set additional bits for further describing the problem
                entry.setCauseFormat("The given access-context has insucuffient rights");
                entry.setDetailsFormat("The access-context (user: ${acUser}, role: ${acRole}) "
                    + "requires more rights to create a new resource definition ");
                entry.putVariable("acUser", accCtx.subjectId.name.displayValue);
                entry.putVariable("acRole", accCtx.subjectRole.name.displayValue);

                apiCallRc.addEntry(entry);
            }
            else
            if (lastVolDfn == null)
            { // handle accDeniedExc2
                // TODO implement error reporting
            }
        }
        catch (InvalidNameException nameExc)
        {
            // handle invalidNameExc1

            // TODO: Generate a problem report with less debug information
            String nameExcMsg = nameExc.getMessage();
            String causeText = "The specified name is not valid for use as a resource name.";
            if (nameExcMsg != null)
            {
                causeText += "\n" + nameExcMsg;
            }
            DrbdManageException resNameExc = new DrbdManageException(
                nameExc.getMessage(),
                // Description
                "Creation of the resource definition failed.",
                // Cause
                causeText,
                // Correction
                "Retry creating the resource definition with a name that is valid for use as a resource name",
                // Error details
                null,
                // Nested exception
                nameExc
            );
            controller.getErrorReporter().reportProblem(
                Level.ERROR, resNameExc, accCtx, client,
                "createResourceDefinition" // TODO: Provide useful context information
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiCallRc.MASK_ERROR);
            // TODO: set additional bits for further describing the problem
            entry.setMessageFormat(nameExcMsg);
            entry.setCauseFormat("The given resource name is invalid");
            entry.setDetailsFormat("The access-context (user: ${acUser}, role: ${acRole}) "
                + "requires more rights to create a new resource definition ");

            apiCallRc.addEntry(entry);
        }
        catch (ValueOutOfRangeException valOORangeExc)
        {
            if (volNr == null)
            { // handle valOORangeExc1

                // TODO: Generate a problem report with less debug information
                controller.getErrorReporter().reportError(valOORangeExc);

                ApiCallRcEntry invalidVolNum = new ApiCallRcEntry();
                invalidVolNum.setReturnCodeBit(ApiCallRc.MASK_ERROR);
                // TODO: Additionally set further bitflags as return code describing the problem

                invalidVolNum.setCorrectionFormat("Specify a valid volume number");
                invalidVolNum.setDetailsFormat("Given volume number ${volNr} was invalid");
                invalidVolNum.putVariable("volNr", Integer.toString(currentVolCrtData.getId()));
                apiCallRc.addEntry(invalidVolNum);
            }
            else
            if (minorNr == null)
            { // handle valOORangeExc2

                // TODO: Generate a problem report with less debug information
                controller.getErrorReporter().reportError(valOORangeExc);

                ApiCallRcEntry invalidMinorNum = new ApiCallRcEntry();
                invalidMinorNum.setReturnCodeBit(ApiCallRc.MASK_ERROR);
                // TODO: Additionally set further bitflags as return code describing the problem

                invalidMinorNum.setCorrectionFormat("Specify a valid minor number");
                invalidMinorNum.setDetailsFormat("Given minor number ${minorNr} was invalid");
                invalidMinorNum.putVariable("volNr", Integer.toString(currentVolCrtData.getMinorNr()));
                apiCallRc.addEntry(invalidMinorNum);
            }
        }
        catch (MdException metaDataExc)
        {
            // handle mdExc1

            // TODO: Generate a problem report with less debug information
            controller.getErrorReporter().reportError(metaDataExc);

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiCallRc.MASK_ERROR);
            // TODO: set additional bits for further describing the problem
            entry.setMessageFormat(metaDataExc.getMessage());

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
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    "A database error occured while trying to rollback the transaction."
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRc.MASK_ERROR);
                // TODO: set additional bits for further describing the problem
                entry.setMessageFormat(sqlExc.getMessage());

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

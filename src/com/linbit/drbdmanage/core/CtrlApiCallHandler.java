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
    public static final String PROPS_NODE_TYPE_KEY = "nodeType";
    public static final String PROPS_NODE_FLAGS_KEY = "nodeFlags";

    public static final String PROPS_RESOURCE_DEFINITION_PEER_COUNT_KEY = "rscDfnPeerCountKey";
    public static final String PROPS_RESOURCE_DEFINITION_AL_SIZE_KEY = "rscDfnAlSizeKey";
    public static final String PROPS_RESOURCE_DEFINITION_AL_STRIPES_KEY = "rscDfnAlStripesKey";

    public static final String API_RC_VAR_NODE_NAME_KEY = "nodeName";
    public static final String API_RC_VAR_RESOURCE_NAME_KEY = "resName";
    public static final String API_RC_VAR_VOlUME_NUMBER_KEY = "volNr";
    public static final String API_RC_VAR_VOlUME_MINOR_KEY = "volMinor";
    public static final String API_RC_VAR_VOlUME_SIZE_KEY = "volSize";
    public static final String API_RC_VAR_RESOURCE_PEER_COUNT_KEY = "peerCount";
    public static final String API_RC_VAR_RESOURCE_AL_STRIPES_KEY = "alStripes";
    public static final String API_RC_VAR_RESOURCE_AL_SIZE_KEY = "alSize";
    public static final String API_RC_VAR_ACC_CTX_ID_KEY = "accCtxId";
    public static final String API_RC_VAR_ACC_CTX_ROLE_KEY = "accCtxRole";

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

            NodeType type = NodeType.valueOfIgnoreCase(props.get(PROPS_NODE_TYPE_KEY));
            NodeFlag[] flags = NodeFlag.valuesOfIgnoreCase(props.get(PROPS_NODE_FLAGS_KEY));
            node = NodeData.getInstance( // sqlExc2, accDeniedExc1
                accCtx,
                nodeName,
                type,
                flags,
                transMgr,
                true
            );

            transMgr.commit(); // sqlExc3

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiCallRcConstants.RC_NODE_CREATED);
            entry.setMessageFormat("Node ${" + API_RC_VAR_NODE_NAME_KEY + "} successfully created");
            entry.putVariable(API_RC_VAR_NODE_NAME_KEY, nodeNameStr);

            apiCallRc.addEntry(entry);
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
                entry.setReturnCodeBit(ApiCallRcConstants.RC_NODE_CREATION_FAILED);
                entry.setMessageFormat("Failed to create database transaction");
                entry.setCauseFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
            else
            if (node == null)
            { // handle sqlExc2
                controller.getErrorReporter().reportError(
                    sqlExc,
                    null,
                    null,
                    "A database error occured while trying to persist the node."
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_NODE_CREATION_FAILED);
                entry.setMessageFormat("Failed to persist node.");
                entry.setCauseFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
            else
            {
                // handle sqlExc3

                controller.getErrorReporter().reportError(
                    sqlExc,
                    null,
                    null,
                    "A database error occured while trying to commit the transaction."
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_NODE_CREATION_FAILED);
                entry.setMessageFormat("Failed to commit transaction");
                entry.setCauseFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            // handle invalidNameExc1

            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                "The given name for the node is invalid"
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiCallRcConstants.RC_NODE_CREATION_FAILED);
            entry.setMessageFormat("The given name '${" + API_RC_VAR_NODE_NAME_KEY + "}' is invalid");
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putVariable(API_RC_VAR_NODE_NAME_KEY, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // handle accDeniedExc1

            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                "The given access context has no permission to create a new node"
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(ApiCallRcConstants.RC_NODE_CREATION_FAILED);
            entry.setMessageFormat("The given access context has no permission to create a new node");
            entry.setCauseFormat(accDeniedExc.getMessage());

            apiCallRc.addEntry(entry);
        }

        if (transMgr != null && transMgr.isDirty())
        {
            try
            {
                transMgr.rollback();
            }
            catch (SQLException sqlExc)
            {
                controller.getErrorReporter().reportError(
                    sqlExc,
                    null,
                    null,
                    "A database error occured while trying to rollback the transaction."
                );

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiCallRcConstants.RC_NODE_CREATION_FAILED);
                entry.setMessageFormat("Failed to rollback database transaction");
                entry.setCauseFormat(sqlExc.getMessage());

                apiCallRc.addEntry(entry);
            }
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

        short peerCount = getAsShort(props, PROPS_RESOURCE_DEFINITION_PEER_COUNT_KEY, controller.getDefaultPeerCount());
        int alStripes = getAsInt(props, PROPS_RESOURCE_DEFINITION_AL_STRIPES_KEY, controller.getDefaultAlStripes());
        long alStripeSize = getAsLong(props, PROPS_RESOURCE_DEFINITION_AL_SIZE_KEY, controller.getDefaultAlSize());

        try
        {
            transMgr = new TransactionMgr(dbConnPool.getConnection()); // sqlExc1

            rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE); // accDeniedExc1
            rscDfn = ResourceDefinitionData.getInstance( // sqlExc2, accDeniedExc1 (same as last line)
                accCtx,
                new ResourceName(resourceName), // invalidNameExc1
                null, // init flags
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
                // mdExc1

                lastVolDfn = VolumeDefinitionData.getInstance( // mdExc2, sqlExc3, accDeniedExc2
                    accCtx,
                    rscDfn,
                    volNr,
                    minorNr,
                    size,
                    null, // init flags
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
                volSuccessEntry.setMessageFormat(
                    String.format(
                        "Volume Definition with number ${%s} and minor number ${%s} successfully created",
                        API_RC_VAR_VOlUME_NUMBER_KEY,
                        API_RC_VAR_VOlUME_MINOR_KEY
                    )
                );
                volSuccessEntry.putVariable(API_RC_VAR_VOlUME_NUMBER_KEY, Integer.toString(volCrtData.getId()));
                volSuccessEntry.putVariable(API_RC_VAR_VOlUME_MINOR_KEY, Integer.toString(volCrtData.getMinorNr()));

                apiCallRc.addEntry(volSuccessEntry);
            }

            ApiCallRcEntry successEntry = new ApiCallRcEntry();

            successEntry.setReturnCode(ApiCallRcConstants.RC_RESOURCE_DEFINITION_CREATED);
            successEntry.setMessageFormat("Resource definition '${" + API_RC_VAR_RESOURCE_NAME_KEY + "}' successfully created.");
            successEntry.putVariable(API_RC_VAR_RESOURCE_NAME_KEY, resourceName);
            successEntry.putVariable(API_RC_VAR_RESOURCE_PEER_COUNT_KEY, Short.toString(peerCount));
            successEntry.putVariable(API_RC_VAR_RESOURCE_AL_STRIPES_KEY, Integer.toString(alStripes));
            successEntry.putVariable(API_RC_VAR_RESOURCE_AL_SIZE_KEY, Long.toString(alStripeSize));

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
                    entry.putVariable(API_RC_VAR_VOlUME_NUMBER_KEY, Integer.toString(currentVolCrtData.getId()));
                    entry.putVariable(API_RC_VAR_VOlUME_MINOR_KEY, Integer.toString(currentVolCrtData.getMinorNr()));
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
                entry.putVariable(API_RC_VAR_ACC_CTX_ID_KEY, accCtx.subjectId.name.displayValue);
                entry.putVariable(API_RC_VAR_ACC_CTX_ROLE_KEY, accCtx.subjectRole.name.displayValue);

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
                entry.putVariable(API_RC_VAR_ACC_CTX_ID_KEY, accCtx.subjectId.name.displayValue);
                entry.putVariable(API_RC_VAR_ACC_CTX_ROLE_KEY, accCtx.subjectRole.name.displayValue);

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
            entry.setCauseFormat("The given resource name '${" + API_RC_VAR_RESOURCE_NAME_KEY + "}'is invalid");
            entry.putVariable(API_RC_VAR_RESOURCE_NAME_KEY, resourceName);

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
                entry.setCauseFormat("Given volume number ${" + API_RC_VAR_VOlUME_NUMBER_KEY + "} was invalid");
                entry.putVariable(API_RC_VAR_VOlUME_NUMBER_KEY, Integer.toString(currentVolCrtData.getId()));

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
                entry.setCauseFormat("Given minor number ${" + API_RC_VAR_VOlUME_MINOR_KEY + "} was invalid");
                entry.putVariable(API_RC_VAR_VOlUME_MINOR_KEY, Integer.toString(currentVolCrtData.getId()));

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
            entry.setCauseFormat("Given volume size ${" + API_RC_VAR_VOlUME_SIZE_KEY + "} was invalid");
            entry.putVariable(API_RC_VAR_VOlUME_SIZE_KEY, Long.toString(currentVolCrtData.getSize()));

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

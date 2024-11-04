package com.linbit.linstor.debug;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.repository.KeyValueStoreRepository;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.PrivilegeSet;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.security.Role;
import com.linbit.linstor.security.RoleName;
import com.linbit.linstor.security.SecTypeName;
import com.linbit.linstor.security.SecurityType;
import com.linbit.linstor.security.ShutdownProtHolder;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.slf4j.event.Level;

/**
 * Changes object protection settings
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdChangeObjProt extends BaseDebugCmd
{
    private static final String CMD_NAME = "ChgObjProt";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_OBJ_CLASS   = "CLASS";
    private static final String PRM_OBJ_NAME    = "NAME";
    private static final String PRM_OBJ_OWNER   = "OWNER";
    private static final String PRM_OBJ_SECTYPE = "SECTYPE";
    private static final String PRM_OBJ_ROLE    = "ROLE";
    private static final String PRM_OBJ_ACCESS  = "ACCESS";
    private static final String PRM_MND_AC      = "MAC";
    private static final String PRM_DSC_AC      = "DAC";

    private static final String OPT_ENFORCE     = "ENFORCE";
    private static final String OPT_OVRD        = "OVRD";

    private static final String PRM_ACCESS_NONE = "NONE";

    private static final String CLS_NODE        = "NODE";
    private static final String CLS_RSCDFN      = "RSCDFN";
    private static final String CLS_RSC         = "RSC";
    private static final String CLS_STORPOOLDFN = "STORPOOLDFN";
    private static final String CLS_KEYVALSTOR  = "KEYVALSTOR";
    private static final String CLS_SYSOBJ      = "SYSOBJ";

    private static final String SO_NODE_DIR         = "NODEDIR";
    private static final String SO_RSCDFN_DIR       = "RSCDFNDIR";
    private static final String SO_STORPOOLDFN_DIR  = "STORPOOLDFNDIR";
    private static final String SO_CFGVAL           = "CFGVAL";
    private static final String SO_SHUTDOWN         = "SHUTDOWN";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_CLASS,
            "Class of the protected object.\n" +
            "Supported classes are:\n" +
            "    " + CLS_NODE + "\n" +
            "    " + CLS_RSCDFN + "\n" +
            "    " + CLS_RSC + "\n" +
            "    " + CLS_STORPOOLDFN + "\n" +
            "    " + CLS_KEYVALSTOR + "\n" +
            "    " + CLS_SYSOBJ
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_NAME,
            "Name of the protected object\n\n" +
            "For nodes (class " + CLS_NODE + ") and resource definitions (class " + CLS_RSCDFN + ")\n" +
            "objects, this is the name of the node or resource definition, respectively.\n\n" +
            "For resources (class " + CLS_RSC + ") the name must be specified as a path,\n" +
            "with path components separated by a forward slash (/):\n" +
            "    NodeName/ResourceName\n\n" +
            "For system objects (class " + CLS_SYSOBJ + "), name is one of:\n" +
            "    " + SO_NODE_DIR + "\n" +
            "        Controls the ability to view, create or delete nodes\n" +
            "    " + SO_RSCDFN_DIR + "\n" +
            "        Controls the ability to view, create or delete resource definitions\n" +
            "    " + SO_STORPOOLDFN_DIR + "\n" +
            "        Controls the ability to view, create or delete storage pool definitions\n" +
            "    " + SO_CFGVAL + "\n" +
            "        Controls the ability to view or change the configuration\n" +
            "    " + SO_SHUTDOWN + "\n" +
            "        LINSTOR shutdown authorization"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_OWNER,
            "Changes the owner of the protected object to the specified security role"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_SECTYPE,
            "Changes the security type of the protected object to the specified security type"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_ROLE,
            "Creates, changes or deletes the access control entry for the specified security role.\n" +
            "The access permission to grant to the specified security role must be specified with the\n" +
            PRM_OBJ_ACCESS + " parameter. If an access permission of " + PRM_ACCESS_NONE + " is\n" +
            "specified, the access control entry for the specified security role is removed."
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_ACCESS,
            "The access permission to grant to the security role specified with the\n" +
            PRM_OBJ_ROLE + "parameter.\n" +
            "Valid values are:\n" +
            "    " + PRM_ACCESS_NONE + "\n" +
            "        Deletes the access control entry for the specified role\n" +
            "    " + AccessType.VIEW.name() + "\n" +
            "        Allows the specified security role to view information about the protected object\n" +
            "    " + AccessType.USE.name() + "\n" +
            "        Allows the specified security role to use the protected object for certain operations,\n" +
            "        such as creating new resources\n" +
            "    " + AccessType.CHANGE.name() + "\n" +
            "        Allows the specified security role to change the protected object\n" +
            "    " + AccessType.CONTROL.name() + "\n" +
            "        Allows the specified security role to control the access control list of the\n" +
            "        protected object, or to delete the protected object"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_DSC_AC,
            "Discretionary access control enforcement mode\n" +
            "    " + OPT_ENFORCE + "\n" +
            "        Enforce discretionary access control\n" +
            "    " + OPT_OVRD + "\n" +
            "        Override discretionary access control\n" +
            "        (Attempts to enable the PRIV_OBJ_OWNER and PRIV_OBJ_CONTROL privileges)\n" +
            "    If unspecified, the default value is " + OPT_ENFORCE

        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_MND_AC,
            "Mandatory access control enforcement mode\n" +
            "    " + OPT_ENFORCE + "\n" +
            "        Enforce mandatory access control\n" +
            "    " + OPT_OVRD + "\n" +
            "        Override mandatory access control\n" +
            "        (Attempts to enable the PRIV_OVRD_MAC privilege)\n" +
            "    If unspecified, the default value is " + OPT_ENFORCE
        );
    }

    private final Provider<TransactionMgr> trnActProvider;
    private final ErrorReporter errLog;

    private ReadWriteLock rcfgLock;
    private ReadWriteLock nodeRpsLock;
    private ReadWriteLock storPoolDfnRpsLock;
    private ReadWriteLock rscDfnRpsLock;
    private ReadWriteLock kvStoreRpsLock;
    private ReadWriteLock sysCfgLock;

    private NodeRepository                  nodeRps;
    private ResourceDefinitionRepository    rscDfnRps;
    private StorPoolDefinitionRepository    storPoolDfnRps;
    private KeyValueStoreRepository         keyValStoreRps;
    private SystemConfRepository            sysCfgRps;
    private ShutdownProtHolder              shutdownObj;

    @Inject
    public CmdChangeObjProt(
        Provider<TransactionMgr> trnActProviderRef,
        ErrorReporter errorReporterRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock rcfgLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnRpsLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodeRpsLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnRpsLockRef,
        @Named(CoreModule.CTRL_CONF_LOCK) ReadWriteLock sysCfgLockRef,
        @Named(CoreModule.KVS_MAP_LOCK) ReadWriteLock kvStoreRpsLockRef,
        NodeRepository nodeRpsRef,
        ResourceDefinitionRepository rscDfnRpsRef,
        StorPoolDefinitionRepository storPoolDfnRpsRef,
        KeyValueStoreRepository keyValStoreRpsRef,
        SystemConfRepository sysCfgRpsRef,
        ShutdownProtHolder shutdownObjRef
    )
    {
        super(
            new String[]
            {
                CMD_NAME
            },
            "Change object protection",
            "Changes the owner, access control list or security type of protected objects",
            PARAMETER_DESCRIPTIONS,
            null
        );

        trnActProvider      = trnActProviderRef;
        errLog              = errorReporterRef;

        rcfgLock            = rcfgLockRef;
        storPoolDfnRpsLock  = storPoolDfnRpsLockRef;
        nodeRpsLock         = nodeRpsLockRef;
        rscDfnRpsLock       = rscDfnRpsLockRef;
        kvStoreRpsLock      = kvStoreRpsLockRef;
        sysCfgLock          = sysCfgLockRef;

        nodeRps         = nodeRpsRef;
        rscDfnRps       = rscDfnRpsRef;
        storPoolDfnRps  = storPoolDfnRpsRef;
        keyValStoreRps   = keyValStoreRpsRef;
        sysCfgRps       = sysCfgRpsRef;
        shutdownObj     = shutdownObjRef;
    }

    @Override
    public void execute(
        final PrintStream debugOut,
        final PrintStream debugErr,
        final AccessContext accCtx,
        final Map<String, String> parameters
    )
        throws Exception
    {
        List<Lock> lockList = new LinkedList<>();
        LockGuard scopeLock = null;
        Lock rcfgRdLock = rcfgLock.readLock();
        rcfgRdLock.lock();
        try
        {
            final StringBuilder responseMsg = new StringBuilder();
            boolean ovrdDscAccCtrl = parseOverrideOption(PRM_DSC_AC, parameters);
            boolean ovrdMndAccCtrl = parseOverrideOption(PRM_MND_AC, parameters);

            String objClass = parameters.get(PRM_OBJ_CLASS);
            String objName = parameters.get(PRM_OBJ_NAME);

            if (objClass == null || objName == null)
            {
                throw new MissingParamException(PRM_OBJ_NAME, PRM_OBJ_CLASS);
            }

            objClass = objClass.toUpperCase();

            ProtectedObject obj = null;
            switch (objClass)
            {
                case CLS_NODE:
                    lockList.add(nodeRpsLock.readLock());
                    scopeLock = createScopeLock(lockList);
                    Node nodeObj = getNode(accCtx, objName);
                    setObjInfo(responseMsg, nodeObj.getName().displayValue, CLS_NODE);
                    obj = nodeObj;
                    break;
                case CLS_RSCDFN:
                    lockList.add(rscDfnRpsLock.readLock());
                    scopeLock = createScopeLock(lockList);
                    ResourceDefinition rscDfnObj = getResourceDefinition(accCtx, objName);
                    setObjInfo(responseMsg, rscDfnObj.getName().displayValue, CLS_RSCDFN);
                    obj = rscDfnObj;
                    break;
                case CLS_RSC:
                    lockList.add(nodeRpsLock.readLock());
                    lockList.add(rscDfnRpsLock.readLock());
                    scopeLock = createScopeLock(lockList);
                    Resource rscObj = getResource(accCtx, objName);
                    setObjInfo(
                        responseMsg,
                        rscObj.getNode().getName().displayValue + "/" +
                        rscObj.getResourceDefinition().getName().displayValue,
                        CLS_RSC
                    );
                    obj = rscObj;
                    break;
                case CLS_STORPOOLDFN:
                    lockList.add(storPoolDfnRpsLock.readLock());
                    scopeLock = createScopeLock(lockList);
                    StorPoolDefinition storPoolDfnObj = getStorPoolDefinition(accCtx, objName);
                    setObjInfo(
                        responseMsg,
                        storPoolDfnObj.getName().displayValue,
                        CLS_STORPOOLDFN
                    );
                    obj = storPoolDfnObj;
                    break;
                case CLS_KEYVALSTOR:
                    lockList.add(kvStoreRpsLock.readLock());
                    scopeLock = createScopeLock(lockList);
                    KeyValueStore kvStoreObj = getKeyValueStore(accCtx, objName);
                    obj = kvStoreObj;
                    setObjInfo(
                        responseMsg,
                        kvStoreObj.getName().displayValue,
                        CLS_KEYVALSTOR
                    );
                    break;
                case CLS_SYSOBJ:
                    obj = getSystemObject(objName, lockList, responseMsg);
                    scopeLock = createScopeLock(lockList);
                    break;
                default:
                    throw new LinStorException(
                        CMD_NAME + ": Invalid " + PRM_OBJ_CLASS + " value '" + objClass + "'",
                        "Invalid object class identifier",
                        "The value '" + objClass + "' is not a valid value for the parameter " + PRM_OBJ_CLASS,
                        "Valid object class identifiers are:\n" +
                        "    " +
                        com.linbit.utils.StringUtils.join(
                            " ",
                            CLS_NODE, CLS_RSCDFN, CLS_RSC, CLS_STORPOOLDFN, CLS_KEYVALSTOR, CLS_SYSOBJ
                        ),
                        null
                    );
            }

            if (obj != null)
            {
                String newOwnerPrm      = parameters.get(PRM_OBJ_OWNER);
                String newSecTypePrm    = parameters.get(PRM_OBJ_SECTYPE);
                String accessRolePrm    = parameters.get(PRM_OBJ_ROLE);
                String accessTypePrm    = parameters.get(PRM_OBJ_ACCESS);

                if (newOwnerPrm == null && newSecTypePrm == null && accessRolePrm == null && accessTypePrm == null)
                {
                    // No parameters
                    throw new LinStorException(
                        CMD_NAME + ": No parameters were specified",
                        "Object protection was not changed",
                        "The command line did not specify any changes to the object protection",
                        "The following combinations of parameters can be used to change the protection " +
                        "of an object:\n" +
                        "- " + PRM_OBJ_OWNER + " to change the object's owner\n" +
                        "- " + PRM_OBJ_SECTYPE + " to change the object's security type\n" +
                        "- " + PRM_OBJ_ROLE + " and " + PRM_OBJ_ACCESS + " to change the object's " +
                        "access control list\n",
                        null
                    );
                }

                changeObjProt(
                    accCtx, debugErr, parameters,
                    obj, ovrdDscAccCtrl, ovrdMndAccCtrl,
                    responseMsg, newOwnerPrm, newSecTypePrm, accessRolePrm, accessTypePrm
                );

                if (responseMsg.length() >= 1)
                {
                    debugOut.print(responseMsg.toString());
                    debugOut.flush();
                }
            }
            else
            {
                debugPrintHelper.printError(
                    debugErr,
                    "Object '" + objName + "' of class " + objClass + " does not exist",
                    null,
                    "Check whether\n" +
                    "  - the object name has been typed correctly\n" +
                    "  - the correct object class has been selected\n",
                    null
                );
            }
        }
        catch (MissingParamException paramExc)
        {
            String[] checkParams = paramExc.getParamList();
            debugPrintHelper.printMultiMissingParamError(debugErr, parameters, checkParams);
        }
        catch (DatabaseException dbExc)
        {
            String reportNr = errLog.reportError(Level.ERROR, dbExc);
            debugPrintHelper.printError(
                debugErr,
                "Changing the object protection of the selected object failed",
                "The database transaction failed due to a database error",
                "Check error report " + reportNr + " for a more information about this problem",
                null
            );
        }
        catch (LinStorException exc)
        {
            debugPrintHelper.printLsException(debugErr, exc);
        }
        finally
        {
            if (scopeLock != null)
            {
                scopeLock.close();
            }
            rcfgRdLock.unlock();
        }
    }

    // Caller must hold the appropriate locks for the protected object
    private void changeObjProt(
        final AccessContext accCtx,
        final PrintStream debugErr,
        final Map<String, String> parameters,
        final ProtectedObject obj,
        final boolean ovrdDscAccCtrl,
        final boolean ovrdMndAccCtrl,
        final StringBuilder responseMsg,
        final @Nullable String newOwnerPrm,
        final @Nullable String newSecTypePrm,
        final @Nullable String accessRolePrm,
        final @Nullable String accessTypePrm
    )
        throws AccessDeniedException, LinStorException, DatabaseException
    {
        final ObjectProtection objProt = obj.getObjProt();
        final TransactionMgr transMgr = trnActProvider.get();
        objProt.setConnection(transMgr);

        AccessContext chgAccCtx = accCtx.clone();
        PrivilegeSet effPriv = chgAccCtx.getEffectivePrivs();

        if (!(ovrdDscAccCtrl && ovrdMndAccCtrl))
        {
            effPriv.disablePrivileges(Privilege.PRIV_SYS_ALL);
        }
        if (ovrdDscAccCtrl)
        {
            boolean haveObjOwner = false;
            boolean haveObjControl = false;
            try
            {
                effPriv.enablePrivileges(Privilege.PRIV_OBJ_OWNER);
                haveObjOwner = true;
            }
            catch (AccessDeniedException ignored)
            {
            }
            try
            {
                effPriv.enablePrivileges(Privilege.PRIV_OBJ_CONTROL);
                haveObjControl = true;
            }
            catch (AccessDeniedException ignored)
            {
            }
            if (!(haveObjOwner && haveObjControl))
            {
                responseMsg.append("WARNING: The following DAC privileges could not be enabled:\n");
                if (!haveObjOwner)
                {
                    responseMsg.append("    ");
                    responseMsg.append(Privilege.PRIV_OBJ_OWNER.name);
                    responseMsg.append('\n');
                }
                if (!haveObjControl)
                {
                    responseMsg.append("    ");
                    responseMsg.append(Privilege.PRIV_OBJ_CONTROL.name);
                    responseMsg.append('\n');
                }
            }
        }
        if (ovrdMndAccCtrl)
        {
            try
            {
                effPriv.enablePrivileges(Privilege.PRIV_MAC_OVRD);
            }
            catch (AccessDeniedException accExc)
            {
                responseMsg.append("WARNING: The MAC privilege ");
                responseMsg.append(Privilege.PRIV_MAC_OVRD);
                responseMsg.append(" could not be enabled\n");
            }
        }

        boolean committed = false;
        try
        {
            if (newOwnerPrm != null)
            {
                try
                {
                    RoleName rlName = new RoleName(newOwnerPrm);
                    Role newOwner = Role.get(rlName);
                    if (newOwner != null)
                    {
                        // If the PRIV_OBJ_OWNER privilege is not enabled already, enable it temporarily
                        // This privilege is always required for changing ownership
                        if (!ovrdDscAccCtrl)
                        {
                            effPriv.enablePrivileges(Privilege.PRIV_OBJ_OWNER);
                        }
                        objProt.setOwner(chgAccCtx, newOwner);
                        // Disable the PRIV_OBJ_OWNER privilege again if it was not supposed to be
                        // enabled for all operations
                        if (!ovrdDscAccCtrl)
                        {
                            effPriv.disablePrivileges(Privilege.PRIV_OBJ_OWNER);
                        }

                        responseMsg.append("* Object owner changed to role '");
                        responseMsg.append(newOwner.name.displayValue);
                        responseMsg.append("'\n");
                    }
                    else
                    {
                        throw new LinStorException(
                            CMD_NAME + " command failed: " +
                            "The specified security role '" + rlName.displayValue + "' does not exist",
                            "The security role '" + rlName.displayValue + "' does not exist",
                            null,
                            null,
                            null
                        );
                    }
                }
                catch (InvalidNameException invName)
                {
                    throw new LinStorException(
                        CMD_NAME + " command failed: Invalid security role name '" + newOwnerPrm + "'",
                        "The specified security role name '" + newOwnerPrm + "' is not valid",
                        "The value entered for the parameter " + PRM_OBJ_OWNER +
                        " is not a valid security role name",
                        "Check whether the security role name was typed correctly",
                        null
                    );
                }
            }

            if (newSecTypePrm != null)
            {
                try
                {
                    SecTypeName typeName = new SecTypeName(newSecTypePrm);
                    SecurityType newSecType = SecurityType.get(typeName);
                    if (newSecType != null)
                    {
                        // Always use the fully privileged access context for changing the security type,
                        // since that operation requires all privileges
                        objProt.setSecurityType(accCtx, newSecType);
                        responseMsg.append("* Security type changed to '");
                        responseMsg.append(newSecType.name.displayValue);
                        responseMsg.append("'\n");
                    }
                    else
                    {
                        throw new LinStorException(
                            CMD_NAME + " command failed: " +
                            "The specified security type '" + typeName.displayValue + "' does not exist",
                            "The security type '" + typeName.displayValue + "' does not exist",
                            null,
                            null,
                            null
                        );
                    }
                }
                catch (InvalidNameException invName)
                {
                    throw new LinStorException(
                        CMD_NAME + " command failed: Invalid security type name '" + newSecTypePrm + "'",
                        "The specified security type name '" + newSecTypePrm + "' is not valid",
                        "The value entered for the parameter " + PRM_OBJ_SECTYPE +
                        " is not a valid security type name",
                        "Check whether the security type name was typed correctly",
                        null
                    );
                }
            }

            if (accessRolePrm != null && accessTypePrm != null)
            {
                RoleName acRoleName = null;
                try
                {
                    acRoleName = new RoleName(accessRolePrm);
                }
                catch (InvalidNameException invName)
                {
                    throw new LinStorException(
                        CMD_NAME + " command failed: Invalid security role name '" + accessRolePrm + "'",
                        "The specified security role name '" + accessRolePrm + "' is not valid",
                        "The value entered for the parameter " + PRM_OBJ_ROLE +
                        " is not a valid security role name",
                        "Check whether the security role name was typed correctly",
                        null
                    );
                }
                if (acRoleName != null)
                {
                    Role acRole = Role.get(acRoleName);
                    if (acRole != null)
                    {
                        if (accessTypePrm.equalsIgnoreCase(PRM_ACCESS_NONE))
                        {
                            // Delete ACL entry
                            objProt.delAclEntry(chgAccCtx, acRole);
                            responseMsg.append("* Removed access control entry for role '");
                            responseMsg.append(acRole.name.displayValue);
                            responseMsg.append("'\n");
                        }
                        else
                        {
                            // Create or update ACL entry
                            try
                            {
                                AccessType acType = AccessType.get(accessTypePrm);
                                objProt.addAclEntry(chgAccCtx, acRole, acType);
                                responseMsg.append("- Added access control entry for role '");
                                responseMsg.append(acRole.name.displayValue);
                                responseMsg.append("' with ");
                                responseMsg.append(acType.name());
                                responseMsg.append(" permission\n");
                            }
                            catch (InvalidNameException invName)
                            {
                                throw new LinStorException(
                                    CMD_NAME + " command failed: " +
                                    "The specified access type '" + accessTypePrm + "' is not valid",
                                    "The value '" + accessTypePrm + "' is not a valid value for the parameter '" +
                                    PRM_OBJ_ACCESS,
                                    null,
                                    "Valid values are:\n" +
                                    com.linbit.utils.StringUtils.join(
                                        " ",
                                        AccessType.VIEW.name(), AccessType.USE.name(),
                                        AccessType.CHANGE.name(), AccessType.CONTROL.name()
                                    ),
                                    null
                                );
                            }
                        }
                    }
                    else
                    {
                        throw new LinStorException(
                            CMD_NAME + " command failed: " +
                            "The specified security role '" + acRoleName.displayValue + "' does not exist",
                            "The security role '" + acRoleName.displayValue + "' does not exist",
                            null,
                            null,
                            null
                        );
                    }
                }
            }
            else
            {
                // If one of the parameters is null, but not both of them...
                if (accessRolePrm == null ^ accessTypePrm == null)
                {
                    debugPrintHelper.printMultiMissingParamError(
                        debugErr, parameters, PRM_OBJ_ROLE, PRM_OBJ_ACCESS
                    );
                }
            }

            transMgr.commit();
            committed = true;
        }
        finally
        {
            if (transMgr != null)
            {
                if (!committed)
                {
                    try
                    {
                        transMgr.rollback();
                    }
                    catch (TransactionException ignored)
                    {
                        // A database exception may have been the cause for a failed COMMIT,
                        // but the database connection must still be returned to the connection pool,
                        // therefore, catch and ignore the exception
                    }
                }
                transMgr.returnConnection();
            }
        }
    }

    // Caller must hold rcfgLock and nodeRpsLock
    private @Nullable Node getNode(AccessContext accCtx, String objName)
        throws AccessDeniedException, LinStorException
    {
        Node nodeObj = null;
        try
        {
            NodeName name = new NodeName(objName);
            nodeObj = nodeRps.get(accCtx, name);
        }
        catch (InvalidNameException invName)
        {
            throw new LinStorException(
                CMD_NAME + " command failed: Invalid node name '" + objName + "'",
                "The specified name '" + objName + "' is not a valid name for objects of class " + CLS_NODE,
                null,
                "Check whether the object name was typed correctly",
                null
            );
        }
        return nodeObj;
    }

    // Caller must hold rcfgLock and rscDfnRpsLock
    private @Nullable ResourceDefinition getResourceDefinition(AccessContext accCtx, String objName)
        throws AccessDeniedException, LinStorException
    {
        ResourceDefinition rscDfn = null;
        try
        {
            ResourceName name = new ResourceName(objName);
            rscDfn = rscDfnRps.get(accCtx, name);
        }
        catch (InvalidNameException invName)
        {
            throw new LinStorException(
                CMD_NAME + " command failed: Invalid resource definition name '" + objName + "'",
                "The specified name '" + objName + "' is not a valid name for objects of class " + CLS_RSCDFN,
                null,
                "Check whether the object name was typed correctly",
                null
            );
        }
        return rscDfn;
    }

    // Caller must hold rcfgLock, nodeRpsLock and rscDfnRpsLock
    private @Nullable Resource getResource(AccessContext accCtx, String objName)
        throws AccessDeniedException, LinStorException
    {
        int splitIdx = objName.indexOf('/');
        if (splitIdx == -1)
        {
            throw new LinStorException(
                CMD_NAME + " command failed: The specified resource name is not a node/rscdfn path",
                "The parameter " + PRM_OBJ_NAME + " is not valid for an object of type " + CLS_RSC,
                "The parameter does not specify a path that identifies the resource object",
                "A resource must be identified by a path name in the form NodeName/ResourceDefinitionName, " +
                "that is, the node name of the node where the resource is assigned, followed by a slash (/), " +
                "and then followed by the name of the resource definition associated with the resource.",
                null
            );
        }
        String nodeNameText = objName.substring(0, splitIdx);
        String rscDfnNameText = objName.substring(splitIdx + 1);
        NodeName nodeObjName;
        try
        {
            nodeObjName = new NodeName(nodeNameText);
        }
        catch (InvalidNameException invName)
        {
            throw new LinStorException(
                CMD_NAME + " command failed: Invalid node name",
                "The node name contained in the resource path is not valid",
                null,
                "Check whether the node name has been typed correctly",
                null
            );
        }
        ResourceName rscDfnName;
        try
        {
            rscDfnName = new ResourceName(rscDfnNameText);
        }
        catch (InvalidNameException invName)
        {
            throw new LinStorException(
                CMD_NAME + " command failed: Invalid resource definition name",
                "The resource definition name contained in the resource path is not valid",
                null,
                "Check whether the resource definition name has been typed correctly",
                null
            );
        }

        Resource rsc = null;
        Node nodeObj = nodeRps.get(accCtx, nodeObjName);
        if (nodeObj != null)
        {
            rsc = nodeObj.getResource(accCtx, rscDfnName);
        }

        return rsc;
    }

    // Caller must hold rcfgLock and storPoolDfnRpsLock
    private @Nullable StorPoolDefinition getStorPoolDefinition(AccessContext accCtx, String objName)
        throws AccessDeniedException, LinStorException
    {
        StorPoolDefinition storPoolDfn = null;
        try
        {
            StorPoolName name = new StorPoolName(objName);
            storPoolDfn = storPoolDfnRps.get(accCtx, name);
        }
        catch (InvalidNameException invName)
        {
            throw new LinStorException(
                CMD_NAME + " command failed: Invalid storage pool definition name '" + objName + "'",
                "The specified name '" + objName + "' is not a valid name for objects of class " + CLS_STORPOOLDFN,
                null,
                "Check whether the object name was typed correctly",
                null
            );
        }
        return storPoolDfn;
    }

    // Caller must hold rcfgLock and kvStoreRpsLock
    private @Nullable KeyValueStore getKeyValueStore(AccessContext accCtx, String objName)
        throws AccessDeniedException, LinStorException
    {
        KeyValueStore kvStoreObj;
        try
        {
            KeyValueStoreName kvName = new KeyValueStoreName(objName);
            kvStoreObj = keyValStoreRps.get(accCtx, kvName);
        }
        catch (InvalidNameException invName)
        {
            throw new LinStorException(
                CMD_NAME + " command failed: Invalid key/value store name '" + objName + "'",
                "The specified name '" + objName + "' is not a valid name for objects of class " + CLS_KEYVALSTOR,
                null,
                "Check whether the object name was typed correctly",
                null
            );
        }
        return kvStoreObj;
    }

    // Caller must hold rcfgLock
    private ProtectedObject getSystemObject(
        String objName,
        List<Lock> lockList,
        StringBuilder responseMsg
    )
        throws LinStorException
    {
        ProtectedObject protObj;
        String upperObjName = objName.toUpperCase();
        switch (upperObjName)
        {
            case SO_NODE_DIR:
                protObj = nodeRps;
                lockList.add(nodeRpsLock.writeLock());
                break;
            case SO_RSCDFN_DIR:
                protObj = rscDfnRps;
                lockList.add(rscDfnRpsLock.writeLock());
                break;
            case SO_STORPOOLDFN_DIR:
                protObj = storPoolDfnRps;
                lockList.add(storPoolDfnRpsLock.writeLock());
                break;
            case SO_CFGVAL:
                protObj = sysCfgRps;
                lockList.add(sysCfgLock.writeLock());
                break;
            case SO_SHUTDOWN:
                protObj = shutdownObj;
                break;
            default:
                throw new LinStorException(
                    CMD_NAME + " command failed: Invalid system object name '" + objName + "'",
                    "Invalid system object name '" + objName + "'",
                    null,
                    "Valid system object names are:\n" +
                    "    " + SO_NODE_DIR + "\n" +
                    "    " + SO_RSCDFN_DIR + "\n" +
                    "    " + SO_STORPOOLDFN_DIR + "\n" +
                    "    " + SO_CFGVAL + "\n" +
                    "    " + SO_SHUTDOWN + "\n",
                    null
                );
        }
        setObjInfo(responseMsg, upperObjName, CLS_SYSOBJ);
        return protObj;
    }

    private void setObjInfo(final StringBuilder responseMsg, final String objName, final String type)
    {
        responseMsg.append("Changed protection of object '");
        responseMsg.append(objName);
        responseMsg.append("' of type ");
        responseMsg.append(type);
        responseMsg.append(":\n");
    }

    private boolean parseOverrideOption(final String param, final Map<String, String> parameters)
        throws LinStorException
    {
        boolean result = false;
        String value = parameters.get(param);
        if (value != null)
        {
            if (value.equalsIgnoreCase(OPT_OVRD))
            {
                result = true;
            }
            else
            if (!value.equalsIgnoreCase(OPT_ENFORCE))
            {
                throw new LinStorException(
                    CMD_NAME + " command failed: Invalid value '" + value + "' for parameter " + param,
                    "The specified value '" + value + "' is not a valid value for the parameter " + param,
                    null,
                    "Valid values are '" + OPT_ENFORCE + "' and '" + OPT_OVRD + "'",
                    null
                );
            }
        }
        return result;
    }

    private LockGuard createScopeLock(final List<Lock> lockList)
    {
        Lock[] locksArray = new Lock[lockList.size()];
        locksArray = lockList.toArray(locksArray);
        LockGuard scopeLock = LockGuard.createLocked(locksArray);
        return scopeLock;
    }

    private class MissingParamException extends LinStorException
    {
        private final String[] checkParams;

        MissingParamException(String... paramList)
        {
            super(
                "A required parameter was not present on the command line.\n" +
                "This is a debug error message that is only displayed because this\n" +
                "exception was not handled properly.\n" +
                "Please report this problem to the developers of this application."
            );
            if (paramList == null)
            {
                throw new ImplementationError(
                    "Attempt to construct an instance of " + MissingParamException.class.getSimpleName() +
                    " with a null argument"
                );
            }
            checkParams = paramList;
        }

        private String[] getParamList()
        {
            return checkParams;
        }
    }
}

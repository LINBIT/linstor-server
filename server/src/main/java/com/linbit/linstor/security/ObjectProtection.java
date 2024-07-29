package com.linbit.linstor.security;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Security protection for linstor object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class ObjectProtection extends BaseTransactionObject implements Comparable<ObjectProtection>
{
    private static final String PATH_SEPARATOR               = "/";
    private static final String PATH_RESOURCES               = "/resources/";
    private static final String PATH_RESOURCE_DEFINITIONS    = "/resourcedefinitions/";
    private static final String PATH_SNAPSHOT_DEFINITIONS    = "/snapshotdefinitions/";
    private static final String PATH_RESOURCE_GROUPS         = "/resourcegroups/";
    private static final String PATH_NODES                   = "/nodes/";
    private static final String PATH_STOR_POOL_DEFINITIONS   = "/storpooldefinitions/";
    private static final String PATH_KVS                     = "/keyvaluestores/";
    private static final String PATH_EXT_FILES               = "/extfiles/";
    private static final String PATH_REMOTE = "/remote/";
    private static final String PATH_SCHEDULE = "/schedule/";

    private static final String PATH_SYS                     = "/sys/";
    private static final String PATH_CONTROLLER              = PATH_SYS + "controller/";
    private static final String PATH_SATELLITE               = PATH_SYS + "satellite/";

    public static final String DEFAULT_SECTYPE_NAME = "default";


    // Identity that created the object
    //
    // The creator's identity may change if the
    // account that was used to create the object
    // is deleted
    private final TransactionSimpleObject<ObjectProtection, Identity> objectCreator;

    // Role that has owner rights on the object
    private final TransactionSimpleObject<ObjectProtection, Role> objectOwner;

    // Access control list for the object
    private final AccessControlList objectAcl;
    private final Map<Role, AccessControlEntry> cachedAcl;

    // Security type for the object
    private final TransactionSimpleObject<ObjectProtection, SecurityType> objectType;

    // Database driver
    private final SecObjProtDatabaseDriver dbDriver;

    private final String objPath;

    /**
     * Creates an ObjectProtection instance for a newly created object
     *
     * @param accCtx The object creator's access context
     * @param driver The DatabaseDriver. Can be null for temporary objects
     * @param transObjFactoryRef
     * @param transMgrProvider
     * @throws DatabaseException
     */
    ObjectProtection(
        AccessContext accCtx,
        String objPathRef,
        AccessControlList objectAclRef,
        SecObjProtDatabaseDriver driver,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        ErrorCheck.ctorNotNull(ObjectProtection.class, AccessContext.class, accCtx);

        dbDriver = driver;
        objPath = objPathRef;

        SingleColumnDatabaseDriver<ObjectProtection, Identity> idDriver = null;
        SingleColumnDatabaseDriver<ObjectProtection, Role> roleDriver = null;
        SingleColumnDatabaseDriver<ObjectProtection, SecurityType> secTypeDriver = null;

        if (driver != null)
        {
            idDriver = driver.getCreatorIdentityDriver();
            roleDriver = driver.getOwnerRoleDriver();
            secTypeDriver = driver.getSecurityTypeDriver();
        }

        objectCreator = transObjFactoryRef.createTransactionSimpleObject(this, accCtx.subjectId, idDriver);
        objectOwner = transObjFactoryRef.createTransactionSimpleObject(this, accCtx.subjectRole, roleDriver);
        objectType = transObjFactoryRef.createTransactionSimpleObject(this, accCtx.subjectDomain, secTypeDriver);
        objectAcl = objectAclRef;
        cachedAcl = new HashMap<>();

        transObjs = Arrays.asList(
            objectCreator,
            objectOwner,
            objectType,
            objectAcl
        );
    }

    /**
     * Check whether a subject can be granted the requested level of access
     * to the object protected by this instance
     *
     * @param context The security context of the subject requesting access
     * @param requested The type of access requested by the subject
     * @throws AccessDeniedException If access is denied
     */
    public void requireAccess(AccessContext context, AccessType requested)
        throws AccessDeniedException
    {
        objectType.get().requireAccess(context, requested);
        objectAcl.requireAccess(context, requested);
    }

    /**
     * Returns the level of access to the object protected by this instance
     * that is granted to the specified security context
     *
     * @param context The security context of the subject requesting access
     * @return Allowed AccessType, or null if access is denied
     */
    public @Nullable AccessType queryAccess(AccessContext context)
    {
        AccessType result = null;
        {
            AccessType macAccess = objectType.get().queryAccess(context);
            AccessType rbacAccess = objectAcl.queryAccess(context);

            // Determine the level of access that is allowed by both security components
            result = AccessType.intersect(macAccess, rbacAccess);
        }
        return result;
    }

    public Identity getCreator()
    {
        return objectCreator.get();
    }

    public void resetCreator(AccessContext context)
        throws AccessDeniedException, DatabaseException
    {
        PrivilegeSet privs = context.getEffectivePrivs();
        privs.requirePrivileges(Privilege.PRIV_SYS_ALL);
        objectCreator.set(Identity.SYSTEM_ID);
    }

    public Role getOwner()
    {
        return objectOwner.get();
    }

    public void setOwner(AccessContext context, Role newOwner)
        throws AccessDeniedException, DatabaseException
    {
        PrivilegeSet privs = context.getEffectivePrivs();
        privs.requirePrivileges(Privilege.PRIV_OBJ_OWNER);
        objectOwner.set(newOwner);
    }

    public AccessControlList getAcl()
    {
        return objectAcl;
    }

    public SecurityType getSecurityType()
    {
        return objectType.get();
    }

    public void setSecurityType(AccessContext context, SecurityType newSecType)
        throws AccessDeniedException, DatabaseException
    {
        SecurityLevel globalSecLevel = SecurityLevel.get();
        switch (globalSecLevel)
        {
            case NO_SECURITY:
                break;
            case RBAC:
                // fall-through
            case MAC:
                PrivilegeSet privs = context.getEffectivePrivs();
                privs.requirePrivileges(Privilege.PRIV_SYS_ALL);
                break;
            default:
                throw new ImplementationError(
                    "Missing case label for enum constant " + globalSecLevel.name(),
                    null
                );
        }
        objectType.set(newSecType);
    }

    public void addAclEntry(AccessContext context, Role entryRole, AccessType grantedAccess)
        throws AccessDeniedException, DatabaseException
    {
        objectType.get().requireAccess(context, AccessType.CONTROL);
        if (context.subjectRole != objectOwner.get())
        {
            objectAcl.requireAccess(context, AccessType.CONTROL);
            // Only object owners or privileged users may change the access controls for the
            // role that is being used to change the entry
            if (context.subjectRole == entryRole &&
                !context.getEffectivePrivs().hasPrivileges(Privilege.PRIV_OBJ_CONTROL)
            )
            {
                throw new AccessDeniedException(
                    "Changing the access control entry for the role performing the " +
                    "change was denied",
                    // Description
                    "Permission to change the access control entry was denied",
                    // Cause
                    "Changing the access control entry for the role performing the " +
                    "change is not permitted",
                    // Correction
                    "- Use another authorized role to change the access control entry\n" +
                    "- Use the role that owns the object to change the access control entry\n" +
                    "- Use a role with administrative privileges to change the " +
                    "access control entry\n",
                    // No error details
                    null
                );
            }
        }
        objectAcl.addEntry(entryRole, grantedAccess);
    }

    public void delAclEntry(AccessContext context, Role entryRole)
        throws AccessDeniedException, DatabaseException
    {
        objectType.get().requireAccess(context, AccessType.CONTROL);
        if (context.subjectRole != objectOwner.get())
        {
            objectAcl.requireAccess(context, AccessType.CONTROL);
            if (context.subjectRole == entryRole &&
                !context.getEffectivePrivs().hasPrivileges(Privilege.PRIV_OBJ_CONTROL)
            )
            {
                throw new AccessDeniedException(
                    "Deleting the access control entry for the role performing the change " +
                    "was denied",
                    // Description
                    "Permission to change the access control entry was denied",
                    // Cause
                    "Changing the access control entry for the role performing the " +
                    "change is not permitted",
                    // Correction
                    "- Use another authorized role to change the access control entry\n" +
                    "- Use the role that owns the object to change the access control entry\n" +
                    "- Use a role with administrative privileges to change the " +
                    "access control entry\n",
                    // No error details
                    null
                );
            }
        }
        objectAcl.delEntry(entryRole);
    }

    @Override
    public boolean isDirty()
    {
        return super.isDirty() || !cachedAcl.isEmpty();
    }

    @Override
    public void commitImpl()
    {
        // noop, all sub-objects can commit themselves
    }

    @Override
    public void rollbackImpl()
    {
        // noop, all sub-objects can rollback themselves
    }

    /**
     * Deletes this object protection. In order to do this, the given access context
     * needs {@link AccessType#CONTROL}.
     * @param accCtx
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.CONTROL);
        activateTransMgr();
        objectAcl.deleteAll();
        dbDriver.delete(this);
    }

    @Override
    public int compareTo(ObjectProtection otherRef)
    {
        return objPath.compareTo(otherRef.objPath);
    }

    /**
     * ObjProt-Path for Resources
     *
     * @param nodeName
     * @param resDefName
     * @return
     */
    public static String buildPath(NodeName nodeName, ResourceName resDefName)
    {
        return PATH_RESOURCES +
            nodeName.value + PATH_SEPARATOR +
            resDefName.value;
    }

    /**
     * ObjProt-Path for ResourceDefinitions
     *
     * @param resDfnName
     * @return
     */
    public static String buildPath(ResourceName resDfnName)
    {
        return PATH_RESOURCE_DEFINITIONS + resDfnName.value;
    }

    /**
     * ObjProt-Path for SnapshotDefinitions
     */
    public static String buildPath(ResourceName rscName, SnapshotName snapName)
    {
        return PATH_SNAPSHOT_DEFINITIONS +
            rscName.value + PATH_SEPARATOR +
            snapName.value;
    }


    /**
     * ObjProt-Path for ResourceGroups
     *
     * @param resGrpName
     * @return
     */
    public static String buildPath(ResourceGroupName rscGrpName)
    {
        return PATH_RESOURCE_GROUPS + rscGrpName.value;
    }

    /**
     * ObjProt-Path for Controller
     *
     * @param subPath
     * @return
     */
    public static String buildPathController(String subPath)
    {
        return PATH_CONTROLLER + subPath;
    }

    /**
     * ObjProt-Path for satellite
     *
     * @param subPath
     * @return
     */
    public static String buildPathSatellite(String subPath)
    {
        return PATH_SATELLITE + subPath;
    }

    /**
     * ObjProt-Path for Nodes
     *
     * @param nodeName
     * @return
     */
    public static String buildPath(NodeName nodeName)
    {
        return PATH_NODES + nodeName.value;
    }

    /**
     * ObjProt-Path for StorPoolDefinitions
     *
     * @param storPoolName
     * @return
     */
    public static String buildPath(StorPoolName storPoolName)
    {
        return PATH_STOR_POOL_DEFINITIONS + storPoolName.value;
    }

    /**
     * ObjProt-Path for KeyValueStores
     *
     * @param kvsName
     * @return
     */
    public static String buildPath(KeyValueStoreName kvsName)
    {
        return PATH_KVS + kvsName.value;
    }

    /**
     * ObjProt-Path for External files
     *
     * @param nameRef
     *
     * @return
     */
    public static String buildPath(ExternalFileName nameRef)
    {
        return PATH_EXT_FILES + nameRef.extFileName;
    }

    /**
     * ObjProt-Path for Remotes
     *
     * @param remoteName
     *
     * @return
     */
    public static String buildPath(RemoteName remoteName)
    {
        return PATH_REMOTE + remoteName.value;
    }

    /**
     * ObjProt-Path for Schedules
     *
     * @param scheduleName
     *
     * @return
     */
    public static String buildPath(ScheduleName scheduleName)
    {
        return PATH_SCHEDULE + scheduleName.value;
    }

    String getObjectProtectionPath()
    {
        return objPath;
    }
}

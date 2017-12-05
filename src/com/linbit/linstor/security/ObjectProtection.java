package com.linbit.linstor.security;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.TransactionSimpleObject;
import com.linbit.linstor.BaseTransactionObject;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.Satellite;

/**
 * Security protection for linstor object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class ObjectProtection extends BaseTransactionObject
{
    private static final String PATH_SEPARATOR               = "/";
    private static final String PATH_RESOURCES               = "/resources/";
    private static final String PATH_RESOURCE_DEFINITIONS    = "/resourcedefinitions/";
    private static final String PATH_NODES                   = "/nodes/";
    private static final String PATH_STOR_POOL_DEFINITIONS   = "/storpooldefinitions/";

    private static final String PATH_SYS                     = "/sys/";
    private static final String PATH_CONTROLLER              = PATH_SYS + "controller/";
    private static final String PATH_SATELLITE               = PATH_SYS + "satellite/";

    public static final String DEFAULT_SECTYPE_NAME = "default";


    // Identity that created the object
    //
    // The creator's identity may change if the
    // account that was used to create the object
    // is deleted
    private TransactionSimpleObject<ObjectProtection, Identity> objectCreator;

    // Role that has owner rights on the object
    private TransactionSimpleObject<ObjectProtection, Role> objectOwner;

    // Access control list for the object
    private final AccessControlList objectAcl;
    private Map<Role, AccessControlEntry> cachedAcl;

    // Security type for the object
    private TransactionSimpleObject<ObjectProtection, SecurityType> objectType;

    // Database driver
    private ObjectProtectionDatabaseDriver dbDriver;

    // Is this object already persisted or not
    private boolean persisted;
    private String objPath;

    /**
     * Loads an ObjectProtection instance from the database.
     *
     * The {@code accCtx} parameter is only used when no ObjectProtection was found in the
     * database and the {@code createIfNotExists} parameter is set to true
     *
     * @param accCtx
     * @param transMgr
     * @param objPath
     * @param createIfNotExists
     * @return
     * @throws SQLException
     * @throws AccessDeniedException
     */
    public static ObjectProtection getInstance(
        AccessContext accCtx,
        String objPath,
        boolean createIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        ObjectProtectionDatabaseDriver dbDriver = LinStor.getObjectProtectionDatabaseDriver();
        ObjectProtection objProt = null;

        objProt = dbDriver.loadObjectProtection(objPath, false, transMgr);

        if (objProt == null && createIfNotExists)
        {
            objProt = new ObjectProtection(accCtx, objPath, dbDriver);
            dbDriver.insertOp(objProt, transMgr);
            // as we just created a new ObjProt, we have to set the permissions
            // use the *Impl to skip the access checks as there are no rules yet and would cause
            // an exception
            objProt.addAclEntryImpl(accCtx.subjectRole, AccessType.CONTROL);
            // as we are not initialized yet, we have to add the acl entry manually in the DB
            dbDriver.insertAcl(objProt, accCtx.subjectRole, AccessType.CONTROL, transMgr);
        }

        if (objProt != null)
        {
            objProt.requireAccess(accCtx, AccessType.CHANGE);
            objProt.objPath = objPath;
            objProt.dbDriver = dbDriver;

            if (transMgr == null)
            {
                // satellite
                objProt.persisted = false;
            }
            else
            {
                transMgr.register(objProt);
                objProt.persisted = true;
            }

            objProt.initialized();
            objProt.setConnection(transMgr);
        }

        return objProt;
    }

    /**
     * Creates an ObjectProtection instance for a newly created object
     *
     * @param accCtx The object creator's access context
     * @param driver The DatabaseDriver. Can be null for temporary objects
     * @throws SQLException
     */
    ObjectProtection(AccessContext accCtx, String objPathRef, ObjectProtectionDatabaseDriver driver)
    {
        ErrorCheck.ctorNotNull(ObjectProtection.class, AccessContext.class, accCtx);

        dbDriver = driver;
        objPath = objPathRef;

        SingleColumnDatabaseDriver<ObjectProtection, Identity> idDriver = null;
        SingleColumnDatabaseDriver<ObjectProtection, Role> roleDriver = null;
        SingleColumnDatabaseDriver<ObjectProtection, SecurityType> secTypeDriver = null;

        if (driver != null)
        {
            idDriver = driver.getIdentityDatabaseDrier();
            roleDriver = driver.getRoleDatabaseDriver();
            secTypeDriver = driver.getSecurityTypeDriver();
        }

        objectCreator = new TransactionSimpleObject<>(this, accCtx.subjectId, idDriver);
        objectOwner = new TransactionSimpleObject<>(this, accCtx.subjectRole, roleDriver);
        objectType = new TransactionSimpleObject<>(this, accCtx.subjectDomain, secTypeDriver);
        objectAcl = new AccessControlList();
        cachedAcl = new HashMap<>();

        transObjs = Arrays.<TransactionObject>asList(
            objectCreator,
            objectOwner,
            objectType
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
    public AccessType queryAccess(AccessContext context)
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
        throws AccessDeniedException, SQLException
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
        throws AccessDeniedException, SQLException
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
        throws AccessDeniedException, SQLException
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
        throws AccessDeniedException, SQLException
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
        addAclEntryImpl(entryRole, grantedAccess);
    }

    private void addAclEntryImpl(Role entryRole, AccessType grantedAccess) throws SQLException
    {
        AccessControlEntry oldValue = objectAcl.addEntry(entryRole, grantedAccess);
        setAcl(entryRole, grantedAccess, oldValue);
    }

    public void delAclEntry(AccessContext context, Role entryRole)
        throws AccessDeniedException, SQLException
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
        AccessControlEntry oldEntry = objectAcl.delEntry(entryRole);
        delAcl(entryRole, oldEntry);
    }

    @Override
    public boolean isDirty()
    {
        return super.isDirty() || !cachedAcl.isEmpty();
    }

    @Override
    public void commit()
    {
        super.commit();
        cachedAcl.clear();
    }

    @Override
    public void rollback()
    {
        super.rollback();

        for (Entry<Role, AccessControlEntry> entry : cachedAcl.entrySet())
        {
            if (entry.getValue() == null)
            {
                objectAcl.delEntry(entry.getKey());
            }
            else
            {
                objectAcl.addEntry(entry.getKey(), entry.getValue().access);
            }
        }
        cachedAcl.clear();
    }

    private void setAcl(Role entryRole, AccessType grantedAccess, AccessControlEntry oldEntry) throws SQLException
    {
        if (isInitialized())
        {
            ensureObjProtIsPersisted();

            if (oldEntry == null)
            {
                dbDriver.insertAcl(this, entryRole, grantedAccess, transMgr);
            }
            else
            {
                dbDriver.updateAcl(this, entryRole, grantedAccess, transMgr);
            }

            if (!cachedAcl.containsKey(entryRole))
            {
                cachedAcl.put(entryRole, oldEntry);
            }
        }
    }

    private void delAcl(Role entryRole, AccessControlEntry oldEntry) throws SQLException
    {
        if (isInitialized())
        {
            ensureObjProtIsPersisted();

            dbDriver.deleteAcl(this, entryRole, transMgr);

            if (!cachedAcl.containsKey(entryRole))
            {
                cachedAcl.put(entryRole, oldEntry);
            }
        }
    }

    private void ensureObjProtIsPersisted() throws SQLException
    {
        if (!persisted)
        {
            dbDriver.insertOp(this, transMgr);
            persisted = true;
        }
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
     * ObjProt-Path for Controller
     *
     * @param controller
     * @param subPath
     * @return
     */
    public static String buildPath(Controller controller, String subPath)
    {
        return PATH_CONTROLLER + subPath;
    }

    /**
     * ObjProt-Path for satellite
     *
     * @param satellite
     * @param subPath
     * @return
     */
    public static String buildPath(Satellite satellite, String subPath)
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
    public static String buildPathSPD(StorPoolName storPoolName)
    {
        return PATH_STOR_POOL_DEFINITIONS + storPoolName.value;
    }

    String getObjectProtectionPath()
    {
        return objPath;
    }
}

package com.linbit.drbdmanage.security;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.TransactionSimpleObject;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.DrbdManage;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.Satellite;
import com.linbit.drbdmanage.StorPoolName;

/**
 * Security protection for drbdmanageNG object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class ObjectProtection extends BaseTransactionObject
{
    private static final String PATH_SEPARATOR               = "/";
    private static final String PATH_RESOURCES               = "/resources/";
    private static final String PATH_RESOURCE_DEFINITIONS    = "/resourcedefinitions/";
    private static final String PATH_NODES                   = "/nodes/";
    private static final String PATH_NET_INTERFACES          = "/netinterfaces/";
    private static final String PATH_SYS                     = "/sys/";
    private static final String PATH_STOR_POOL_DEFINITIONS   = "/storpooldefinitions/";
    private static final String PATH_STOR_POOLS              = "/storpools/";
    private static final String PATH_CONNECTION_DEFINITIONS  = "/connectiondefinitions/";

    private static final String PATH_CONTROLLER              = PATH_SYS + "controller/";
    private static final String PATH_SATELLITE               = PATH_SYS + "satellite/";

    public static final String DEFAULT_SECTYPE_NAME = "default";


    // Identity that created the object
    //
    // The creator's identity may change if the
    // account that was used to create the object
    // is deleted
    private TransactionSimpleObject<Identity> objectCreator;

    // Role that has owner rights on the object
    private TransactionSimpleObject<Role> objectOwner;

    // Access control list for the object
    private final AccessControlList objectAcl;
    private Map<Role, AccessControlEntry> cachedAcl;

    // Security type for the object
    private TransactionSimpleObject<SecurityType> objectType;

    // Database driver
    private ObjectProtectionDatabaseDriver dbDriver;

    // Is this object already persisted or not
    private boolean persisted;

    private boolean initialized;

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
    public static ObjectProtection getInstance(AccessContext accCtx, TransactionMgr transMgr, String objPath, boolean createIfNotExists)
        throws SQLException, AccessDeniedException
    {
        ObjectProtectionDatabaseDriver dbDriver = DrbdManage.getObjectProtectionDatabaseDriver(objPath);
        ObjectProtection objProt = null;

        if (transMgr != null)
        {
            objProt = dbDriver.loadObjectProtection(transMgr.dbCon);
        }

        if (objProt == null && createIfNotExists)
        {
            objProt = new ObjectProtection(accCtx, dbDriver);
            if (transMgr != null)
            {
                dbDriver.insertOp(transMgr.dbCon, objProt);
            }
        }

        if (objProt != null)
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);
            objProt.dbDriver = dbDriver;

            if (transMgr == null)
            {
                // satellite
                objProt.persisted = false;
                objProt.dbCon = null;
            }
            else
            {
                transMgr.register(objProt);
                objProt.dbCon = transMgr.dbCon;
                objProt.persisted = true;
            }
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
    ObjectProtection(AccessContext accCtx, ObjectProtectionDatabaseDriver driver)
    {
        ErrorCheck.ctorNotNull(ObjectProtection.class, AccessContext.class, accCtx);

        dbDriver = driver;

        ObjectDatabaseDriver<Identity> idDriver = null;
        ObjectDatabaseDriver<Role> roleDriver = null;
        ObjectDatabaseDriver<SecurityType> secTypeDriver = null;

        if (driver != null)
        {
            idDriver = driver.getIdentityDatabaseDrier();
            roleDriver = driver.getRoleDatabaseDriver();
            secTypeDriver = driver.getSecurityTypeDriver();
        }

        objectCreator = new TransactionSimpleObject<Identity>(accCtx.subjectId, idDriver);
        objectOwner = new TransactionSimpleObject<Role>(accCtx.subjectRole, roleDriver);
        objectType = new TransactionSimpleObject<SecurityType>(accCtx.subjectDomain, secTypeDriver);
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

        updateOp();
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

        updateOp();
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

        updateOp();
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

    private void updateOp() throws SQLException
    {
        if (initialized && dbCon != null)
        {
            if (!persisted)
            {
                dbDriver.insertOp(dbCon, this);
                persisted = true;
            }
            else
            {
                dbDriver.updateOp(dbCon, this);
            }
        }
    }

    private void setAcl(Role entryRole, AccessType grantedAccess, AccessControlEntry oldEntry) throws SQLException
    {
        if (initialized)
        {
            if (dbCon != null)
            {
                if (!persisted)
                {
                    updateOp();
                }

                if (oldEntry == null)
                {
                    dbDriver.insertAcl(dbCon, entryRole, grantedAccess);
                }
                else
                {
                    dbDriver.updateAcl(dbCon, entryRole, grantedAccess);
                }
            }
            if (!cachedAcl.containsKey(entryRole))
            {
                cachedAcl.put(entryRole, oldEntry);
            }
        }
    }

    private void delAcl(Role entryRole, AccessControlEntry oldEntry) throws SQLException
    {
        if (initialized)
        {
            if (dbCon != null)
            {
                if (!persisted)
                {
                    updateOp();
                }

                dbDriver.deleteAcl(dbCon, entryRole);
            }
            if (!cachedAcl.containsKey(entryRole))
            {
                cachedAcl.put(entryRole, oldEntry);
            }
        }
    }

    public static String buildPath(NodeName nodeName, ResourceName resDefName)
    {
        return PATH_RESOURCES +
            nodeName.value + PATH_SEPARATOR +
            resDefName.value;
    }

    public static String buildPath(ResourceName resDfnName)
    {
        return PATH_RESOURCE_DEFINITIONS + resDfnName.value;
    }

    /**
     * @param controller
     * @param subPath
     */
    public static String buildPath(Controller controller, String subPath)
    {
        return PATH_CONTROLLER + subPath;
    }

    /**
     * @param satellite
     * @param subPath
     */
    public static String buildPath(Satellite satellite, String subPath)
    {
        return PATH_SATELLITE + subPath;
    }

    public static String buildPath(NodeName nodeName, NetInterfaceName netName)
    {
        return PATH_NET_INTERFACES +
            nodeName.value + PATH_SEPARATOR +
            netName.value;
    }

    public static String buildPath(NodeName nodeName)
    {
        return PATH_NODES + nodeName.value;
    }

    public static String buildPathSPD(StorPoolName storPoolName)
    {
        return PATH_STOR_POOL_DEFINITIONS + storPoolName.value;
    }

    public static String buildPathSP(StorPoolName storPoolName)
    {
        return PATH_STOR_POOLS + storPoolName.value;
    }

    public static String buildPath(NodeName sourceName, NodeName targetName)
    {
        NodeName source;
        NodeName target;

        if (sourceName.compareTo(targetName) < 0)
        {
            source = sourceName;
            target = targetName;
        }
        else
        {
            source = targetName;
            target = sourceName;
        }

        return PATH_CONNECTION_DEFINITIONS +
            source.value + PATH_SEPARATOR +
            target.value;
    }
}

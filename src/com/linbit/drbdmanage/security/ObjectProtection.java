package com.linbit.drbdmanage.security;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.TransactionSimpleObject;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.DrbdManage;
import com.linbit.drbdmanage.NetInterface;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.Satellite;

/**
 * Security protection for drbdmanageNG object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class ObjectProtection implements TransactionObject
{
    private static final String PATH_SEPARATOR               = "/";
    private static final String PATH_RESOURCES               = "/resources/";
    private static final String PATH_RESOURCE_DEFINITIONS    = "/resourcedefinitions/";
    private static final String PATH_NODES                   = "/nodes/";
    private static final String PATH_NET_INTERFACES          = "/netinterfaces/";
    private static final String PATH_SYS                     = "/sys/";
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

    // Database connection
    private Connection dbCon;

    // Database driver
    private ObjectProtectionDatabaseDriver dbDriver;

    // Is this object already persisted or not
    private boolean persisted;

    /**
     * Loads an ObjectProtection instance from the database.
     *
     * The {@code accCtx} parameter is only used when no ObjectProtection was found in the
     * database and the {@code createIfNotExists} parameter is set to true
     *
     * @param transMgr
     * @param objPath
     * @param createIfNotExists
     * @param accCtx
     * @return
     * @throws SQLException
     */
    public static ObjectProtection load(TransactionMgr transMgr, String objPath, boolean createIfNotExists, AccessContext accCtx) throws SQLException
    {
        if (transMgr == null)
        {
            throw new ImplementationError("Trying to load an ObjectProtection without a TransactionManager", new NullPointerException());
        }
        ObjectProtectionDatabaseDriver dbDriver = DrbdManage.getObjectProtectionDatabaseDriver(objPath);
        ObjectProtection objProt = dbDriver.loadObjectProtection(transMgr.dbCon);

        if (objProt == null && createIfNotExists)
        {
            objProt = create(objPath, accCtx, transMgr);
        }

        if (objProt != null)
        {
            objProt.dbDriver = dbDriver;
            objProt.dbCon = transMgr.dbCon;
            objProt.persisted = true;
            transMgr.register(objProt);
        }

        return objProt;
    }

    public static ObjectProtection create(String objPath, AccessContext accCtx, TransactionMgr transMgr) throws SQLException
    {
        ObjectProtectionDatabaseDriver dbDriver = DrbdManage.getObjectProtectionDatabaseDriver(objPath);
        ObjectProtection objProt = new ObjectProtection(accCtx, dbDriver);
        objProt.dbDriver = dbDriver;
        if (transMgr == null)
        {
            objProt.persisted = false;
            objProt.dbCon = null;
        }
        else
        {
            transMgr.register(objProt);
            objProt.dbCon = transMgr.dbCon;
            dbDriver.insertOp(transMgr.dbCon, objProt);
            objProt.persisted = true;
        }

        return objProt;
    }


    ObjectProtection(AccessContext accCtx)
    {
        this(accCtx, null);
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

    // TODO dont forget to call this method (ObjectProtection.destroy(Connection) )
    public void destroy(Connection con) throws SQLException
    {
        dbDriver.deleteOp(con);
        persisted = false;
    }

    @Override
    public void setConnection(TransactionMgr transMgr)
    {
        transMgr.register(this);
        dbCon = transMgr.dbCon;
    }

    @Override
    public boolean isDirty()
    {
        return !cachedAcl.isEmpty() || objectCreator.isDirty() ||
            objectOwner.isDirty() || objectType.isDirty();
    }

    @Override
    public void commit()
    {
        objectCreator.commit();
        objectOwner.commit();
        objectType.commit();

        cachedAcl.clear();
    }

    @Override
    public void rollback()
    {
        objectCreator.rollback();
        objectOwner.rollback();
        objectType.rollback();

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
        if (dbCon != null)
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

    private void delAcl(Role entryRole, AccessControlEntry oldEntry) throws SQLException
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

    public static String buildPath(ResourceData res)
    {
        String nodeName = res.getAssignedNode().getName().value;
        String defName = res.getDefinition().getName().value;
        return PATH_RESOURCES +
            nodeName + PATH_SEPARATOR +
            defName;
    }

    public static String buildPath(ResourceDefinitionData resourceDefinitionData)
    {
        return PATH_RESOURCE_DEFINITIONS + resourceDefinitionData.getName().value;
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

    public static String buildPath(NetInterface netInterfaceData)
    {
        String nodeName = netInterfaceData.getNode().getName().value;
        String netName = netInterfaceData.getName().value;
        return PATH_NET_INTERFACES +
            nodeName + PATH_SEPARATOR +
            netName;
    }

    public static String buildPath(Node node)
    {
        return PATH_NODES + node.getName().value;
    }

}

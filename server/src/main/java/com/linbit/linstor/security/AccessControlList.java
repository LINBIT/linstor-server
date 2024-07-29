package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Object access control list
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class AccessControlList extends BaseTransactionObject
{
    private final SecObjProtAclDatabaseDriver dbDriver;
    private final Map<RoleName, AccessControlEntry> acl;
    private final String objPath;

    AccessControlList(
        String objPathRef,
        SecObjProtAclDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        this(objPathRef, new TreeMap<>(), dbDriverRef, transObjFactoryRef, transMgrProviderRef);
    }

    AccessControlList(
        String objPathRef,
        Map<RoleName, AccessControlEntry> backingMapRef,
        SecObjProtAclDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        objPath = objPathRef;
        dbDriver = dbDriverRef;
        TransactionMap<AccessControlList, RoleName, AccessControlEntry> txAcl = transObjFactoryRef
            .createTransactionPrimitiveMap(this, backingMapRef, null);
        acl = Collections.synchronizedMap(txAcl);

        transObjs = Arrays.asList(txAcl);
    }

    /**
     * Checks whether the subject has the requested type of access
     * to objects protected by this access control list instance
     *
     * @param context The security context of the subject requesting access
     * @param requested The type of access requested by the subject
     * @throws AccessDeniedException If access is denied
     */
    public void requireAccess(AccessContext context, AccessType requested)
        throws AccessDeniedException
    {
        synchronized (acl)
        {
            SecurityLevel globalSecLevel = SecurityLevel.get();
            switch (globalSecLevel)
            {
                case NO_SECURITY:
                    break;
                case RBAC:
                    // fall-through
                case MAC:
                    boolean allowFlag = false;

                    // Look for an entry for the subject's role in this access control list
                    AccessControlEntry entry = acl.get(context.subjectRole.name);

                    // If an entry was found, check whether the requested level of access
                    // is within the bounds of the level of access allowed by the
                    // access control entry.
                    // If no entry was found, access is denied.
                    if (entry != null)
                    {
                        allowFlag = entry.access.hasAccess(requested);
                    }

                    if (!allowFlag)
                    {
                        allowFlag |= hasAccessPrivilege(context, requested);
                    }

                    if (!allowFlag)
                    {
                        throw new AccessDeniedException(
                            "Access of type '" + requested + "' not allowed by the " +
                                "access control list",
                            // Description
                            "Access to the protected object was denied",
                            // Cause
                            "The access control list for the protected object does not allow " +
                                "access of type " + requested.name() + " by role " +
                                context.subjectRole.name,
                            // Correction
                            "An entry that allows access must be added by an authorized role",
                            // No error details
                            null
                        );
                    }
                    break;
                default:
                    throw new ImplementationError(
                        "Missing case label for enum constant " + globalSecLevel.name(),
                        null
                    );
            }
        }
    }

    /**
     * Returns the level of access to the object protected by this access control list instance
     * that is granted to the specified security context
     *
     * @param context The security context of the subject requesting access
     * @return Allowed AccessType, or null if access is denied
     */
    public AccessType queryAccess(AccessContext context)
    {
        AccessType result = null;
        SecurityLevel globalSecLevel = SecurityLevel.get();
        switch (globalSecLevel)
        {
            case NO_SECURITY:
                result = AccessType.CONTROL;
                break;
            case RBAC:
                // fall-through
            case MAC:
                // Query the level of access allowed by privileges
                AccessType privAccess = context.privEffective.toRbacAccess();

                // Look for an entry for the subject's role in this access control list
                AccessType aclAccess = null;
                {
                    AccessControlEntry entry = acl.get(context.subjectRole.name);
                    if (entry != null)
                    {
                        aclAccess = entry.access;
                    }
                }

                // Combine access permissions
                result = AccessType.union(privAccess, aclAccess);
                break;
            default:
                throw new AssertionError(globalSecLevel.name());
        }
        return result;
    }

    /**
     * Returns the level of access that is granted by the access control list
     * to the role referenced by the specified security context
     *
     * @param context Security context for access controls
     * @return Allowed level of access, or null if access is denied
     */
    public AccessType getEntry(AccessContext context)
    {
        Role subjRole = context.subjectRole;
        return getEntry(subjRole);
    }

    /**
     * Returns the level of access that is granted by the access control list
     * to the specified role
     *
     * @param subjRole The role to find access control entries for
     * @return Allowed level of access, or null if access is denied
     */
    public @Nullable AccessType getEntry(Role subjRole)
    {

        AccessType access = null;
        synchronized (acl)
        {
            AccessControlEntry entry = acl.get(subjRole.name);
            if (entry != null)
            {
                access = entry.access;
            }
        }
        return access;
    }

    @Nullable
    AccessControlEntry addEntry(Role entryRole, AccessType grantedAccess) throws DatabaseException
    {
        synchronized (acl)
        {
            AccessControlEntry entry = new AccessControlEntry(objPath, entryRole, grantedAccess);
            AccessControlEntry oldEntry = acl.put(entryRole.name, entry);
            if (oldEntry == null)
            {
                dbDriver.create(entry);
            }
            else
            {
                dbDriver.getAccessTypeDriver().update(entry, oldEntry.access);
            }
            return oldEntry;
        }
    }

    public void deleteAll() throws DatabaseException
    {
        synchronized (acl)
        {
            for (AccessControlEntry acEntry : acl.values())
            {
                dbDriver.delete(acEntry);
            }
            acl.clear();
        }
    }

    AccessControlEntry delEntry(Role entryRole) throws DatabaseException
    {
        synchronized (acl)
        {
            AccessControlEntry acEntry = acl.remove(entryRole.name);
            dbDriver.delete(acEntry);
            return acEntry;
        }
    }

    public Map<RoleName, AccessControlEntry> getEntries()
    {
        Map<RoleName, AccessControlEntry> aclCopy = new TreeMap<>();
        synchronized (acl)
        {
            aclCopy.putAll(acl);
        }
        return aclCopy;
    }

    private boolean hasAccessPrivilege(AccessContext context, AccessType requested)
    {
        PrivilegeSet privileges = context.privEffective;

        boolean allowFlag = false;
        switch (requested)
        {
            case VIEW:
                allowFlag |= privileges.hasSomePrivilege(
                    Privilege.PRIV_OBJ_VIEW,
                    Privilege.PRIV_OBJ_USE,
                    Privilege.PRIV_OBJ_CHANGE,
                    Privilege.PRIV_OBJ_CONTROL,
                    Privilege.PRIV_OBJ_OWNER
                );
                break;
            case USE:
                allowFlag |= privileges.hasSomePrivilege(
                    Privilege.PRIV_OBJ_USE,
                    Privilege.PRIV_OBJ_CHANGE,
                    Privilege.PRIV_OBJ_CONTROL,
                    Privilege.PRIV_OBJ_OWNER
                );
                break;
            case CHANGE:
                allowFlag |= privileges.hasSomePrivilege(
                    Privilege.PRIV_OBJ_CHANGE,
                    Privilege.PRIV_OBJ_CONTROL,
                    Privilege.PRIV_OBJ_OWNER
                );
                break;
            case CONTROL:
                allowFlag |= privileges.hasSomePrivilege(
                    Privilege.PRIV_OBJ_CONTROL,
                    Privilege.PRIV_OBJ_OWNER
                );
                break;
            default:
                throw new ImplementationError(
                    "Switch statement reached default case, unhandled enumeration case",
                    null
                );
        }
        return allowFlag;
    }
}

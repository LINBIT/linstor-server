package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecAclMap;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.ETCDSingleColumnDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SEC_ACL_MAP;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SEC_OBJECT_PROTECTION;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SEC_ROLES;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecObjectProtection.CREATOR_IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecObjectProtection.OWNER_ROLE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecObjectProtection.SECURITY_TYPE_NAME;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@Singleton
public class ObjectProtectionEtcdDriver extends BaseEtcdDriver implements ObjectProtectionDatabaseDriver
{
    private final AccessContext accCtx;
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transObjFactory;

    private final SingleColumnDatabaseDriver<ObjectProtection, Identity> identityDriver;
    private final SingleColumnDatabaseDriver<ObjectProtection, Role> roleDriver;
    private final SingleColumnDatabaseDriver<ObjectProtection, SecurityType> securityTypeDriver;

    @Inject
    public ObjectProtectionEtcdDriver(
        @SystemContext AccessContext accCtxRef,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        accCtx = accCtxRef;
        errorReporter = errorReporterRef;
        transObjFactory = transObjFactoryRef;

        Function<ObjectProtection, String[]> pkFkt = objProt -> new String[]
        {
            objProt.getObjectProtectionPath()
        };
        identityDriver = new ETCDSingleColumnDriver<>(
            transMgrProviderRef,
            CREATOR_IDENTITY_NAME,
            pkFkt,
            id -> id.name.value
        );
        roleDriver = new ETCDSingleColumnDriver<>(
            transMgrProviderRef,
            OWNER_ROLE_NAME,
            pkFkt,
            role -> role.name.value
        );
        securityTypeDriver = new ETCDSingleColumnDriver<>(
            transMgrProviderRef,
            SECURITY_TYPE_NAME,
            pkFkt,
            secType -> secType.name.value
        );
    }

    @Override
    public void insertOp(ObjectProtection objProtRef) throws DatabaseException
    {
        namespace(SEC_OBJECT_PROTECTION, objProtRef.getObjectProtectionPath())
            .put(CREATOR_IDENTITY_NAME, objProtRef.getCreator().name.value)
            .put(OWNER_ROLE_NAME, objProtRef.getOwner().name.value)
            .put(SECURITY_TYPE_NAME, objProtRef.getSecurityType().name.value);
    }

    @Override
    public void deleteOp(String objectPathRef) throws DatabaseException
    {
        namespace(SEC_ACL_MAP, objectPathRef)
                .delete(true);
        namespace(SEC_OBJECT_PROTECTION, objectPathRef)
            .delete(true);
    }

    @Override
    public void insertAcl(ObjectProtection parentRef, Role roleRef, AccessType grantedAccessRef)
        throws DatabaseException
    {
        updateAcl(parentRef, roleRef, grantedAccessRef);
    }

    @Override
    public void updateAcl(ObjectProtection parentRef, Role roleRef, AccessType grantedAccessRef)
        throws DatabaseException
    {
        namespace(SEC_ACL_MAP, parentRef.getObjectProtectionPath(), roleRef.name.value)
            .put(SecAclMap.ACCESS_TYPE, Short.toString(grantedAccessRef.getAccessMask()));
    }

    @Override
    public void deleteAcl(ObjectProtection parent, Role role) throws DatabaseException
    {
        String aclId = getAclId(parent.getObjectProtectionPath(), role.name.displayValue);
        errorReporter.logTrace("Deleting AccessControl entry %s", aclId);
        namespace(SEC_ACL_MAP, parent.getObjectProtectionPath(), role.name.value)
            .delete(true);
        errorReporter.logTrace("AccessControl entry deleted %s", aclId);
    }

    @Override
    public ObjectProtection loadObjectProtection(String objectPathRef, boolean logWarnIfNotExistsRef)
        throws DatabaseException
    {
        errorReporter.logTrace("Loading ObjectProtection %s", getObjProtId(objectPathRef));

        Map<String, String> opMap = namespace(SEC_OBJECT_PROTECTION, objectPathRef).get(true);
        Set<String> composedOpKeys = EtcdUtils.getComposedPkList(opMap);

        ObjectProtection objProt = null;
        if (composedOpKeys.isEmpty())
        {
            if (logWarnIfNotExistsRef)
            {
                errorReporter.logWarning("ObjectProtection not found in DB %s", getObjProtId(objectPathRef));
            }
        }
        else
        if (composedOpKeys.size() > 1)
        {
            throw new ImplementationError("Unexpected count of object protections found: " + composedOpKeys.size());
        }
        else
        {
            String composedOpKey = composedOpKeys.iterator().next();
            Identity identity = null;
            Role role = null;
            SecurityType secType = null;
            try
            {
                identity = Identity.get(
                    new IdentityName(
                        opMap.get(
                            EtcdUtils.buildKey(CREATOR_IDENTITY_NAME, composedOpKey)
                        )
                    )
                );
                // TODO: role should be part of the key, not a dedicated column
                String roleNameStr = opMap.get(EtcdUtils.buildKey(OWNER_ROLE_NAME, composedOpKey));
                role = Role.get(new RoleName(roleNameStr));
                secType = SecurityType.get(
                    new SecTypeName(
                        opMap.get(
                            EtcdUtils.buildKey(SECURITY_TYPE_NAME, composedOpKey)
                        )
                    )
                );

                Map<String, String> roleMap = namespace(SEC_ROLES, roleNameStr).get(true);

                PrivilegeSet privLimitSet = new PrivilegeSet(
                    Long.parseLong(
                        roleMap.get(
                            EtcdUtils.buildKey(SecRoles.ROLE_PRIVILEGES, roleNameStr)
                        )
                    )
                );
                AccessContext loadedAccCtx = new AccessContext(identity, role, secType, privLimitSet);
                objProt = new ObjectProtection(loadedAccCtx, objectPathRef, this, transObjFactory, transMgrProvider);
                objProt.setPersisted(true);
            }
            catch (InvalidNameException invalidNameExc)
            {
                String name;
                String invalidValue = invalidNameExc.invalidName;
                if (identity == null)
                {
                    name = "IdentityName";
                }
                else
                if (role == null)
                {
                    name = "RoleName";
                }
                else
                {
                    name = "SecTypeName";
                }
                throw new LinStorDBRuntimeException(
                    String.format(
                        "A stored %s in the namespace %s could not be restored." +
                            "(ObjectPath=%s, invalid %s=%s)",
                        name,
                        SEC_OBJECT_PROTECTION.getName(),
                        objectPathRef,
                        name,
                        invalidValue
                    ),
                    invalidNameExc
                );
            }
        }

        if (objProt != null)
        {
            errorReporter.logTrace("ObjectProtection instance created. %s", getObjProtId(objectPathRef));
            // restore ACL

            Map<String, String> aclMap = namespace(SEC_ACL_MAP, objectPathRef).get(true);
            Set<String> aclKeys = EtcdUtils.getComposedPkList(aclMap);
            try
            {
                for (String aclKey : aclKeys)
                {
                    Role role = Role.get(
                        new RoleName(
                            aclMap.get(
                                EtcdUtils.buildKey(SecAclMap.ROLE_NAME, aclKey)
                            )
                        )
                    );
                    AccessType type = AccessType.get(
                        Integer.parseInt(
                            aclMap.get(
                                EtcdUtils.buildKey(SecAclMap.ACCESS_TYPE, aclKey)
                            )
                        )
                    );
                    objProt.restoreAclEntry(role, type);
                }
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "A stored RoleName in the namespace %s could not be restored." +
                            "(ObjectPath=%s, invalid RoleName=%s)",
                        SEC_ACL_MAP.getName(),
                        objectPathRef,
                        invalidNameExc.invalidName
                    ),
                    invalidNameExc
                );
            }
        }

        errorReporter.logTrace("AccessControl entries restored %s", getObjProtId(objectPathRef));

        errorReporter.logTrace("ObjectProtection loaded %s", getObjProtId(objectPathRef));
        return objProt;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier()
    {
        return identityDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver()
    {
        return roleDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver()
    {
        return securityTypeDriver;
    }

    public static String getAclId(String objPath, String roleName, AccessType acType)
    {
        return "(ObjectPath=" + objPath + " Role=" + roleName + " AccessType=" + acType + ")";
    }

    public static String getAclId(String objPath, String roleName)
    {
        return "(ObjectPath=" + objPath + " Role=" + roleName + ")";
    }

    public static String getObjProtId(String objectPathRef)
    {
        return "(ObjProtPath=" + objectPathRef + ")";
    }
}

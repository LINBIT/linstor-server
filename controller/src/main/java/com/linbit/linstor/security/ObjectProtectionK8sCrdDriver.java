package com.linbit.linstor.security;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecObjectProtection;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecObjectProtectionSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SEC_ACL_MAP;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SEC_OBJECT_PROTECTION;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SEC_ROLES;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;

@Singleton
public class ObjectProtectionK8sCrdDriver implements ObjectProtectionDatabaseDriver
{
    private final ErrorReporter errorReporter;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    private final SingleColumnDatabaseDriver<ObjectProtection, ?> updateDriver;

    @Inject
    public ObjectProtectionK8sCrdDriver(
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        updateDriver = (objProt, ignored) -> transMgrProvider.get().getTransaction()
            .replace(SEC_OBJECT_PROTECTION, asCrd(objProt));
    }

    @Override
    public void insertOp(ObjectProtection objProtRef) throws DatabaseException
    {
        transMgrProvider.get().getTransaction().create(SEC_OBJECT_PROTECTION, asCrd(objProtRef));
    }

    @Override
    public void deleteOp(String objectPathRef) throws DatabaseException
    {
        transMgrProvider.get().getTransaction().delete(
            SEC_OBJECT_PROTECTION,
            GenCrdCurrent.createSecObjectProtection(
                objectPathRef,
                null,
                null,
                null
            )
        );
    }

    @Override
    public void insertAcl(ObjectProtection parent, Role role, AccessType accessType)
        throws DatabaseException
    {
        transMgrProvider.get().getTransaction().create(
            SEC_ACL_MAP,
            asAclMapCrd(parent, role, accessType)
        );
    }

    @Override
    public void updateAcl(ObjectProtection parent, Role role, AccessType accessType)
        throws DatabaseException
    {
        transMgrProvider.get().getTransaction().replace(
            SEC_ACL_MAP,
            asAclMapCrd(parent, role, accessType)
        );
    }

    @Override
    public void deleteAcl(ObjectProtection parent, Role role) throws DatabaseException
    {
        String aclId = getAclId(parent.getObjectProtectionPath(), role.name.displayValue);
        errorReporter.logTrace("Deleting AccessControl entry %s", aclId);
        transMgrProvider.get().getTransaction().delete(
            SEC_ACL_MAP,
            asAclMapCrd(
                parent,
                role,
                null
            )
        );
        errorReporter.logTrace("AccessControl entry deleted %s", aclId);
    }

    @Override
    public ObjectProtection loadObjectProtection(String objectPathRef, boolean logWarnIfNotExistsRef)
        throws DatabaseException
    {
        ObjectProtection objProt = null;

        errorReporter.logTrace("Loading ObjectProtection %s", getObjProtId(objectPathRef));

        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        SecObjectProtectionSpec objProtSpec = tx.getSpec(
            SEC_OBJECT_PROTECTION,
            objProtCrdTmp -> objProtCrdTmp.getSpec().objectPath.equals(objectPathRef),
            false,
            null
        );

        if (objProtSpec == null)
        {
            if (logWarnIfNotExistsRef)
            {
                errorReporter.logWarning("ObjectProtection not found in DB %s", getObjProtId(objectPathRef));
            }
        }
        else
        {
            Identity identity = null;
            Role role = null;
            SecurityType secType = null;
            try
            {
                identity = Identity.get(new IdentityName(objProtSpec.creatorIdentityName));
                role = Role.get(new RoleName(objProtSpec.ownerRoleName));
                secType = SecurityType.get(new SecTypeName(objProtSpec.securityTypeName));

                GenCrdCurrent.SecRolesSpec loadedRoleSpec = tx.getSpec(
                    SEC_ROLES,
                    roleCrdTmp -> roleCrdTmp.getSpec().roleName.equals(objProtSpec.ownerRoleName),
                    true,
                    "Role " + objProtSpec.ownerRoleName + " not found in the Database"
                );

                PrivilegeSet privLimitSet = new PrivilegeSet(loadedRoleSpec.rolePrivileges);
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

            HashMap<String, GenCrdCurrent.SecAclMapSpec> map = tx.getSpec(
                SEC_ACL_MAP,
                aclCrdTmp -> aclCrdTmp.getSpec().objectPath.equals(objectPathRef)
            );
            try
            {
                for (GenCrdCurrent.SecAclMapSpec aclSpec : map.values())
                {
                    Role role = Role.get(new RoleName(aclSpec.roleName));
                    AccessType type = AccessType.get(aclSpec.accessType);

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

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier()
    {
        return (SingleColumnDatabaseDriver<ObjectProtection, Identity>) updateDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver()
    {
        return (SingleColumnDatabaseDriver<ObjectProtection, Role>) updateDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver()
    {
        return (SingleColumnDatabaseDriver<ObjectProtection, SecurityType>) updateDriver;
    }

    private SecObjectProtection asCrd(ObjectProtection objProt)
    {
        return GenCrdCurrent.createSecObjectProtection(
            objProt.getObjectProtectionPath(),
            objProt.getCreator().name.value,
            objProt.getOwner().name.value,
            objProt.getSecurityType().name.value
        );
    }

    private GenCrdCurrent.SecAclMap asAclMapCrd(
        ObjectProtection objProt,
        Role roleRef,
        @Nullable AccessType accessTypeRef
    )
    {
        return GenCrdCurrent.createSecAclMap(
            objProt.getObjectProtectionPath(),
            roleRef.name.value,
            accessTypeRef == null ? 0 : accessTypeRef.getAccessMask()
        );
    }
}

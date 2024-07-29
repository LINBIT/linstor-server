package com.linbit.linstor.security;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecDefaultRoleDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecDfltRolesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecIdRoleMapSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecIdentitiesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecRolesSpec;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;
import com.linbit.linstor.security.pojo.SignInEntryPojo;
import com.linbit.linstor.transaction.ControllerK8sCrdTransactionMgr;
import com.linbit.linstor.transaction.ControllerK8sCrdTransactionMgrGenerator;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DbK8sCrdPersistence extends BaseDbAccessor<ControllerK8sCrdDatabase>
{
    private final ControllerK8sCrdTransactionMgrGenerator txMgrGen;

    @Inject
    public DbK8sCrdPersistence(
        ControllerK8sCrdTransactionMgrGenerator txMgrGenRef,
        SecIdentityDatabaseDriver secIdDriverRef,
        SecConfigDatabaseDriver secCfgDriverRef,
        SecDefaultRoleDatabaseDriver secDfltRoleDriverRef
    )
    {
        super(secIdDriverRef, secCfgDriverRef, secDfltRoleDriverRef);
        txMgrGen = txMgrGenRef;
    }

    @Override
    public SignInEntryPojo getSignInEntry(ControllerK8sCrdDatabase k8sDb, IdentityName idName) throws DatabaseException
    {
        SignInEntryPojo signInEntry = null;

        ControllerK8sCrdTransactionMgr tx = txMgrGen.startTransaction();
        SecIdentitiesSpec identity = loadIdentity(tx, idName, false);

        if (identity != null)
        {
            SecDfltRolesSpec dfltRole = loadDefaultRole(tx, idName, true);
            SecRolesSpec role = loadRole(tx, dfltRole.roleName, true);

            signInEntry = new SignInEntryPojo(
                identity.identityDspName,
                dfltRole.roleName,
                role.domainName,
                role.rolePrivileges,
                identity.passSalt,
                identity.passHash
            );
        }
        return signInEntry;
    }

    @Override
    public @Nullable IdentityRoleEntryPojo getIdRoleMapEntry(
        ControllerK8sCrdDatabase k8sCrdDb,
        IdentityName idName,
        RoleName rlName
    )
        throws DatabaseException
    {
        IdentityRoleEntryPojo identityRoleEntry = null;

        SecIdRoleMapSpec idRoleSpec = loadIdRole(txMgrGen.startTransaction(), idName, rlName, false);
        if (idRoleSpec != null)
        {
            identityRoleEntry = new IdentityRoleEntryPojo(
                idRoleSpec.identityName,
                idRoleSpec.roleName
            );
        }

        return identityRoleEntry;
    }

    @Override
    public @Nullable IdentityRoleEntryPojo getDefaultRole(ControllerK8sCrdDatabase k8sCrdDb, IdentityName idName)
        throws DatabaseException
    {
        IdentityRoleEntryPojo identityRoleEntry = null;

        SecDfltRolesSpec dfltRolesSpec = loadDefaultRole(txMgrGen.startTransaction(), idName, false);
        if (dfltRolesSpec != null)
        {
            identityRoleEntry = new IdentityRoleEntryPojo(dfltRolesSpec.identityName, dfltRolesSpec.roleName);
        }
        return identityRoleEntry;
    }

    private @Nullable SecIdentitiesSpec loadIdentity(
        ControllerK8sCrdTransactionMgr tx,
        IdentityName idNameRef,
        boolean failIfNull
    )
        throws DatabaseException
    {
        return tx.getTransaction().getSpec(
            GeneratedDatabaseTables.SEC_IDENTITIES,
            idCrd -> idNameRef.value.equals(idCrd.getSpec().identityName),
            failIfNull,
            "Identity with name '" + idNameRef + "' not found in database"
        );
    }

    private @Nullable SecDfltRolesSpec loadDefaultRole(
        ControllerK8sCrdTransactionMgr tx,
        IdentityName idNameRef,
        boolean failIfNull
    )
        throws DatabaseException
    {
        return tx.getTransaction().getSpec(
            GeneratedDatabaseTables.SEC_DFLT_ROLES,
            dfltRoleCrd -> idNameRef.value.equals(dfltRoleCrd.getSpec().identityName),
            failIfNull,
            "Identity with name '" + idNameRef + "' not found in database"
        );
    }

    private @Nullable SecRolesSpec loadRole(
        ControllerK8sCrdTransactionMgr tx,
        String roleNameRef,
        boolean failIfNull
    )
        throws DatabaseException
    {
        return tx.getTransaction().getSpec(
            GeneratedDatabaseTables.SEC_ROLES,
            roleCrd -> roleNameRef.equalsIgnoreCase(roleCrd.getSpec().roleName),
            failIfNull,
            "Role with name '" + roleNameRef + "' not found in database"
        );
    }

    private @Nullable SecIdRoleMapSpec loadIdRole(
        ControllerK8sCrdTransactionMgr tx,
        IdentityName idName,
        RoleName roleName,
        boolean failIfNull
    )
        throws DatabaseException
    {
        return tx.getTransaction().getSpec(
            GeneratedDatabaseTables.SEC_ID_ROLE_MAP,
            idRoleCrd -> idRoleCrd.getSpec().identityName.equals(idName.value) &&
                idRoleCrd.getSpec().roleName.equals(roleName.value),
            failIfNull,
            "Id " + idName.displayValue + " has no Role " + roleName.displayValue + " defined"
        );
    }

}

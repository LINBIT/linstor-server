package com.linbit.linstor.security;

import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecDfltRoles;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdRoleMap;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecDefaultRoleDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;
import com.linbit.linstor.security.pojo.SignInEntryPojo;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
public class DbEtcdPersistence extends BaseDbAccessor<ControllerETCDDatabase>
    implements DbAccessor<ControllerETCDDatabase>
{
    @Inject
    public DbEtcdPersistence(
        SecIdentityDatabaseDriver secIdDriverRef,
        SecConfigDatabaseDriver secCfgDriverRef,
        SecDefaultRoleDatabaseDriver secDfltRoleDriverRef
    )
    {
        super(secIdDriverRef, secCfgDriverRef, secDfltRoleDriverRef);
    }

    @Override
    public @Nullable SignInEntryPojo getSignInEntry(ControllerETCDDatabase etcdDb, IdentityName idName)
        throws DatabaseException
    {
        SignInEntryPojo signInEntry = null;

        Map<String, String> identityRow = EtcdUtils.getTableRow(
            etcdDb.getKvClient(),
            EtcdUtils.buildKey(GeneratedDatabaseTables.SEC_IDENTITIES, idName.value)
        );

        if (identityRow.size() > 0)
        {
            Map<String, String> dfltRoleRow = EtcdUtils.getTableRow(
                etcdDb.getKvClient(),
                EtcdUtils.buildKey(GeneratedDatabaseTables.SEC_DFLT_ROLES, idName.value)
            );

            if (dfltRoleRow.isEmpty())
            {
                throw new DatabaseException(GeneratedDatabaseTables.SEC_DFLT_ROLES.getName() + " is empty");
            }
            Map<String, String> roleRow = EtcdUtils.getTableRow(
                etcdDb.getKvClient(),
                EtcdUtils.buildKey(
                    GeneratedDatabaseTables.SEC_ROLES,
                    dfltRoleRow.get(SecDfltRoles.ROLE_NAME.getName())
                )
            );
            if (roleRow.isEmpty())
            {
                throw new DatabaseException(GeneratedDatabaseTables.SEC_ROLES.getName() + " is empty");
            }

            Long rolePrivileges;
            {
                String rolePrivilegesStr = roleRow.get(SecRoles.ROLE_PRIVILEGES.getName());
                rolePrivileges = rolePrivilegesStr == null ? null : Long.parseLong(rolePrivilegesStr);
            }
            signInEntry = new SignInEntryPojo(
                identityRow.get(SecIdentities.IDENTITY_NAME.getName()),
                dfltRoleRow.get(SecDfltRoles.ROLE_NAME.getName()),
                roleRow.get(SecRoles.DOMAIN_NAME.getName()),
                rolePrivileges,
                identityRow.get(SecIdentities.PASS_SALT.getName()),
                identityRow.get(SecIdentities.PASS_HASH.getName())
            );
        }
        return signInEntry;
    }

    @Override
    public @Nullable IdentityRoleEntryPojo getIdRoleMapEntry(
        ControllerETCDDatabase etcdDb,
        IdentityName idName,
        RoleName rlName
    )
        throws DatabaseException
    {
        IdentityRoleEntryPojo identityRoleEntry = null;
        Map<String, String> idRoleMapRow = EtcdUtils.getTableRow(
            etcdDb.getKvClient(),
            EtcdUtils.buildKey(
                GeneratedDatabaseTables.SEC_ID_ROLE_MAP,
                idName.value,
                rlName.value
            )
        );

        if (idRoleMapRow.size() > 0)
        {
            identityRoleEntry = new IdentityRoleEntryPojo(
                idRoleMapRow.get(SecIdRoleMap.IDENTITY_NAME.getName()),
                idRoleMapRow.get(SecIdRoleMap.ROLE_NAME.getName())
            );
        }

        return identityRoleEntry;
    }

    @Override
    public @Nullable IdentityRoleEntryPojo getDefaultRole(ControllerETCDDatabase etcdDb, IdentityName idName)
        throws DatabaseException
    {
        IdentityRoleEntryPojo identityRoleEntry = null;
        Map<String, String> dfltRoleRow = EtcdUtils.getTableRow(
            etcdDb.getKvClient(),
            EtcdUtils.buildKey(
                GeneratedDatabaseTables.SEC_DFLT_ROLES,
                idName.value
            )
        );

        if (dfltRoleRow.size() > 0)
        {
            identityRoleEntry = new IdentityRoleEntryPojo(
                dfltRoleRow.get(SecDfltRoles.IDENTITY_NAME.getName()),
                dfltRoleRow.get(SecDfltRoles.ROLE_NAME.getName())
            );
        }

        return identityRoleEntry;
    }
}

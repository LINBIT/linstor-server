package com.linbit.linstor.security;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.core.objects.EtcdDbDriver;
import com.linbit.linstor.dbcp.etcd.DbEtcd;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.security.data.IdentityRoleEntry;
import com.linbit.linstor.security.data.SignInEntry;
import com.linbit.linstor.security.data.TypeEnforcementRule;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DOMAIN_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PASS_HASH;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PASS_SALT;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ROLE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ROLE_PRIVILEGES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_IDENTITIES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_ROLES;

import java.util.List;
import java.util.Map;

import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.RangeResponse;

import static com.ibm.etcd.client.KeyUtils.bs;

public class DbEtcdPersistence implements DbAccessor
{
    @Override
    public SignInEntry getSignInEntry(ControllerDatabase ctrlDb, IdentityName idName) throws DatabaseException
    {
        ControllerETCDDatabase etcdDb = (ControllerETCDDatabase) ctrlDb;

        Map<String, String> identityRow = DbEtcd.getTableRow(
            etcdDb.getKvClient(),
            EtcdDbDriver.tblKey(GeneratedDatabaseTables.SEC_IDENTITIES, idName.value, "")
        );

        if (identityRow.size() > 0)
        {
            final String identityName = identityRow.get(IDENTITY_NAME);

            Map<String, String> roleRow = DbEtcd.getTableRow(
                etcdDb.getKvClient(),
                EtcdDbDriver.tblKey(GeneratedDatabaseTables.SEC_ROLES, idName.value, "")
            );

            return new SignInEntry(
                identityName,
                identityName,
                roleRow.get(DOMAIN_NAME),
                Long.parseLong(roleRow.get(ROLE_PRIVILEGES)),
                identityRow.get(PASS_SALT),
                identityRow.get(PASS_HASH)
            );
        }
        return null;
    }

    @Override
    public IdentityRoleEntry getIdRoleMapEntry(
        ControllerDatabase ctrlDb,
        IdentityName idName,
        RoleName rlName
    ) throws DatabaseException
    {
        return null;
    }

    @Override
    public IdentityRoleEntry getDefaultRole(ControllerDatabase ctrlDb, IdentityName idName) throws DatabaseException
    {
        return null;
    }

    @Override
    public List<String> loadIdentities(ControllerDatabase ctrlDb) throws DatabaseException
    {
        return null;
    }

    @Override
    public List<String> loadSecurityTypes(ControllerDatabase ctrlDb) throws DatabaseException
    {
        return null;
    }

    @Override
    public List<String> loadRoles(ControllerDatabase ctrlDb) throws DatabaseException
    {
        return null;
    }

    @Override
    public List<TypeEnforcementRule> loadTeRules(ControllerDatabase ctrlDb) throws DatabaseException
    {
        return null;
    }

    @Override
    public String loadSecurityLevel(ControllerDatabase ctrlDb) throws DatabaseException
    {
        return null;
    }

    @Override
    public boolean loadAuthRequired(ControllerDatabase ctrlDb) throws DatabaseException
    {
        return false;
    }

    @Override
    public void setSecurityLevel(ControllerDatabase ctrlDb, SecurityLevel newLevel) throws DatabaseException
    {

    }

    @Override
    public void setAuthRequired(ControllerDatabase ctrlDb, boolean newPolicy) throws DatabaseException
    {

    }
}

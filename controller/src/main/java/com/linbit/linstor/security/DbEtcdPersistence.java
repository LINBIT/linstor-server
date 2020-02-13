package com.linbit.linstor.security;

import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecAccessTypes;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecConfiguration;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecDfltRoles;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdRoleMap;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypeRules;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypes;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;
import com.linbit.linstor.security.pojo.SignInEntryPojo;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;
import com.linbit.linstor.transaction.ControllerETCDTransactionMgr;
import com.linbit.linstor.transaction.EtcdTransaction;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@Singleton
public class DbEtcdPersistence implements DbAccessor<ControllerETCDDatabase>
{
    @Inject
    public DbEtcdPersistence()
    {
    }

    @Override
    public SignInEntryPojo getSignInEntry(ControllerETCDDatabase etcdDb, IdentityName idName) throws DatabaseException
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
    public IdentityRoleEntryPojo getIdRoleMapEntry(
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
                GeneratedDatabaseTables.SEC_IDENTITIES,
                StringUtils.join(EtcdUtils.PK_DELIMITER, idName.value, rlName.value),
                ""
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
    public IdentityRoleEntryPojo getDefaultRole(ControllerETCDDatabase etcdDb, IdentityName idName)
        throws DatabaseException
    {
        IdentityRoleEntryPojo identityRoleEntry = null;
        Map<String, String> dfltRoleRow = EtcdUtils.getTableRow(
            etcdDb.getKvClient(),
            EtcdUtils.buildKey(
                GeneratedDatabaseTables.SEC_IDENTITIES,
                idName.value,
                ""
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

    @Override
    public List<String> loadIdentities(ControllerETCDDatabase etcdDb) throws DatabaseException
    {
        return loadDistinctSecDspNames(etcdDb, SecIdentities.IDENTITY_DSP_NAME);
    }

    @Override
    public List<String> loadSecurityTypes(ControllerETCDDatabase etcdDb) throws DatabaseException
    {
        return loadDistinctSecDspNames(etcdDb, SecTypes.TYPE_DSP_NAME);
    }

    @Override
    public List<String> loadRoles(ControllerETCDDatabase etcdDb) throws DatabaseException
    {
        return loadDistinctSecDspNames(etcdDb, SecRoles.ROLE_DSP_NAME);
    }

    private List<String> loadDistinctSecDspNames(ControllerETCDDatabase etcdDb, Column dspNameCol)
    {
        Set<String> pkSet = getPkSet(
            EtcdUtils.getTableRow(
                etcdDb.getKvClient(),
                EtcdUtils.buildKey(dspNameCol.getTable())
            )
        );
        List<String> ret = new ArrayList<>();
        for (String pk : pkSet)
        {
            ret.add(
                EtcdUtils.getFirstValue(
                    etcdDb.getKvClient(),
                    EtcdUtils.buildKey(dspNameCol, pk)
                )
            );
        }
        return ret;
    }

    private Set<String> getPkSet(Map<String, String> tableRowRef)
    {
        Set<String> ret = new TreeSet<>();
        for (String key : tableRowRef.keySet())
        {
            ret.add(extractPrimaryKey(key));
        }
        return ret;
    }

    private String extractPrimaryKey(String key)
    {
        // key is something like
        // /LINSTOR/$table/$composedPk/$column = $valueOfColumn
        int postDelimIdx = key.lastIndexOf(EtcdUtils.PATH_DELIMITER);
        int preDelimIdx = key.lastIndexOf(EtcdUtils.PATH_DELIMITER, postDelimIdx - 1);
        return key.substring(preDelimIdx + 1, postDelimIdx);
    }

    @Override
    public List<TypeEnforcementRulePojo> loadTeRules(ControllerETCDDatabase etcdDb) throws DatabaseException
    {
        Set<String> composedPkList = getPkSet(
            EtcdUtils.getTableRow(
                etcdDb.getKvClient(),
                EtcdUtils.buildKey(GeneratedDatabaseTables.SEC_TYPE_RULES)
            )
        );

        TreeMap<String, String> accTypeIntToName = new TreeMap<>();
        Map<String, String> accTypeTable = EtcdUtils
            .getTableRow(etcdDb.getKvClient(), EtcdUtils.buildKey(GeneratedDatabaseTables.SEC_ACCESS_TYPES));

        for (Entry<String, String> entry : accTypeTable.entrySet())
        {
            if (entry.getKey().endsWith(SecAccessTypes.ACCESS_TYPE_VALUE.getName()))
            {
                accTypeIntToName.put(entry.getValue(), extractPrimaryKey(entry.getKey()));
            }
        }

        List<TypeEnforcementRulePojo> ret = new ArrayList<>();
        for (String composedPk : composedPkList)
        {
            String[] pk = EtcdUtils.splitPks(composedPk, false);
            ret.add(
                new TypeEnforcementRulePojo(
                    pk[0],
                    pk[1],
                    accTypeIntToName.get(
                        EtcdUtils.getFirstValue(
                            etcdDb.getKvClient(),
                            EtcdUtils.buildKey(
                                SecTypeRules.ACCESS_TYPE,
                                pk
                            )
                        )
                    )
                )
            );
        }
        return ret;
    }

    @Override
    public String loadSecurityLevel(ControllerETCDDatabase etcdDb) throws DatabaseException
    {
        return EtcdUtils.getFirstValue(
            etcdDb.getKvClient(),
            EtcdUtils.buildKey(SecConfiguration.ENTRY_VALUE, SecurityDbConsts.KEY_SEC_LEVEL)
        );
    }

    @Override
    public boolean loadAuthRequired(ControllerETCDDatabase etcdDb) throws DatabaseException
    {
        return Boolean.parseBoolean(
            EtcdUtils.getFirstValue(
                etcdDb.getKvClient(),
                EtcdUtils.buildKey(GeneratedDatabaseTables.SEC_CONFIGURATION, SecurityDbConsts.KEY_AUTH_REQ)
            )
        );
    }

    @Override
    public void setSecurityLevel(ControllerETCDDatabase etcdDb, SecurityLevel newLevel) throws DatabaseException
    {
        /**
         * This action should always have its own transaction (i.e. no other actions should
         * be active concurrently)
         */
        ControllerETCDTransactionMgr txMgr = new ControllerETCDTransactionMgr(etcdDb, 1);
        EtcdTransaction tx = txMgr.getTransaction();
        tx.put(
            EtcdUtils.buildKey(SecConfiguration.ENTRY_VALUE, SecurityDbConsts.KEY_SEC_LEVEL),
            newLevel.name()
        );
        txMgr.commit();
    }

    @Override
    public void setAuthRequired(ControllerETCDDatabase etcdDb, boolean newPolicy) throws DatabaseException
    {
        /**
         * This action should always have its own transaction (i.e. no other actions should
         * be active concurrently)
         */
        ControllerETCDTransactionMgr txMgr = new ControllerETCDTransactionMgr(etcdDb, 1);
        EtcdTransaction tx = txMgr.getTransaction();

        tx.put(
            EtcdUtils.buildKey(GeneratedDatabaseTables.SEC_CONFIGURATION, SecurityDbConsts.KEY_AUTH_REQ),
            Boolean.toString(newPolicy)
        );
        txMgr.commit();
    }
}

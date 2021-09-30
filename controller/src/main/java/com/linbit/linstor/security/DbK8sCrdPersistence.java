package com.linbit.linstor.security;

import com.linbit.StringConv;
import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdentities;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.Rollback;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecAccessTypesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecConfigurationSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecDfltRolesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecIdRoleMapSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecIdentitiesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.SecRolesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;
import com.linbit.linstor.security.pojo.SignInEntryPojo;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;
import com.linbit.linstor.transaction.ControllerK8sCrdCurrentTransactionMgr;
import com.linbit.linstor.transaction.ControllerK8sCrdTransactionMgrGenerator;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class DbK8sCrdPersistence implements DbAccessor<ControllerK8sCrdDatabase>
{
    private final ControllerK8sCrdTransactionMgrGenerator txMgrGen;

    @Inject
    public DbK8sCrdPersistence(ControllerK8sCrdTransactionMgrGenerator txMgrGenRef)
    {
        txMgrGen = txMgrGenRef;
    }

    @Override
    public SignInEntryPojo getSignInEntry(ControllerK8sCrdDatabase k8sDb, IdentityName idName) throws DatabaseException
    {
        SignInEntryPojo signInEntry = null;

        ControllerK8sCrdCurrentTransactionMgr tx = txMgrGen.startTransaction();
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
    public IdentityRoleEntryPojo getIdRoleMapEntry(
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
    public IdentityRoleEntryPojo getDefaultRole(ControllerK8sCrdDatabase k8sCrdDb, IdentityName idName)
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

    @Override
    public List<String> loadIdentities(ControllerK8sCrdDatabase k8sCrdDb) throws DatabaseException
    {
        return loadDistinctSecDspNames(txMgrGen.startTransaction(), SecIdentities.IDENTITY_DSP_NAME);
    }

    @Override
    public List<String> loadSecurityTypes(ControllerK8sCrdDatabase k8sCrdDb) throws DatabaseException
    {
        return loadDistinctSecDspNames(txMgrGen.startTransaction(), SecTypes.TYPE_DSP_NAME);
    }

    @Override
    public List<String> loadRoles(ControllerK8sCrdDatabase k8sCrdDb) throws DatabaseException
    {
        return loadDistinctSecDspNames(txMgrGen.startTransaction(), SecRoles.ROLE_DSP_NAME);
    }

    @Override
    public List<TypeEnforcementRulePojo> loadTeRules(ControllerK8sCrdDatabase k8sCrdDb) throws DatabaseException
    {
        List<TypeEnforcementRulePojo> ret = new ArrayList<>();

        ControllerK8sCrdCurrentTransactionMgr tx = txMgrGen.startTransaction();
        Map<String, GenCrdCurrent.SecTypeRulesSpec> typeRulesMap = tx.getTransaction().get(
            GeneratedDatabaseTables.SEC_TYPE_RULES
        );
        Map<String, GenCrdCurrent.SecAccessTypesSpec> typeAccessTypesMap = tx.getTransaction().get(
            GeneratedDatabaseTables.SEC_ACCESS_TYPES
        );

        HashMap<Short, SecAccessTypesSpec> accessTypeSpecsLut = new HashMap<>();
        for (GenCrdCurrent.SecAccessTypesSpec accessTypeSpec : typeAccessTypesMap.values())
        {
            accessTypeSpecsLut.put(accessTypeSpec.accessTypeValue, accessTypeSpec);
        }

        for (GenCrdCurrent.SecTypeRulesSpec typeRuleSpec : typeRulesMap.values())
        {
            SecAccessTypesSpec accessTypesSpec = accessTypeSpecsLut.get(typeRuleSpec.accessType);

            ret.add(
                new TypeEnforcementRulePojo(
                    typeRuleSpec.domainName,
                    typeRuleSpec.typeName,
                    accessTypesSpec.accessTypeName
                )
            );
        }
        return ret;
    }

    @Override
    public String loadSecurityLevel(ControllerK8sCrdDatabase k8sCrdDb) throws DatabaseException
    {
        return getValueOfConfigSpec(txMgrGen.startTransaction(), SecurityDbConsts.KEY_SEC_LEVEL);
    }

    @Override
    public boolean loadAuthRequired(ControllerK8sCrdDatabase k8sCrdDb) throws DatabaseException
    {
        return StringConv.getDfltBoolean(
            getValueOfConfigSpec(txMgrGen.startTransaction(), SecurityDbConsts.KEY_AUTH_REQ),
            true
        );
    }

    @Override
    public void setSecurityLevel(ControllerK8sCrdDatabase k8sCrdDb, SecurityLevel newLevel) throws DatabaseException
    {
        /*
         * This action should always have its own transaction (i.e. no other actions should
         * be active concurrently)
         */
        ControllerK8sCrdCurrentTransactionMgr txMgr = new ControllerK8sCrdCurrentTransactionMgr(k8sCrdDb);
        K8sCrdTransaction<GenCrdCurrent.Rollback> tx = txMgr.getTransaction();
        tx.update(
            GeneratedDatabaseTables.SEC_CONFIGURATION,
            GenCrdCurrent.createSecConfiguration(
                SecurityDbConsts.KEY_SEC_LEVEL,
                SecurityDbConsts.KEY_DSP_SEC_LEVEL,
                newLevel.name()
            )
        );
        txMgr.commit();
    }

    @Override
    public void setAuthRequired(ControllerK8sCrdDatabase k8sCrdDb, boolean newPolicy) throws DatabaseException
    {
        /*
         * This action should always have its own transaction (i.e. no other actions should
         * be active concurrently)
         */
        ControllerK8sCrdCurrentTransactionMgr txMgr = new ControllerK8sCrdCurrentTransactionMgr(k8sCrdDb);
        K8sCrdTransaction<Rollback> tx = txMgr.getTransaction();
        tx.update(
            GeneratedDatabaseTables.SEC_CONFIGURATION,
            GenCrdCurrent.createSecConfiguration(
                SecurityDbConsts.KEY_AUTH_REQ,
                SecurityDbConsts.KEY_DSP_AUTH_REQ,
                Boolean.toString(newPolicy)
            )
        );
        txMgr.commit();
    }

    private SecIdentitiesSpec loadIdentity(
        ControllerK8sCrdCurrentTransactionMgr tx,
        IdentityName idNameRef,
        boolean failIfNull
    )
        throws DatabaseException
    {
        return tx.getTransaction().get(
            GeneratedDatabaseTables.SEC_IDENTITIES,
            idSpec -> idNameRef.value.equals(idSpec.identityName),
            failIfNull,
            "Identity with name '" + idNameRef + "' not found in database"
        );
    }

    private SecDfltRolesSpec loadDefaultRole(
        ControllerK8sCrdCurrentTransactionMgr tx,
        IdentityName idNameRef,
        boolean failIfNull
    )
        throws DatabaseException
    {
        return tx.getTransaction().get(
            GeneratedDatabaseTables.SEC_DFLT_ROLES,
            dfltRoleSpec -> idNameRef.value.equals(dfltRoleSpec.identityName),
            failIfNull,
            "Identity with name '" + idNameRef + "' not found in database"
        );
    }

    private SecRolesSpec loadRole(
        ControllerK8sCrdCurrentTransactionMgr tx,
        String roleNameRef,
        boolean failIfNull
    )
        throws DatabaseException
    {
        return tx.getTransaction().get(
            GeneratedDatabaseTables.SEC_ROLES,
            roleSpec -> roleNameRef.equalsIgnoreCase(roleSpec.roleName),
            failIfNull,
            "Role with name '" + roleNameRef + "' not found in database"
        );
    }

    private SecIdRoleMapSpec loadIdRole(
        ControllerK8sCrdCurrentTransactionMgr tx,
        IdentityName idName,
        RoleName roleName,
        boolean failIfNull
    )
        throws DatabaseException
    {
        return tx.getTransaction().get(
            GeneratedDatabaseTables.SEC_ID_ROLE_MAP,
            idRole -> idRole.identityName.equals(idName.value) && idRole.roleName.equals(roleName.value),
            failIfNull,
            "Id " + idName.displayValue + " has no Role " + roleName.displayValue + " defined"
        );
    }

    private List<String> loadDistinctSecDspNames(
        ControllerK8sCrdCurrentTransactionMgr tx,
        Column dspNameCol
    )
    {
        List<String> ret = new ArrayList<>();

        Collection<LinstorSpec> list = tx.getTransaction().get(dspNameCol.getTable()).values();
        for (LinstorSpec linstorSpec : list)
        {
            ret.add((String) linstorSpec.getByColumn(dspNameCol));
        }
        return ret;
    }

    private String getValueOfConfigSpec(
        ControllerK8sCrdCurrentTransactionMgr tx,
        String key
    )
    {
        String value = null;
        SecConfigurationSpec spec = loadConfigSpec(tx, key);
        if (spec != null)
        {
            value = spec.entryValue;
        }
        return value;
    }

    private SecConfigurationSpec loadConfigSpec(
        ControllerK8sCrdCurrentTransactionMgr tx,
        String key
    )
    {
        SecConfigurationSpec spec = null;

        Map<String, GenCrdCurrent.SecConfigurationSpec> configurationMap = tx.getTransaction().get(
            GeneratedDatabaseTables.SEC_CONFIGURATION
        );
        for (GenCrdCurrent.SecConfigurationSpec configSpec : configurationMap.values())
        {
            if (configSpec.entryKey.equals(key))
            {
                spec = configSpec;
                break;
            }
        }
        return spec;
    }
}

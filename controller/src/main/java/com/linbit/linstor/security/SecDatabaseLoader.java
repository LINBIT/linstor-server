package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.StringConv;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ObjProtMap;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver.SecConfigDbEntry;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver.SecIdentityDbObj;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclCtrlDatabaseDriver.SecObjProtAclParent;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtCtrlDatabaseDriver.SecObjProtInitObj;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtCtrlDatabaseDriver.SecObjProtParent;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleCtrlDatabaseDriver.SecRoleInit;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleCtrlDatabaseDriver.SecRoleParent;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeCtrlDatabaseDriver.SecTypeInitObj;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeRulesCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeRulesCtrlDatabaseDriver.SecTypeRulesParent;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class SecDatabaseLoader
{
    private final SecConfigDbDriver secCfgDriver;
    private final SecObjProtAclCtrlDatabaseDriver secAclMapDriver;
    private final SecIdentityCtrlDatabaseDriver secIdentityDriver;
    private final SecObjProtCtrlDatabaseDriver secObjProtDriver;
    private final SecRoleCtrlDatabaseDriver secRoleDriver;
    private final SecTypeCtrlDatabaseDriver secTypeDriver;
    private final SecTypeRulesCtrlDatabaseDriver secTypeRuleDriver;
    private final ObjProtMap objProtMap;

    @Inject
    public SecDatabaseLoader(
        SecConfigDbDriver secCfgDriverRef,
        SecObjProtAclCtrlDatabaseDriver secAclMapDriverRef,
        SecIdentityCtrlDatabaseDriver secIdentityDriverRef,
        SecObjProtCtrlDatabaseDriver secObjProtDriverRef,
        SecRoleCtrlDatabaseDriver secRoleDriverRef,
        SecTypeCtrlDatabaseDriver secTypeDriverRef,
        SecTypeRulesCtrlDatabaseDriver secTypeRuleDriverRef,
        CoreModule.ObjProtMap objProtMapRef
    )
    {
        secCfgDriver = secCfgDriverRef;
        secAclMapDriver = secAclMapDriverRef;
        secIdentityDriver = secIdentityDriverRef;
        secObjProtDriver = secObjProtDriverRef;
        secRoleDriver = secRoleDriverRef;
        secTypeDriver = secTypeDriverRef;
        secTypeRuleDriver = secTypeRuleDriverRef;
        objProtMap = objProtMapRef;
    }

    public void loadAll() throws DatabaseException
    {
        /*
         * the following database tables do exist (for more or less good reasons), but either have a database driver
         * which does not need to be loaded or do not even have a database driver.
         * such database tables are queried on the fly during authentication for example and can therefore be ignored
         * here.
         */
        // SecAccessTypes
        // SecDfltRoles
        // SecIdRoleMap

        ArrayList<SecConfigDbEntry> loadAllAsList = secCfgDriver.loadAllAsList(null);
        for (SecConfigDbEntry secConfigDbEntry : loadAllAsList)
        {
            switch (secConfigDbEntry.key.toUpperCase())
            {
                case SecurityDbConsts.KEY_AUTH_REQ:
                    boolean authRequired = StringConv.getDfltBoolean(secConfigDbEntry.value, true);
                    Authentication.setLoadedConfig(authRequired);
                    break;
                case SecurityDbConsts.KEY_SEC_LEVEL:
                    SecurityLevel.setLoadedSecLevel(secConfigDbEntry.value);
                    break;
                default:
                    throw new ImplementationError(
                        "Unknown SecurityConfiguration entry: " + secConfigDbEntry.key + " = " + secConfigDbEntry.value
                    );
            }
        }

        Map<SecIdentityDbObj, Void> idsMap = secIdentityDriver.loadAll(null);
        Identity.ensureDefaultsExist();
        TreeMap<IdentityName, Identity> idsMapByName = new TreeMap<>();
        for (SecIdentityDbObj dbObj : idsMap.keySet())
        {
            idsMapByName.put(dbObj.getIdentity().name, dbObj.getIdentity());
        }

        Map<SecurityType, SecTypeInitObj> secTypesMap = secTypeDriver.loadAll(null);
        // SecurityType.ensureDefaultsExists is not needed here, since we have to call .setLoadedSecTypes soon which
        // additionally include the typeEnforementRules
        TreeMap<SecTypeName, SecurityType> secTypesMapByName = DatabaseLoader.mapByName(
            secTypesMap,
            secType -> secType.name
        );
        ArrayList<TypeEnforcementRulePojo> secTypeEnforcementRuleList = secTypeRuleDriver.loadAllAsList(
            new SecTypeRulesParent(secTypesMapByName)
        );
        SecurityType.setLoadedSecTypes(secTypesMap.keySet(), secTypeEnforcementRuleList);

        Map<Role, SecRoleInit> rolesMap = secRoleDriver.loadAll(new SecRoleParent(secTypesMapByName));
        Role.ensureDefaultsExist();
        TreeMap<RoleName, Role> rolesMapByName = DatabaseLoader.mapByName(rolesMap, role -> role.name);

        Map<ObjectProtection, SecObjProtInitObj> loadedObjProtMap = secObjProtDriver.loadAll(
            new SecObjProtParent(rolesMapByName, idsMapByName, secTypesMapByName)
        );
        Map<String, PairNonNull<ObjectProtection, Map<RoleName, AccessControlEntry>>> objProtMapByObjPath = new TreeMap<>();
        for (Map.Entry<ObjectProtection, SecObjProtInitObj> entry : loadedObjProtMap.entrySet())
        {
            ObjectProtection objProt = entry.getKey();
            SecObjProtInitObj init =  entry.getValue();
            objProtMapByObjPath.put(
                init.getObjPath(),
                new PairNonNull<>(objProt, init.getAclBackingMap())
            );
        }

        // return type can be ignored since the driver injects its data directly into the objProts
        secAclMapDriver.loadAll(new SecObjProtAclParent(objProtMapByObjPath, rolesMapByName));

        // register objects in corresponding maps
        for (ObjectProtection op : loadedObjProtMap.keySet())
        {
            objProtMap.put(op.getObjectProtectionPath(), op);
        }
    }
}

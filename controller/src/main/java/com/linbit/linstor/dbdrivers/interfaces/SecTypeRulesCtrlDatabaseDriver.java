package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeRulesCtrlDatabaseDriver.SecTypeRulesParent;
import com.linbit.linstor.security.SecTypeName;
import com.linbit.linstor.security.SecurityType;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;

import java.util.Map;

public interface SecTypeRulesCtrlDatabaseDriver extends SecTypeRulesDatabaseDriver,
    ControllerDatabaseDriver<TypeEnforcementRulePojo, Void, SecTypeRulesParent>
{
    class SecTypeRulesParent
    {
        private final Map<SecTypeName, SecurityType> secTypes;

        public SecTypeRulesParent(Map<SecTypeName, SecurityType> secTypesRef)
        {
            secTypes = secTypesRef;
        }

        public SecurityType getSecType(SecTypeName secTypeNameRef)
        {
            return secTypes.get(secTypeNameRef);
        }
    }
}

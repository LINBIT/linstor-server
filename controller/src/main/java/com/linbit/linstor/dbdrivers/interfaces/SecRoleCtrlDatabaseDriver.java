package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleCtrlDatabaseDriver.SecRoleInit;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleCtrlDatabaseDriver.SecRoleParent;
import com.linbit.linstor.security.Role;
import com.linbit.linstor.security.SecTypeName;
import com.linbit.linstor.security.SecurityType;

import java.util.Map;

public interface SecRoleCtrlDatabaseDriver extends SecRoleDatabaseDriver,
    ControllerDatabaseDriver<Role, SecRoleInit, SecRoleParent>
{
    class SecRoleInit
    {
        public final boolean enabled;
        public final SecurityType domain;

        public SecRoleInit(boolean enabledRef, SecurityType domainRef)
        {
            enabled = enabledRef;
            domain = domainRef;
        }
    }

    class SecRoleParent
    {
        private final Map<SecTypeName, SecurityType> secTypeMap;

        public SecRoleParent(Map<SecTypeName, SecurityType> secTypeMapRef)
        {
            secTypeMap = secTypeMapRef;
        }

        public SecurityType getSecType(SecTypeName secTypeNameRef)
        {
            return secTypeMap.get(secTypeNameRef);
        }

    }
}

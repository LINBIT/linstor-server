package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclCtrlDatabaseDriver.SecObjProtAclParent;
import com.linbit.linstor.security.AccessControlEntry;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.Role;
import com.linbit.linstor.security.RoleName;
import com.linbit.utils.PairNonNull;

import java.util.Map;

public interface SecObjProtAclCtrlDatabaseDriver extends SecObjProtAclDatabaseDriver,
    ControllerDatabaseDriver<AccessControlEntry, Void, SecObjProtAclParent>
{
    class SecObjProtAclParent
    {
        private final Map<String, PairNonNull<ObjectProtection, Map<RoleName, AccessControlEntry>>> objProtMap;
        private final Map<RoleName, Role> roles;

        public SecObjProtAclParent(
            Map<String, PairNonNull<ObjectProtection, Map<RoleName, AccessControlEntry>>> objProtMapRef,
            Map<RoleName, Role> rolesRef
        )
        {
            objProtMap = objProtMapRef;
            roles = rolesRef;
        }

        public Role getRole(RoleName roleNameRef)
        {
            return roles.get(roleNameRef);
        }

        public Map<RoleName, AccessControlEntry> getParentAcl(String objPathRef)
        {
            @Nullable PairNonNull<ObjectProtection, Map<RoleName, AccessControlEntry>> objProtMapRef = objProtMap.get(
                objPathRef
            );
            if (objProtMapRef == null)
            {
                throw new ImplementationError(
                    "Object path " + objPathRef + " does not have an entry in the objProtMap"
                );
            }
            return objProtMapRef.objB;
        }
    }
}

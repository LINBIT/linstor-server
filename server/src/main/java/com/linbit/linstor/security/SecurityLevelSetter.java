package com.linbit.linstor.security;

import java.sql.SQLException;

public interface SecurityLevelSetter
{
    void setSecurityLevel(AccessContext accCtx, SecurityLevel newLevel)
        throws AccessDeniedException, SQLException;
}

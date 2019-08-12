package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;

public interface SecurityLevelSetter
{
    void setSecurityLevel(AccessContext accCtx, SecurityLevel newLevel)
        throws AccessDeniedException, DatabaseException;
}

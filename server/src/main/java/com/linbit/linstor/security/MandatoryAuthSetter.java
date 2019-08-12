
package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;

public interface MandatoryAuthSetter
{
    void setAuthRequired(AccessContext accCtx, boolean newPolicy)
        throws AccessDeniedException, DatabaseException;
}

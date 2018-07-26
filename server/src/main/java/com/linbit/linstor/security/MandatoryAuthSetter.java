
package com.linbit.linstor.security;

import java.sql.SQLException;

public interface MandatoryAuthSetter
{
    void setAuthRequired(AccessContext accCtx, boolean newPolicy)
        throws AccessDeniedException, SQLException;
}

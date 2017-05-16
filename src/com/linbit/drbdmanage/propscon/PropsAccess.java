package com.linbit.drbdmanage.propscon;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;

/**
 * Manages access to properties containers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class PropsAccess
{
    public static Props secureGetProps(AccessContext accCtx, ObjectProtection objProt, Props propsRef)
        throws AccessDeniedException
    {
        // Always require at least VIEW access
        objProt.requireAccess(accCtx, AccessType.VIEW);

        // If CHANGE or CONTROL access is permitted, return a modifiable instance of the
        // properties container, otherwise wrap the properties container in a read-only
        // container and return this read-only container instead
        Props securedProps;
        AccessType allowedAccess = objProt.queryAccess(accCtx);
        if (allowedAccess.hasAccess(AccessType.CHANGE))
        {
            securedProps = propsRef;
        }
        else
        {
            securedProps = new ReadOnlyProps(propsRef);
        }
        return securedProps;
    }
}

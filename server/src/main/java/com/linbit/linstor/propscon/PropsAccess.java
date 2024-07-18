package com.linbit.linstor.propscon;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

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
            securedProps = new ReadOnlyPropsImpl(propsRef);
        }
        return securedProps;
    }

    private PropsAccess()
    {
    }

    public static Props secureGetProps(
        AccessContext accCtx,
        ObjectProtection objProt1,
        ObjectProtection objProt2,
        Props propsRef
    )
        throws AccessDeniedException
    {
        // Always require at least VIEW access
        objProt1.requireAccess(accCtx, AccessType.VIEW);
        objProt2.requireAccess(accCtx, AccessType.VIEW);

        Props securedProps;
        if (propsRef instanceof ReadOnlyPropsImpl)
        {
            securedProps = propsRef;
        }
        else
        {
            // If CHANGE or CONTROL access is permitted, return a modifiable instance of the
            // properties container, otherwise wrap the properties container in a read-only
            // container and return this read-only container instead
            AccessType allowedAccess1 = objProt1.queryAccess(accCtx);
            AccessType allowedAccess2 = objProt2.queryAccess(accCtx);
            if (allowedAccess1.hasAccess(AccessType.CHANGE) &&
                allowedAccess2.hasAccess(AccessType.CHANGE))
            {
                securedProps = propsRef;
            }
            else
            {
                securedProps = new ReadOnlyPropsImpl(propsRef);
            }
        }
        return securedProps;
    }
}

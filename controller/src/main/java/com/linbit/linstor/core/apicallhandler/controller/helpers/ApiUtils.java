package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ImplementationError;
import com.linbit.linstor.security.AccessDeniedException;

public class ApiUtils
{
    public interface AccessCheckedRunnable<T>
    {
        T execPriveleged() throws AccessDeniedException;
    }

    public static <T> T execPriveleged(AccessCheckedRunnable<T> runnable)
    {
        T ret;
        try
        {
            ret = runnable.execPriveleged();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return ret;
    }
}

package com.linbit.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.security.AccessDeniedException;

public class AccessUtils
{
    public static <T> T execPrivileged(ExceptionThrowingSupplier<T, AccessDeniedException> supplier)
        throws ImplementationError
    {
        return execPrivileged(supplier, "Privileged context has not enough privileges");
    }

    public static <T> T execPrivileged(ExceptionThrowingSupplier<T, AccessDeniedException> supplier, String excMessage)
        throws ImplementationError
    {
        T genericReturnVariableNameLongerThanTwoCharacters;
        try
        {
            genericReturnVariableNameLongerThanTwoCharacters = supplier.supply();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(excMessage);
        }
        return genericReturnVariableNameLongerThanTwoCharacters;
    }

    public static void execPrivileged(ExceptionThrowingRunnable<AccessDeniedException> runner)
        throws ImplementationError
    {
        execPrivileged(runner, "Privileged context has not enough privileges");
    }

    public static void execPrivileged(ExceptionThrowingRunnable<AccessDeniedException> runner, String excMessage)
        throws ImplementationError
    {
        try
        {
            runner.run();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(excMessage);
        }
    }

    private AccessUtils()
    {
    }
}

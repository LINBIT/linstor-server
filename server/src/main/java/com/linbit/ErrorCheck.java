package com.linbit;

import com.linbit.linstor.annotation.Nullable;

/**
 * Implementation error checks
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ErrorCheck
{
    // Do not instantiate this class
    private ErrorCheck()
    {
    }

    public static final void ctorNotNull(Class<?> objClass, Class<?> argClass, @Nullable Object param)
    {
        if (param == null)
        {
            throw new ImplementationError(
                String.format(
                    "Attempt to create an instance of class %s with a null %s reference",
                    objClass.getCanonicalName(), argClass.getCanonicalName()
                ),
                new NullPointerException()
            );
        }
    }

    public static <ARG> ARG requireNonNull(Class<?> objClass, Class<ARG> argClass, ARG arg)
    {
        ctorNotNull(objClass, argClass, arg);
        return arg;
    }
}

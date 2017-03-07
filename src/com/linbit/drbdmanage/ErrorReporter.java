package com.linbit.drbdmanage;

/**
 * Generates / formats error reports
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ErrorReporter
{
    void reportError(Throwable errorInfo);
}

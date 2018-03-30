package com.linbit.linstor.debug;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

import com.linbit.linstor.security.AccessContext;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CommonDebugCmd
{
    Set<String> getCmdNames();

    String getDisplayName(String upperCaseCmdName);

    String getCmdInfo();

    String getCmdDescription();

    Map<String, String> getParametersDescription();

    String getUndeclaredParametersDescription();

    boolean acceptsUndeclaredParameters();

    boolean requiresScope();

    void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception;
}

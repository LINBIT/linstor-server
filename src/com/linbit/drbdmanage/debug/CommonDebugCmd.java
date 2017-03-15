package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

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

    void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception;
}

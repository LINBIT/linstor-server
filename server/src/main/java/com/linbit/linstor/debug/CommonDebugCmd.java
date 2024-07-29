package com.linbit.linstor.debug;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.security.AccessContext;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CommonDebugCmd
{
    Set<String> getCmdNames();

    @Nullable
    String getDisplayName(String upperCaseCmdName);

    String getCmdInfo();

    String getCmdDescription();

    @Nullable
    Map<String, String> getParametersDescription();

    @Nullable
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

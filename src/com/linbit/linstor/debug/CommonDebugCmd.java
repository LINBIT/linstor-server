package com.linbit.linstor.debug;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

import com.linbit.linstor.CommonDebugControl;
import com.linbit.linstor.CoreServices;
import com.linbit.linstor.core.LinStor;
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

    void commonInitialize(
        CoreServices coreSvcsRef,
        CommonDebugControl debugCtlRef,
        DebugConsole debugConRef
    );

    void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception;
}

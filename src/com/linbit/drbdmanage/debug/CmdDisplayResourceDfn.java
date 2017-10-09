package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

public class CmdDisplayResourceDfn extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_RSC_DFN_NAME = "NAME";
    private static final String PRM_FILTER_NAME = "MATCH-NAME";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_RSC_DFN_NAME,
            "Name of the resource definition(s) to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_FILTER_NAME,
            "Filter pattern to apply to the resource definition name.\n" +
            "Resources with a name matching the pattern will be displayed."
        );
    }

    public CmdDisplayResourceDfn()
    {
        super(
            new String[]
            {
                "DspRscDfn"
            },
            "Display resource definition(s)",
            "Displays information about one or multiple resource definitions",
            PARAMETER_DESCRIPTIONS,
            null,
            false
        );
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    )
        throws Exception
    {
        Map<ResourceName, ResourceDefinition> rscDfnMap = cmnDebugCtl.getRscDfnMap();
        printError(debugErr, "This command is not implemented yet.", null, null, null);
    }
}

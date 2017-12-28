package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class CmdDisplayResource extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_RSCDFN_NAME = "NAME";
    private static final String PRM_FILTER_NAME = "MATCHNAME";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_RSCDFN_NAME,
            "Name of the resource(s) to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_FILTER_NAME,
            "Filter pattern to apply to the resource name.\n" +
            "Resources with a name matching the pattern will be displayed."
        );
    }

    public CmdDisplayResource()
    {
        super(
            new String[]
            {
                "DspRsc"
            },
            "Display resource(s)",
            "Displays information about one or multiple resource(s)",
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
        String prmName = parameters.get(PRM_RSCDFN_NAME);
        String prmFilter = parameters.get(PRM_FILTER_NAME);

        final Map<ResourceName, ResourceDefinition> rscDfnMap = cmnDebugCtl.getRscDfnMap();
        final Lock sysReadLock = cmnDebugCtl.getReconfigurationLock().readLock();
        final Lock nodesMapReadLock = cmnDebugCtl.getNodesMapLock().readLock();
        final Lock rscDfnMapReadLock = cmnDebugCtl.getRscDfnMapLock().readLock();

        try
        {
            if (prmName != null)
            {
                if (prmFilter == null)
                {
                    ResourceName prmResName = new ResourceName(prmName);
                    try
                    {
                        sysReadLock.lock();
                        nodesMapReadLock.lock();
                        rscDfnMapReadLock.lock();

                        {
                            ObjectProtection rscDfnMapProt = cmnDebugCtl.getRscDfnMapProt();
                            if (rscDfnMapProt != null)
                            {
                                rscDfnMapProt.requireAccess(accCtx, AccessType.VIEW);
                            }
                        }

                        ResourceDefinition rscDfn = rscDfnMap.get(prmResName);
                        if (rscDfn != null)
                        {
                            if (rscDfn.getResourceCount() >= 1)
                            {
                                printSectionSeparator(debugOut);
                                displayRsc(debugOut, rscDfn, accCtx);
                                printSectionSeparator(debugOut);
                            }
                        }
                        else
                        {
                            debugOut.printf("The resource definition '%s' does not exist\n", prmName);
                        }
                    }
                    finally
                    {
                        rscDfnMapReadLock.unlock();
                        nodesMapReadLock.unlock();
                        sysReadLock.unlock();
                    }
                }
                else
                {
                    printError(
                        debugErr,
                        "The command line contains conflicting parameters",
                        "The parameters " + PRM_RSCDFN_NAME + " and " + PRM_FILTER_NAME + " were combined " +
                        "in the command line.\n" +
                        "Combining the two parameters is not supported.",
                        "Specify either the " + PRM_RSCDFN_NAME + " parameter to display information " +
                        "about a single resource definition, or specify the " + PRM_FILTER_NAME +
                        " parameter to display information about all resource definitions " +
                        "that have a name matching the specified filter.",
                        null
                    );
                }
            }
            else
            {
                // Filter matching nodes
                int count = 0;
                int total = 0;
                try
                {
                    sysReadLock.lock();
                    nodesMapReadLock.lock();
                    rscDfnMapReadLock.lock();

                    total = rscDfnMap.size();

                    {
                        ObjectProtection rscDfnMapProt = cmnDebugCtl.getRscDfnMapProt();
                        if (rscDfnMapProt != null)
                        {
                            rscDfnMapProt.requireAccess(accCtx, AccessType.VIEW);
                        }
                    }

                    Matcher nameMatcher = null;
                    if (prmFilter != null)
                    {
                        Pattern namePattern = Pattern.compile(prmFilter, Pattern.CASE_INSENSITIVE);
                        nameMatcher = namePattern.matcher("");
                    }

                    Iterator<ResourceDefinition> rscDfnIter = rscDfnMap.values().iterator();
                    if (nameMatcher == null)
                    {
                        while (rscDfnIter.hasNext())
                        {
                            if (count == 0)
                            {
                                printSectionSeparator(debugOut);
                            }
                            ResourceDefinition rscDfn = rscDfnIter.next();
                            if (rscDfn.getResourceCount() >= 1)
                            {
                                count += displayRsc(debugOut, rscDfn, accCtx);
                            }
                        }
                    }
                    else
                    {
                        while (rscDfnIter.hasNext())
                        {
                            ResourceDefinition rscDfn = rscDfnIter.next();
                            ResourceName name = rscDfn.getName();
                            nameMatcher.reset(name.value);
                            if (nameMatcher.find())
                            {
                                if (count == 0)
                                {
                                    printSectionSeparator(debugOut);
                                }
                                if (rscDfn.getResourceCount() >= 1)
                                {
                                    count += displayRsc(debugOut, rscDfn, accCtx);
                                }
                            }
                        }
                    }
                }
                finally
                {
                    rscDfnMapReadLock.unlock();
                    nodesMapReadLock.unlock();
                    sysReadLock.unlock();
                }

                String totalFormat;
                if (total == 1)
                {
                    totalFormat = "%d resource entry\n";
                }
                else
                {
                    totalFormat = "%d resource entries\n";
                }

                if (count > 0)
                {
                    printSectionSeparator(debugOut);
                    if (prmFilter == null)
                    {
                        debugOut.printf(totalFormat, total);
                    }
                    else
                    {
                        String countFormat;
                        if (count == 1)
                        {
                            countFormat = "%d resource entry was selected by the filter\n";
                        }
                        else
                        {
                            countFormat = "%d resource entries were selected by the filter\n";
                        }
                        debugOut.printf(countFormat, count);
                        debugOut.printf(totalFormat, total);
                    }
                }
                else
                {
                    if (total == 0)
                    {
                        debugOut.println("No resource entries to display");
                    }
                    else
                    {

                        debugOut.println("No matching resource entries");
                        debugOut.printf(totalFormat, total);
                    }
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            printDmException(debugErr, accExc);
        }
        catch (InvalidNameException nameExc)
        {
            printError(
                debugErr,
                "The value specified for the parameter " + PRM_RSCDFN_NAME + " is not a valid " +
                "resource definition name",
                null,
                "Specify a valid resource definition name to display information about the " +
                "resources associated with a single resource definition, or set a filter pattern " +
                "using the " + PRM_FILTER_NAME + " parameter to display information about all resources " +
                "that have a name that matches the pattern.",
                String.format(
                    "The specified value was '%s'.",
                    prmName
                )
            );
        }
        catch (PatternSyntaxException patternExc)
        {
            printError(
                debugOut,
                "The regular expression specified for the parameter " + PRM_FILTER_NAME + " is not valid.",
                patternExc.getMessage(),
                "Reenter the command using the correct regular expression syntax for the " + PRM_FILTER_NAME +
                " parameter.",
                null
            );
        }
    }

    private int displayRsc(PrintStream output, ResourceDefinition rscDfn, AccessContext accCtx)
    {
        int dspCount = 0;
        ResourceName rscName = rscDfn.getName();
        try
        {
            Iterator<Resource> rscIter = rscDfn.iterateResource(accCtx);
            output.printf(
                "\u001b[1;37m%-48s\u001b[0m %s\n",
                rscName.displayValue, rscDfn.getUuid().toString().toUpperCase()
            );
            StringBuilder outText = new StringBuilder();
            while (rscIter.hasNext())
            {
                Resource rsc = rscIter.next();
                ObjectProtection rscProt = rsc.getObjProt();
                NodeName peerNodeName = rsc.getAssignedNode().getName();

                String pfxIndent;
                if (rscIter.hasNext())
                {
                    outText.append(PFX_SUB);
                    pfxIndent = PFX_VLINE + "  ";
                }
                else
                {
                    outText.append(PFX_SUB_LAST);
                    pfxIndent = "    ";
                }
                outText.append(peerNodeName.displayValue).append("\n");
                outText.append(pfxIndent).append(PFX_SUB).append("  UUID: ");
                outText.append(rsc.getUuid().toString().toUpperCase()).append("\n");
                outText.append(pfxIndent).append(PFX_SUB).append("  Node-ID: ");
                outText.append(Integer.toString(rsc.getNodeId().value)).append("\n");
                try
                {
                    long flagsBits = rsc.getStateFlags().getFlagsBits(accCtx);
                    outText.append(pfxIndent).append(PFX_SUB).append("  Flags: ");
                    outText.append(
                        String.format("%016X\n", flagsBits)
                    );
                }
                catch (AccessDeniedException ignored)
                {
                }
                outText.append(pfxIndent).append(PFX_SUB).append("  Creator: ");
                outText.append(
                    String.format(
                        "%-24s Owner: %-24s\n",
                        rscProt.getCreator().name.displayValue,
                        rscProt.getOwner().name.displayValue
                    )
                );
                outText.append(pfxIndent).append(PFX_SUB_LAST).append("  Security type: ");
                outText.append(rscProt.getSecurityType().name.displayValue).append("\n");
                output.append(outText.toString());
                outText.setLength(0);
                ++dspCount;
            }
        }
        catch (AccessDeniedException ignored)
        {
            // Not authorized for the resource definition
        }
        return dspCount;
    }
}

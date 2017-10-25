package com.linbit.drbdmanage.debug;

import com.linbit.AutoIndent;
import com.linbit.InvalidNameException;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class CmdDisplayResourceDfn extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_RSCDFN_NAME = "NAME";
    private static final String PRM_FILTER_NAME = "MATCHNAME";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_RSCDFN_NAME,
            "Name of the resource definition(s) to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_FILTER_NAME,
            "Filter pattern to apply to the resource definition name.\n" +
            "Resource definitions with a name matching the pattern will be displayed."
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
            "Displays information about one or multiple resource definition(s)",
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
                        rscDfnMapReadLock.lock();

                        {
                            ObjectProtection rscDfnMapProt = cmnDebugCtl.getRscDfnMapProt();
                            if (rscDfnMapProt != null)
                            {
                                rscDfnMapProt.requireAccess(accCtx, AccessType.VIEW);
                            }
                        }

                        ResourceDefinition rscDfnRef = rscDfnMap.get(prmResName);
                        if (rscDfnRef != null)
                        {
                            printSectionSeparator(debugOut);
                            displayRscDfn(debugOut, rscDfnRef, accCtx);
                            printSectionSeparator(debugOut);
                        }
                        else
                        {
                            debugOut.printf("The resource definition '%s' does not exist\n", prmName);
                        }
                    }
                    finally
                    {
                        rscDfnMapReadLock.unlock();
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
                            displayRscDfn(debugOut, rscDfnIter.next(), accCtx);
                            ++count;
                        }
                    }
                    else
                    {
                        while (rscDfnIter.hasNext())
                        {
                            ResourceDefinition rscDfnRef = rscDfnIter.next();
                            ResourceName name = rscDfnRef.getName();
                            nameMatcher.reset(name.value);
                            if (nameMatcher.find())
                            {
                                if (count == 0)
                                {
                                    printSectionSeparator(debugOut);
                                }
                                displayRscDfn(debugOut, rscDfnRef, accCtx);
                                ++count;
                            }
                        }
                    }
                }
                finally
                {
                    rscDfnMapReadLock.unlock();
                    sysReadLock.unlock();
                }

                String totalFormat;
                if (total == 1)
                {
                    totalFormat = "%d resource definition entry is registered in the database\n";
                }
                else
                {
                    totalFormat = "%d resource definition entries are registered in the database\n";
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
                            countFormat = "%d resource definition entry was selected by the filter\n";
                        }
                        else
                        {
                            countFormat = "%d resource definition entries were selected by the filter\n";
                        }
                        debugOut.printf(countFormat, count);
                        debugOut.printf(totalFormat, total);
                    }
                }
                else
                {
                    if (total == 0)
                    {
                        debugOut.println("The database contains no resource definition entries");
                    }
                    else
                    {

                        debugOut.println("No matching resource definition entries were found");
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
                "Specify a valid resource definition name to display information about a " +
                "single resource definition, or set a filter pattern " +
                "using the " + PRM_FILTER_NAME + " parameter to display information about all resource definitions " +
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

    private void displayRscDfn(PrintStream output, ResourceDefinition rscDfnRef, AccessContext accCtx)
    {
        try
        {
            ObjectProtection objProt = rscDfnRef.getObjProt();
            objProt.requireAccess(accCtx, AccessType.VIEW);
            output.printf(
                "\u001b[1;37m%-40s\u001b[0m %-36s\n" +
                "%s  Flags: %016x\n" +
                "%s  Creator: %-24s Owner: %-24s\n" +
                "%s  Security type: %-24s\n",
                rscDfnRef.getName().displayValue,
                rscDfnRef.getUuid().toString().toUpperCase(),
                PFX_SUB, rscDfnRef.getFlags().getFlagsBits(accCtx),
                PFX_SUB, objProt.getCreator().name.displayValue,
                objProt.getOwner().name.displayValue,
                PFX_SUB_LAST, objProt.getSecurityType().name.displayValue
            );
            Iterator<VolumeDefinition> vlmDfnIter = rscDfnRef.iterateVolumeDfn(accCtx);
            if (vlmDfnIter.hasNext())
            {
                output.println("    Volume definitions");
            }
            while (vlmDfnIter.hasNext())
            {
                VolumeDefinition vlmDfnRef = vlmDfnIter.next();
                String itemPfx;
                String treePfx;
                if (vlmDfnIter.hasNext())
                {
                    itemPfx = PFX_SUB;
                    treePfx = PFX_VLINE;
                }
                else
                {
                    itemPfx = PFX_SUB_LAST;
                    treePfx = "  ";
                }
                AutoIndent.printWithIndent(
                    output, 4,
                    String.format(
                        "%s  \u001b[1;37mVolume %6d\u001b[0m %-36s\n" +
                        "%s  %s  Size:     %16d\n" +
                        "%s  %s  Minor Nr: %16d\n" +
                        "%s  %s  Flags:    %016x\n",
                        itemPfx, vlmDfnRef.getVolumeNumber(accCtx).value,
                        vlmDfnRef.getUuid().toString().toUpperCase(),
                        treePfx, PFX_SUB, vlmDfnRef.getVolumeSize(accCtx),
                        treePfx, PFX_SUB, vlmDfnRef.getMinorNr(accCtx).value,
                        treePfx, PFX_SUB_LAST, vlmDfnRef.getFlags().getFlagsBits(accCtx)
                    )
                );
            }
        }
        catch (AccessDeniedException accExc)
        {
            // No view access
        }
    }
}

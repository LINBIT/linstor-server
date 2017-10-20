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
                if (count > 0)
                {
                    printSectionSeparator(debugOut);
                    String format;
                    if (count == 1)
                    {
                        format = "%d resource definition\n";
                    }
                    else
                    {
                        format = "%d resource definitions\n";
                    }
                    debugOut.printf(format, count);
                }
                else
                {
                    if (prmFilter == null)
                    {
                        debugOut.println("The list of registered resource definitions is empty");
                    }
                    else
                    {
                        debugOut.println("No matching resource definition entries were found");
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
                "%-40s %-36s\n" +
                "    Flags: %016x\n" +
                "    Creator: %-24s Owner: %-24s\n" +
                "    Security type: %-24s\n",
                rscDfnRef.getName().displayValue,
                rscDfnRef.getUuid().toString().toUpperCase(),
                rscDfnRef.getFlags().getFlagsBits(accCtx),
                objProt.getCreator().name.displayValue,
                objProt.getOwner().name.displayValue,
                objProt.getSecurityType().name.displayValue
            );
            Iterator<VolumeDefinition> vlmDfnIter = rscDfnRef.iterateVolumeDfn(accCtx);
            while (vlmDfnIter.hasNext())
            {
                VolumeDefinition vlmDfnRef = vlmDfnIter.next();
                AutoIndent.printWithIndent(
                    output, 4,
                    String.format(
                        "Volume %6d %-36s\n" +
                        "    Flags: %016x " +
                        "    Size: %13d\n",
                        vlmDfnRef.getVolumeNumber(accCtx).value,
                        vlmDfnRef.getUuid().toString().toUpperCase(),
                        vlmDfnRef.getVolumeSize(accCtx),
                        vlmDfnRef.getFlags().getFlagsBits(accCtx)
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

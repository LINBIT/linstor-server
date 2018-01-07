package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.UuidUtils;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class CmdDisplayNodes extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_NODE_NAME = "NAME";
    private static final String PRM_FILTER_NAME = "MATCHNAME";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_NODE_NAME,
            "Name of the node(s) to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_FILTER_NAME,
            "Filter pattern to apply to the node name.\n" +
            "Nodes with a name matching the pattern will be displayed."
        );
    }

    public CmdDisplayNodes()
    {
        super(
            new String[]
            {
                "DspNode"
            },
            "Display node(s)",
            "Displays information about one or multiple nodes",
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
        String prmName = parameters.get(PRM_NODE_NAME);
        String prmFilter = parameters.get(PRM_FILTER_NAME);

        final Map<NodeName, Node> nodesMap = cmnDebugCtl.getNodesMap();
        final Lock sysReadLock = cmnDebugCtl.getReconfigurationLock().readLock();
        final Lock nodesReadLock = cmnDebugCtl.getNodesMapLock().readLock();

        try
        {
            if (prmName != null)
            {
                if (prmFilter == null)
                {
                    NodeName prmNodeName = new NodeName(prmName);
                    try
                    {
                        sysReadLock.lock();
                        nodesReadLock.lock();

                        {
                            ObjectProtection nodesMapProt = cmnDebugCtl.getNodesMapProt();
                            if (nodesMapProt != null)
                            {
                                nodesMapProt.requireAccess(accCtx, AccessType.VIEW);
                            }
                        }

                        Node nodeRef = nodesMap.get(prmNodeName);
                        if (nodeRef != null)
                        {
                            printSectionSeparator(debugOut);
                            displayNode(debugOut, nodeRef, accCtx);
                            printSectionSeparator(debugOut);
                        }
                        else
                        {
                            debugOut.printf("The node '%s' does not exist\n", prmName);
                        }
                    }
                    finally
                    {
                        nodesReadLock.unlock();
                        sysReadLock.unlock();
                    }
                }
                else
                {
                    printError(
                        debugErr,
                        "The command line contains conflicting parameters",
                        "The parameters " + PRM_NODE_NAME + " and " + PRM_FILTER_NAME + " were combined " +
                        "in the command line.\n" +
                        "Combining the two parameters is not supported.",
                        "Specify either the " + PRM_NODE_NAME + " parameter to display information " +
                        "about a single node, or specify the " + PRM_FILTER_NAME + " parameter to display " +
                        "information about all nodes that have a name matching the specified filter.",
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
                    nodesReadLock.lock();

                    total = nodesMap.size();

                    {
                        ObjectProtection nodesMapProt = cmnDebugCtl.getNodesMapProt();
                        if (nodesMapProt != null)
                        {
                            nodesMapProt.requireAccess(accCtx, AccessType.VIEW);
                        }
                    }

                    Matcher nameMatcher = null;
                    if (prmFilter != null)
                    {
                        Pattern namePattern = Pattern.compile(prmFilter, Pattern.CASE_INSENSITIVE);
                        nameMatcher = namePattern.matcher("");
                    }

                    Iterator<Node> nodeIter = nodesMap.values().iterator();
                    if (nameMatcher == null)
                    {
                        while (nodeIter.hasNext())
                        {
                            if (count == 0)
                            {
                                printSectionSeparator(debugOut);
                            }
                            displayNode(debugOut, nodeIter.next(), accCtx);
                            ++count;
                        }
                    }
                    else
                    {
                        while (nodeIter.hasNext())
                        {
                            Node nodeRef = nodeIter.next();
                            NodeName name = nodeRef.getName();
                            nameMatcher.reset(name.value);
                            if (nameMatcher.find())
                            {
                                if (count == 0)
                                {
                                    printSectionSeparator(debugOut);
                                }
                                displayNode(debugOut, nodeRef, accCtx);
                                ++count;
                            }
                        }
                    }
                }
                finally
                {
                    nodesReadLock.unlock();
                    sysReadLock.unlock();
                }

                String totalFormat;
                if (total == 1)
                {
                    totalFormat = "%d node entry is registered in the database\n";
                }
                else
                {
                    totalFormat = "%d node entries are registered in the database\n";
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
                            countFormat = "%d node entry was selected by the filter\n";
                        }
                        else
                        {
                            countFormat = "%d node entries were selected by the filter\n";
                        }
                        debugOut.printf(countFormat, count);
                        debugOut.printf(totalFormat, total);
                    }
                }
                else
                {
                    if (total == 0)
                    {
                        debugOut.println("The database contains no node entries");
                    }
                    else
                    {

                        debugOut.println("No matching node entries were found");
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
                "The value specified for the parameter " + PRM_NODE_NAME + " is not a valid node name",
                null,
                "Specify a valid node name to display information about a single node, or set a filter pattern " +
                "using the " + PRM_FILTER_NAME + " parameter to display information about all nodes that have a " +
                "name that matches the pattern.",
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

    private void displayNode(PrintStream output, Node nodeRef, AccessContext accCtx)
    {
        try
        {
            ObjectProtection objProt = nodeRef.getObjProt();
            objProt.requireAccess(accCtx, AccessType.VIEW);
            Iterator<NetInterface> netIfIter = nodeRef.iterateNetInterfaces(accCtx);
            boolean hasNetIf = netIfIter.hasNext();
            output.printf(
                "\u001b[1;37m%-40s\u001b[0m %-36s\n" +
                "%s  Volatile UUID: %s\n" +
                "%s  Flags: %016x\n" +
                "%s  Creator: %-24s Owner: %-24s\n" +
                "%s  Security type: %-24s\n",
                nodeRef.getName().displayValue,
                nodeRef.getUuid().toString().toUpperCase(),
                PFX_SUB, UuidUtils.dbgInstanceIdString(nodeRef),
                PFX_SUB, nodeRef.getFlags().getFlagsBits(accCtx),
                PFX_SUB, objProt.getCreator().name.displayValue,
                objProt.getOwner().name.displayValue,
                hasNetIf ? PFX_SUB : PFX_SUB_LAST, objProt.getSecurityType().name.displayValue
            );
            if (hasNetIf)
            {
                output.println(PFX_SUB_LAST + "  Network interfaces:");
            }
            while (netIfIter.hasNext())
            {
                NetInterface netIf = netIfIter.next();
                String address = "<No authorized>";
                try
                {
                    LsIpAddress lsIp = netIf.getAddress(accCtx);
                    if (lsIp != null)
                    {
                        String addrStr = lsIp.getAddress();
                        if (addrStr != null)
                        {
                            address = addrStr;
                        }
                    }
                }
                catch (AccessDeniedException ignored)
                {
                }
                boolean moreNetIfs = netIfIter.hasNext();
                output.printf(
                    "    %s  \u001b[1;37m%-24s\u001b[0m %s\n" +
                    "    %s  %s  Address: %s\n",
                    moreNetIfs ? PFX_SUB : PFX_SUB_LAST,
                    netIf.getName().displayValue, netIf.getUuid().toString().toUpperCase(),
                    moreNetIfs ? PFX_VLINE : "  ", PFX_SUB_LAST, address
                );
            }
        }
        catch (AccessDeniedException accExc)
        {
            // No view access
        }
    }
}

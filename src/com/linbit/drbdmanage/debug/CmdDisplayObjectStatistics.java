package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.Identity;
import com.linbit.drbdmanage.security.Role;
import com.linbit.drbdmanage.security.SecurityType;
import java.io.PrintStream;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

public class CmdDisplayObjectStatistics extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_DETAIL_NAME = "DETAIL";
    private static final String PRM_SECURITY    = "SECURITY";
    private static final String PRM_CONFIG      = "CONFIG";
    private static final String PRM_STOROBJ     = "STOROBJ";
    private static final String PRM_ALL         = "ALL";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_DETAIL_NAME,
            "Selects the detail categories to display\n" +
            "    SECURITY\n" +
            "        Display security objects count\n" +
            "    CONFIG\n" +
            "        Display controller configuration entries count\n" +
            "    STOROBJ\n" +
            "        Display managed storage management objects count\n" +
            "        (registered nodes, resource definitions, etc.)\n" +
            "    ALL\n" +
            "        Selects all categories"
        );
    }

    public CmdDisplayObjectStatistics()
    {
        super(
            new String[]
            {
                "DspObjSts"
            },
            "Display object statistics",
            "Displays statistics about registered objects",
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
        Lock rcfgRdLock = cmnDebugCtl.getReconfigurationLock().readLock();
        rcfgRdLock.lock();
        try
        {
            boolean catSecurity;
            boolean catConfig;
            boolean catStorObj;
            String prmDetail = parameters.get(PRM_DETAIL_NAME);
            if (prmDetail == null)
            {
                catSecurity = true;
                catConfig   = true;
                catStorObj  = true;
            }
            else
            {
                catSecurity = false;
                catConfig   = false;
                catStorObj  = false;

                prmDetail = prmDetail.toUpperCase();

                InvalidDetailsException detailsExc = null;
                StringTokenizer prmDetailTokens = new StringTokenizer(prmDetail, ",");
                while (prmDetailTokens.hasMoreTokens())
                {
                    String curToken = prmDetailTokens.nextToken().trim();
                    switch (curToken)
                    {
                        case PRM_SECURITY:
                            catSecurity = true;
                            break;
                        case PRM_CONFIG:
                            catConfig = true;
                            break;
                        case PRM_STOROBJ:
                            catStorObj = true;
                            break;
                        case PRM_ALL:
                            catSecurity = true;
                            catConfig   = true;
                            catStorObj  = true;
                            break;
                        default:
                            if (detailsExc == null)
                            {
                                detailsExc = new InvalidDetailsException();
                            }
                            detailsExc.addInvalid(curToken);
                            break;
                    }
                }
                if (detailsExc != null)
                {
                    throw detailsExc;
                }
            }

            if (catSecurity || catConfig || catStorObj)
            {
                debugOut.println("Object counts");
                printSectionSeparator(debugOut);

                if (catSecurity)
                {
                    debugOut.println("Security objects");
                    debugOut.printf(
                        "    Identities:             %5d\n" +
                        "    Roles:                  %5d\n" +
                        "    Types:                  %5d\n" +
                        "    Type rules:             %5d\n",
                        Identity.getIdentityCount(),
                        Role.getRoleCount(),
                        SecurityType.getTypeCount(),
                        SecurityType.getRuleCount()
                    );
                }

                if (catConfig)
                {
                    Lock confRdLock = cmnDebugCtl.getConfLock().readLock();
                    int count = 0;
                    try
                    {
                        confRdLock.lock();
                        Props conf = cmnDebugCtl.getConf();
                        count = conf.size();
                    }
                    finally
                    {
                        confRdLock.unlock();
                    }
                    debugOut.println("Configuration entries");
                    debugOut.printf(
                        "    Entries:                %5d\n",
                        count
                    );
                }

                if (catStorObj)
                {
                    int storPoolDfnCount = 0;
                    Lock storPoolRdLock = cmnDebugCtl.getStorPoolDfnMapLock().readLock();
                    try
                    {
                        storPoolRdLock.lock();
                        storPoolDfnCount = cmnDebugCtl.getStorPoolDfnMap().size();
                    }
                    finally
                    {
                        storPoolRdLock.unlock();
                    }

                    int nodesCount = 0;
                    long rscCount = 0;
                    Lock nodesRdLock = cmnDebugCtl.getNodesMapLock().readLock();
                    try
                    {
                        nodesRdLock.lock();
                        Map<NodeName, Node> nodesMap = cmnDebugCtl.getNodesMap();
                        nodesCount = nodesMap.size();
                        for (Node curNode : nodesMap.values())
                        {
                            rscCount += curNode.getResourceCount();
                        }
                    }
                    finally
                    {
                        nodesRdLock.unlock();
                    }

                    int rscDfnCount = 0;
                    Lock rscDfnRdLock = cmnDebugCtl.getRscDfnMapLock().readLock();
                    try
                    {
                        rscDfnRdLock.lock();
                        rscDfnCount = cmnDebugCtl.getRscDfnMap().size();
                    }
                    finally
                    {
                        rscDfnRdLock.unlock();
                    }

                    debugOut.println("Storage management objects");
                    debugOut.printf(
                        "    Nodes:                  %5d\n" +
                        "    Resource definitions    %5d\n" +
                        "    Deployed resources:     %5d\n" +
                        "    Storage pools:          %5d\n",
                        nodesCount, rscDfnCount, rscCount, storPoolDfnCount
                    );
                }
            }
            else
            {
                printError(
                    debugErr,
                    "The " + PRM_DETAIL_NAME + " parameter was specified, but " +
                    "no categories were selected.",
                    null,
                    "Select at least one category to display.",
                    null
                );
            }
        }
        catch (InvalidDetailsException detailsExc)
        {
            printError(
                debugErr,
                String.format(
                    "The following values are not valid for the parameter %s:\n%s",
                    PRM_DETAIL_NAME, detailsExc.list()
                ),
                null,
                "Enter a valid value for the parameter.",
                String.format(
                    "Valid values are:\n" +
                    "    " + PRM_SECURITY + "\n" +
                    "    " + PRM_CONFIG + "\n" +
                    "    " + PRM_STOROBJ + "\n" +
                    "    " + PRM_ALL
                )
            );
        }
        finally
        {
            rcfgRdLock.unlock();
        }
    }

}

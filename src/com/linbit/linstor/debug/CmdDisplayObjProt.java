package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessControlEntry;
import com.linbit.linstor.security.AccessControlList;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.RoleName;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

public class CmdDisplayObjProt extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_OBJ_CLASS   = "CLASS";
    private static final String PRM_OBJ_NAME    = "NAME";

    private static final String CLS_NODE        = "NODE";
    private static final String CLS_RSCDFN      = "RSCDFN";
    private static final String CLS_RSC         = "RSC";
    private static final String CLS_STORPOOL    = "STORPOOL";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_CLASS,
            "Class of the protected object.\n" +
            "Supported classes are:\n" +
            "    " + CLS_NODE + "\n" +
            "    " + CLS_RSCDFN + "\n" +
            "    " + CLS_RSC + "\n" +
            "    " + CLS_STORPOOL
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_NAME,
            "Name of the protected object\n" +
            "The name of resources and storage pools must be specified as a path,\n" +
            "with path components separated by a forward slash (/):\n" +
            "    NodeName/ResourceDefinitionName\n" +
            "    NodeName/StoragePoolName"
        );
    }

    public CmdDisplayObjProt()
    {
        super(
            new String[]
            {
                "DspObjProt"
            },
            "Display object protection",
            "Displays information about the security protection of an object",
            PARAMETER_DESCRIPTIONS,
            null
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
        String objClass = parameters.get(PRM_OBJ_CLASS);
        String objPath = parameters.get(PRM_OBJ_NAME);

        if (objClass != null && objPath != null)
        {
            Lock rcfgRdLock = cmnDebugCtl.getReconfigurationLock().readLock();
            rcfgRdLock.lock();
            try
            {
                switch (objClass.toUpperCase())
                {
                    case CLS_NODE:
                        printNodeProt(debugOut, debugErr, accCtx, objPath);
                        break;
                    case CLS_RSCDFN:
                        printRscDfnProt(debugOut, debugErr, accCtx, objPath);
                        break;
                    case CLS_RSC:
                        printRscProt(debugOut, debugErr, accCtx, objPath);
                        break;
                    case CLS_STORPOOL:
                        printStorPoolDfnProt(debugOut, debugErr, accCtx, objPath);
                        break;
                    default:
                        printError(
                            debugErr,
                            "",
                            "The specified object class is not valid",
                            "Specify a valid object class. Valid classes are:\n" +
                            "    " + CLS_NODE + "\n" +
                            "    " + CLS_RSCDFN + "\n" +
                            "    " + CLS_RSC + "\n" +
                            "    " + CLS_STORPOOL,
                            null
                        );
                        break;
                }
            }
            catch (InvalidNameException invNameExc)
            {
                String msg = "The command line contains an invalid object name";
                printDmException(
                    debugErr,
                    new LinStorException(
                        msg,
                        msg,
                        invNameExc.getMessage(),
                        "Reenter the command using a valid name of an existing object",
                        null
                    )
                );
            }
            catch (AccessDeniedException accExc)
            {
                printDmException(debugErr, accExc);
            }
            finally
            {
                rcfgRdLock.unlock();
            }
        }
        else
        {
            printMultiMissingParamError(debugErr, parameters, PRM_OBJ_CLASS, PRM_OBJ_NAME);
        }
    }

    private void printNodeProt(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        String objPath
    )
        throws InvalidNameException, AccessDeniedException
    {
        NodeName nodeObjName = new NodeName(objPath);
        Lock nodesMapRdLock = cmnDebugCtl.getNodesMapLock().readLock();
        nodesMapRdLock.lock();
        try
        {
            Node nodeObj = cmnDebugCtl.getNodesMap().get(nodeObjName);
            if (nodeObj != null)
            {
                printSectionSeparator(debugOut);
                debugOut.println(
                    "Object protection for node '" + nodeObjName.displayValue + "'"
                );
                printObjProt(debugOut, debugErr, nodeObj.getObjProt());
                printSectionSeparator(debugOut);
            }
            else
            {
                String msg = "No node entry exists for the specified name '" + nodeObjName.displayValue + "'";
                printDmException(
                    debugErr,
                    new LinStorException(
                        msg,
                        msg,
                        null,
                        "Reenter the command using the name of an existing object",
                        null
                    )
                );
            }
        }
        finally
        {
            nodesMapRdLock.unlock();
        }
    }

    private void printRscDfnProt(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        String objPath
    )
        throws InvalidNameException, AccessDeniedException
    {
        ResourceName rscName = new ResourceName(objPath);
        Lock rscDfnMapRdLock = cmnDebugCtl.getRscDfnMapLock().readLock();
        rscDfnMapRdLock.lock();
        try
        {
            ResourceDefinition rscDfn = cmnDebugCtl.getRscDfnMap().get(rscName);
            if (rscDfn != null)
            {
                printSectionSeparator(debugOut);
                debugOut.println(
                    "Object protection for resource definition '" + rscName.displayValue + "'"
                );
                printObjProt(debugOut, debugErr, rscDfn.getObjProt());
                printSectionSeparator(debugOut);
            }
            else
            {
                String msg = "No resource definition entry exists for the specified name '" +
                    rscName.displayValue + "'";
                printDmException(
                    debugErr,
                    new LinStorException(
                        msg,
                        msg,
                        null,
                        "Reenter the command using the name of an existing object",
                        null
                    )
                );
            }
        }
        finally
        {
            rscDfnMapRdLock.unlock();
        }
    }

    private void printRscProt(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        String objPath
    )
        throws InvalidNameException, AccessDeniedException
    {
        String[] pathTokens = objPath.split("/", 2);
        if (pathTokens.length == 2)
        {
            NodeName nodeObjName = new NodeName(pathTokens[0]);
            ResourceName rscName = new ResourceName(pathTokens[1]);
            Lock nodesMapRdLock = cmnDebugCtl.getNodesMapLock().readLock();
            Lock rscDfnMapRdLock = cmnDebugCtl.getRscDfnMapLock().readLock();
            nodesMapRdLock.lock();
            rscDfnMapRdLock.lock();
            try
            {
                Node nodeObj = cmnDebugCtl.getNodesMap().get(nodeObjName);
                if (nodeObj != null)
                {
                    Resource rsc = nodeObj.getResource(accCtx, rscName);
                    if (rsc != null)
                    {
                        printSectionSeparator(debugOut);
                        debugOut.println(
                            "Object protection for resource '" + rscName.displayValue + "' on node '" +
                            nodeObjName.displayValue + "'"
                        );
                        printObjProt(debugOut, debugErr, rsc.getObjProt());
                        printSectionSeparator(debugOut);
                    }
                    else
                    {
                        String msg = "No resource entry exists on the node '" + nodeObjName.displayValue + "' " +
                            "for the specified resource name '" +
                            rscName.displayValue + "'";
                        printDmException(
                            debugErr,
                            new LinStorException(
                                msg,
                                msg,
                                null,
                                "Reenter the command using the name of an existing object",
                                null
                            )
                        );
                    }
                }
                else
                {
                    String msg = "No node entry exists for the specified name '" + nodeObjName.displayValue + "'";
                    printDmException(
                        debugErr,
                        new LinStorException(
                            msg,
                            msg,
                            null,
                            "Reenter the command using the name of an existing object",
                            null
                        )
                    );
                }
            }
            finally
            {
                rscDfnMapRdLock.unlock();
                nodesMapRdLock.unlock();
            }
        }
        else
        {
            String msg = "The specified object path is not valid";
            printDmException(
                debugErr,
                new LinStorException(
                    msg,
                    msg,
                    null,
                    "Reenter the command using a valid object path.\n" +
                    "The format for a resource path is:\n" +
                    "    NodeName/ResourceDefinitionName",
                    null
                )
            );
        }
    }

    private void printStorPoolDfnProt(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        String objPath
    )
        throws InvalidNameException, AccessDeniedException
    {
        StorPoolName spName = new StorPoolName(objPath);
        Lock storPoolMapRdLock = cmnDebugCtl.getStorPoolDfnMapLock().readLock();
        storPoolMapRdLock.lock();
        try
        {
            StorPoolDefinition storPoolObj = cmnDebugCtl.getStorPoolDfnMap().get(spName);
            if (storPoolObj != null)
            {
                printSectionSeparator(debugOut);
                debugOut.println(
                    "Object protection for storage pool definition '" + spName.displayValue + "'"
                );
                printObjProt(debugOut, debugErr, storPoolObj.getObjProt());
                printSectionSeparator(debugOut);
            }
            else
            {
                String msg = "No entry exists for the specified storage pool name '" +
                    spName.displayValue + "'";
                printDmException(
                    debugErr,
                    new LinStorException(
                        msg,
                        msg,
                        null,
                        "Reenter the command using the name of an existing object",
                        null
                    )
                );
            }
        }
        finally
        {
            storPoolMapRdLock.unlock();
        }
    }

    private void printObjProt(
        PrintStream debugOut,
        PrintStream debugErr,
        ObjectProtection objProt
    )
    {
        debugOut.printf(
            "Creator: %-24s Owner: %-24s\n" +
            "Security type: %-24s\n",
            objProt.getCreator().name.displayValue,
            objProt.getOwner().name.displayValue,
            objProt.getSecurityType().name.displayValue
        );
        AccessControlList objAcl = objProt.getAcl();
        Map<RoleName, AccessControlEntry> aclEntries = objAcl.getEntries();
        if (!aclEntries.isEmpty())
        {
            debugOut.println("Access control list entries:");
            debugOut.printf("    %-24s %s\n", "Role", "Access type");
            for (AccessControlEntry entry : aclEntries.values())
            {
                debugOut.printf(
                    "    %-24s %s\n",
                    entry.subjectRole.name.displayValue, entry.access.name()
                );
            }
        }
        else
        {
            debugOut.println("The access control list contains no entries.");
        }
    }
}

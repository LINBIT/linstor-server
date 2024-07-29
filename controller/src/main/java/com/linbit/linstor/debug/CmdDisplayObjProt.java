package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessControlEntry;
import com.linbit.linstor.security.AccessControlList;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.RoleName;
import com.linbit.linstor.security.ShutdownProtHolder;

import javax.inject.Inject;
import javax.inject.Named;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class CmdDisplayObjProt extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_OBJ_CLASS   = "CLASS";
    private static final String PRM_OBJ_NAME    = "NAME";

    private static final String CLS_NODE        = "NODE";
    private static final String CLS_RSCDFN      = "RSCDFN";
    private static final String CLS_RSC         = "RSC";
    private static final String CLS_STORPOOLDFN = "STORPOOLDFN";
    private static final String CLS_SYSOBJ      = "SYSOBJ";

    private static final String SO_NODE_DIR         = "NODEDIR";
    private static final String SO_RSCDFN_DIR       = "RSCDFNDIR";
    private static final String SO_STORPOOLDFN_DIR  = "STORPOOLDFNDIR";
    private static final String SO_CFGVAL           = "CFGVAL";
    private static final String SO_SHUTDOWN         = "SHUTDOWN";

    private static final String LBL_NODE_DIR        = "nodes directory";
    private static final String LBL_RSCDFN_DIR      = "resource definitions directory";
    private static final String LBL_STORPOOLDFN_DIR = "storage pool definitions directory";
    private static final String LBL_CFGVAL          = "application configuration values";
    private static final String LBL_SHUTDOWN        = "application shutdown";

    private static final String PFX_ARTICLE         = "the ";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_CLASS,
            "Class of the protected object.\n" +
            "Supported classes are:\n" +
            "    " + CLS_NODE + "\n" +
            "    " + CLS_RSCDFN + "\n" +
            "    " + CLS_RSC + "\n" +
            "    " + CLS_STORPOOLDFN + "\n" +
            "    " + CLS_SYSOBJ
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_OBJ_NAME,
            "Name of the protected object\n\n" +
            "For nodes (class " + CLS_NODE + ") and resource definitions (class " + CLS_RSCDFN + ")\n" +
            "objects, this is the name of the node or resource definition, respectively.\n\n" +
            "For resources (class " + CLS_RSC + ") the name must be specified as a path,\n" +
            "with path components separated by a forward slash (/):\n" +
            "    NodeName/ResourceName\n\n" +
            "For system objects (class " + CLS_SYSOBJ + "), name is one of:\n" +
            "    " + SO_NODE_DIR + "\n" +
            "        Controls the ability to view, create or delete nodes\n" +
            "    " + SO_RSCDFN_DIR + "\n" +
            "        Controls the ability to view, create or delete resource definitions\n" +
            "    " + SO_STORPOOLDFN_DIR + "\n" +
            "        Controls the ability to view, create or delete storage pool definitions\n" +
            "    " + SO_CFGVAL + "\n" +
            "        Controls the ability to view or change the configuration\n" +
            "    " + SO_SHUTDOWN + "\n" +
            "        LINSTOR shutdown authorization"
        );
    }

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final NodeRepository nodeRepository;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final SystemConfRepository systemConfRepository;
    private final ShutdownProtHolder shutdownProtHolder;

    @Inject
    public CmdDisplayObjProt(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        SystemConfRepository systemConfRepositoryRef,
        ShutdownProtHolder shutdownProtHolderRef
    )
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

        reconfigurationLock = reconfigurationLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        nodeRepository = nodeRepositoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        systemConfRepository = systemConfRepositoryRef;
        shutdownProtHolder = shutdownProtHolderRef;
    }

    @Override
    public void execute(
        final PrintStream debugOut,
        final PrintStream debugErr,
        final AccessContext accCtx,
        final Map<String, String> parameters
    )
        throws Exception
    {
        String objClass = parameters.get(PRM_OBJ_CLASS);
        String objPath = parameters.get(PRM_OBJ_NAME);

        if (objClass != null && objPath != null)
        {
            Lock rcfgRdLock = reconfigurationLock.readLock();
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
                    case CLS_STORPOOLDFN:
                        printStorPoolDfnProt(debugOut, debugErr, accCtx, objPath);
                        break;
                    case CLS_SYSOBJ:
                        printSysObjProt(debugOut, debugErr, accCtx, objPath);
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
                            "    " + CLS_STORPOOLDFN,
                            null
                        );
                        break;
                }
            }
            catch (InvalidNameException invNameExc)
            {
                String msg = "The command line contains an invalid object name";
                printLsException(
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
                printLsException(debugErr, accExc);
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

    private void printSysObjProt(
        final PrintStream debugOut,
        final PrintStream debugErr,
        final AccessContext accCtx,
        final String objPath
    )
        throws InvalidNameException, AccessDeniedException
    {
        final String sysObj = objPath.trim().toUpperCase();
        final ObjectProtection objProt;
        final String label;
        switch (sysObj)
        {
            case SO_NODE_DIR:
                objProt = nodeRepository.getObjProt();
                label = PFX_ARTICLE + LBL_NODE_DIR;
                break;
            case SO_RSCDFN_DIR:
                objProt = resourceDefinitionRepository.getObjProt();
                label = PFX_ARTICLE + LBL_RSCDFN_DIR;
                break;
            case SO_STORPOOLDFN_DIR:
                objProt = storPoolDefinitionRepository.getObjProt();
                label = PFX_ARTICLE + LBL_STORPOOLDFN_DIR;
                break;
            case SO_CFGVAL:
                objProt = systemConfRepository.getObjProt();
                label = LBL_CFGVAL;
                break;
            case SO_SHUTDOWN:
                objProt = shutdownProtHolder.getObjProt();
                label = LBL_SHUTDOWN;
                break;
            default:
                throw new InvalidNameException(
                    "The identifier '" + objPath + "' is not a valid system object name",
                    objPath
                );
        }

        printSectionSeparator(debugOut);
        debugOut.println("Object protection for " + label);
        printObjProt(debugOut, debugErr, objProt);
        printSectionSeparator(debugOut);
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
        Lock nodesMapRdLock = nodesMapLock.readLock();
        nodesMapRdLock.lock();
        try
        {
            Node nodeObj =  nodeRepository.get(accCtx, nodeObjName);
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
                printLsException(
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
        Lock rscDfnMapRdLock = rscDfnMapLock.readLock();
        rscDfnMapRdLock.lock();
        try
        {
            ResourceDefinition rscDfn = resourceDefinitionRepository.get(accCtx, rscName);
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
                printLsException(
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
            Lock nodesMapRdLock = nodesMapLock.readLock();
            Lock rscDfnMapRdLock = rscDfnMapLock.readLock();
            nodesMapRdLock.lock();
            rscDfnMapRdLock.lock();
            try
            {
                Node nodeObj = nodeRepository.get(accCtx, nodeObjName);
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
                        printLsException(
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
                    printLsException(
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
            printLsException(
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
        Lock storPoolMapRdLock = storPoolDfnMapLock.readLock();
        storPoolMapRdLock.lock();
        try
        {
            StorPoolDefinition storPoolObj = storPoolDefinitionRepository.get(accCtx, spName);
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
                printLsException(
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

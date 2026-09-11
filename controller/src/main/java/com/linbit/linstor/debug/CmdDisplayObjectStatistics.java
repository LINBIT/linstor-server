package com.linbit.linstor.debug;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.propscon.ReadOnlyProps;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import java.io.PrintStream;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class CmdDisplayObjectStatistics extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_DETAIL_NAME = "DETAIL";
    private static final String PRM_CONFIG      = "CONFIG";
    private static final String PRM_STOROBJ     = "STOROBJ";
    private static final String PRM_ALL         = "ALL";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_DETAIL_NAME,
            """
            Selects the detail categories to display
                CONFIG
                    Display controller configuration entries count
                STOROBJ
                    Display managed storage management objects count
                    (registered nodes, resource definitions, etc.)
                ALL
                    Selects all categories"""
        );
    }

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock confLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadOnlyProps conf;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;

    @Inject
    public CmdDisplayObjectStatistics(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.CTRL_CONF_LOCK) ReadWriteLock confLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(LinStor.CONTROLLER_PROPS) ReadOnlyProps confRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
    {
        super(
            new String[]
            {
                "DspObjSts"
            },
            "Display object statistics",
            "Displays statistics about registered objects",
            PARAMETER_DESCRIPTIONS,
            null
        );

        reconfigurationLock = reconfigurationLockRef;
        confLock = confLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        conf = confRef;
        storPoolDfnMap = storPoolDfnMapRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        Map<String, String> parameters
    )
        throws Exception
    {
        Lock rcfgRdLock = reconfigurationLock.readLock();
        rcfgRdLock.lock();
        try
        {
            boolean catConfig;
            boolean catStorObj;
            String prmDetail = parameters.get(PRM_DETAIL_NAME);
            if (prmDetail == null)
            {
                catConfig   = true;
                catStorObj  = true;
            }
            else
            {
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
                        case PRM_CONFIG -> { catConfig = true; }
                        case PRM_STOROBJ -> { catStorObj = true; }
                        case PRM_ALL ->
                        {
                            catConfig   = true;
                            catStorObj  = true;
                        }
                        default ->
                        {
                            if (detailsExc == null)
                            {
                                detailsExc = new InvalidDetailsException();
                            }
                            detailsExc.addInvalid(curToken);
                        }
                    }
                }
                if (detailsExc != null)
                {
                    throw detailsExc;
                }
            }

            if (catConfig || catStorObj)
            {
                debugOut.println("Object counts");
                printSectionSeparator(debugOut);

                if (catConfig)
                {
                    Lock confRdLock = confLock.readLock();
                    int count = 0;
                    confRdLock.lock();
                    try
                    {
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
                    Lock storPoolRdLock = storPoolDfnMapLock.readLock();
                    storPoolRdLock.lock();
                    try
                    {
                        storPoolDfnCount = storPoolDfnMap.size();
                    }
                    finally
                    {
                        storPoolRdLock.unlock();
                    }

                    int nodesCount = 0;
                    long rscCount = 0;
                    Lock nodesRdLock = nodesMapLock.readLock();
                    nodesRdLock.lock();
                    try
                    {
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
                    Lock rscDfnRdLock = rscDfnMapLock.readLock();
                    rscDfnRdLock.lock();
                    try
                    {
                        rscDfnCount = rscDfnMap.size();
                    }
                    finally
                    {
                        rscDfnRdLock.unlock();
                    }

                    debugOut.println("Storage management objects");
                    debugOut.printf(
                        """
                            Nodes:                  %5d
                            Resource definitions    %5d
                            Deployed resources:     %5d
                            Storage pools:          %5d
                        """,
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

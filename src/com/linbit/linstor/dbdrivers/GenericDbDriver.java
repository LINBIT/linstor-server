package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceDataGenericDbDriver;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnectionData;
import com.linbit.linstor.NodeConnectionDataGenericDbDriver;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeDataGenericDbDriver;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.ResourceConnectionDataGenericDbDriver;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDataGenericDbDriver;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionDataGenericDbDriver;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDataGenericDbDriver;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataGenericDbDriver;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeConnectionData;
import com.linbit.linstor.VolumeConnectionDataGenericDbDriver;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDataGenericDbDriver;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeDefinitionDataGenericDbDriver;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.Tripple;
import com.linbit.utils.Tuple;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class GenericDbDriver implements DatabaseDriver
{
    public static final ServiceName DFLT_SERVICE_INSTANCE_NAME;

    static
    {
        try
        {
            DFLT_SERVICE_INSTANCE_NAME = new ServiceName("GernericDatabaseService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The builtin default service instance name is not a valid ServiceName",
                nameExc
            );
        }
    }

    private final AccessContext dbCtx;
    private final NodeDataGenericDbDriver nodeDriver;
    private final NetInterfaceDataGenericDbDriver netIfDriver;
    private final NodeConnectionDataGenericDbDriver nodeConnDriver;
    private final ResourceDefinitionDataGenericDbDriver rscDfnDriver;
    private final ResourceDataGenericDbDriver rscDriver;
    private final ResourceConnectionDataGenericDbDriver rscConnDriver;
    private final VolumeDefinitionDataGenericDbDriver vlmDfnDriver;
    private final VolumeDataGenericDbDriver vlmDriver;
    private final VolumeConnectionDataGenericDbDriver vlmConnDriver;
    private final StorPoolDefinitionDataGenericDbDriver storPoolDfnDriver;
    private final StorPoolDataGenericDbDriver storPoolDriver;

    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    @Inject
    public GenericDbDriver(
        @SystemContext AccessContext privCtx,
        NodeDataGenericDbDriver nodeDriverRef,
        NetInterfaceDataGenericDbDriver netIfDriverRef,
        NodeConnectionDataGenericDbDriver nodeConnDriverRef,
        ResourceDefinitionDataGenericDbDriver resesourceDefinitionDriverRef,
        ResourceDataGenericDbDriver resourceDriverRef,
        ResourceConnectionDataGenericDbDriver rscConnDriverRef,
        VolumeDefinitionDataGenericDbDriver vlmDfnDriverRef,
        VolumeDataGenericDbDriver volumeDriverRef,
        VolumeConnectionDataGenericDbDriver vlmConnDriverRef,
        StorPoolDefinitionDataGenericDbDriver storPoolDefinitionDriverRef,
        StorPoolDataGenericDbDriver storPoolDriverRef,
        @Uninitialized CoreModule.NodesMap nodesMapRef,
        @Uninitialized CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Uninitialized CoreModule.StorPoolDefinitionMap storPoolDfnMapRef
    )
    {
        dbCtx = privCtx;
        nodeDriver = nodeDriverRef;
        netIfDriver = netIfDriverRef;
        nodeConnDriver = nodeConnDriverRef;
        rscDfnDriver = resesourceDefinitionDriverRef;
        rscDriver = resourceDriverRef;
        rscConnDriver = rscConnDriverRef;
        vlmDfnDriver = vlmDfnDriverRef;
        vlmDriver = volumeDriverRef;
        vlmConnDriver = vlmConnDriverRef;
        storPoolDfnDriver = storPoolDefinitionDriverRef;
        storPoolDriver = storPoolDriverRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
    }

    /**
     * This method should only be called with an locked reconfiguration write lock
     */
    @Override
    public void loadAll() throws SQLException
    {
        try
        {
            // load the main objects (nodes, rscDfns, storPoolDfns)
            Map<NodeData, Node.InitMaps> loadedNodesMap = nodeDriver.loadAll();
            Map<ResourceDefinitionData, ResourceDefinition.InitMaps> loadedRscDfnsMap = rscDfnDriver.loadAll();
            Map<StorPoolDefinitionData, StorPoolDefinition.InitMaps> loadedStorPoolDfnsMap =
                storPoolDfnDriver.loadAll();

            // build temporary maps for easier restoring of the remaining objects
            Map<NodeName, NodeData> tmpNodesMap =
                mapByName(loadedNodesMap, Node::getName);
            Map<ResourceName, ResourceDefinitionData> tmpRscDfnMap =
                mapByName(loadedRscDfnsMap, ResourceDefinitionData::getName);
            Map<StorPoolName, StorPoolDefinitionData> tmpStorPoolDfnMap =
                mapByName(loadedStorPoolDfnsMap, StorPoolDefinitionData::getName);

            List<NetInterfaceData> loadedNetIfs = netIfDriver.loadAll(tmpNodesMap);
            for (NetInterfaceData netIf : loadedNetIfs)
            {
                loadedNodesMap.get(netIf.getNode())
                    .getNetIfMap().put(netIf.getName(), netIf);
            }

            List<NodeConnectionData> loadedNodeConns = nodeConnDriver.loadAll(tmpNodesMap);
            for (NodeConnectionData nodeConn : loadedNodeConns)
            {
                Node sourceNode = nodeConn.getSourceNode(dbCtx);
                Node targetNode = nodeConn.getTargetNode(dbCtx);
                loadedNodesMap.get(sourceNode).getNodeConnMap().put(targetNode, nodeConn);
                loadedNodesMap.get(targetNode).getNodeConnMap().put(sourceNode, nodeConn);
            }

            // loading storage pools
            Map<StorPoolData, StorPool.InitMaps> loadedStorPools = storPoolDriver.loadAll(
                tmpNodesMap,
                tmpStorPoolDfnMap
            );
            for (StorPoolData storPool : loadedStorPools.keySet())
            {
                loadedNodesMap.get(storPool.getNode()).getStorPoolMap()
                    .put(storPool.getName(), storPool);
                loadedStorPoolDfnsMap.get(storPool.getDefinition(dbCtx)).getStorPoolMap()
                    .put(storPool.getNode().getName(), storPool);
            }


            // temporary storPool map
            Map<Tuple<NodeName, StorPoolName>, StorPoolData> tmpStorPoolMap =
                mapByName(loadedStorPools, storPool -> new Tuple<>(
                    storPool.getNode().getName(),
                    storPool.getName()
                )
            );

            // loading resources
            Map<ResourceData, Resource.InitMaps> loadedResources = rscDriver.loadAll(tmpNodesMap, tmpRscDfnMap);
            for (ResourceData rsc : loadedResources.keySet())
            {
                loadedNodesMap.get(rsc.getAssignedNode()).getRscMap()
                    .put(rsc.getDefinition().getName(), rsc);
                loadedRscDfnsMap.get(rsc.getDefinition()).getRscMap()
                    .put(rsc.getAssignedNode().getName(), rsc);
            }

            // temporary resource map
            Map<Tuple<NodeName, ResourceName>, ResourceData> tmpRscMap =
                mapByName(loadedResources, rsc -> new Tuple<>(
                    rsc.getAssignedNode().getName(),
                    rsc.getDefinition().getName()
                )
            );

            // loading resource connections
            List<ResourceConnectionData> loadedRscConns = rscConnDriver.loadAll(tmpRscMap);
            for (ResourceConnectionData rscConn : loadedRscConns)
            {
                Resource sourceResource = rscConn.getSourceResource(dbCtx);
                Resource targetResource = rscConn.getTargetResource(dbCtx);
                loadedResources.get(sourceResource).getRscConnMap().put(targetResource, rscConn);
                loadedResources.get(targetResource).getRscConnMap().put(sourceResource, rscConn);
            }

            // loading volume definitions
            Map<VolumeDefinitionData, VolumeDefinition.InitMaps> loadedVlmDfnMap =
                vlmDfnDriver.loadAll(tmpRscDfnMap);

            for (VolumeDefinitionData vlmDfn : loadedVlmDfnMap.keySet())
            {
                loadedRscDfnsMap.get(vlmDfn.getResourceDefinition()).getVlmDfnMap()
                    .put(vlmDfn.getVolumeNumber(), vlmDfn);
            }

            // temporary volume definition map
            Map<Tuple<ResourceName, VolumeNumber>, VolumeDefinitionData> tmpVlmDfnMap =
                mapByName(loadedVlmDfnMap, vlmDfn -> new Tuple<>(
                    vlmDfn.getResourceDefinition().getName(),
                    vlmDfn.getVolumeNumber()
                )
            );

            // loading volumes
            Map<VolumeData, Volume.InitMaps> loadedVolumes = vlmDriver.loadAll(
                tmpRscMap,
                tmpVlmDfnMap,
                tmpStorPoolMap
            );

            for (VolumeData vlm : loadedVolumes.keySet())
            {
                loadedStorPools.get(vlm.getStorPool(dbCtx)).getVolumeMap()
                    .put(Volume.getVolumeKey(vlm), vlm);
                loadedResources.get(vlm.getResource()).getVlmMap()
                    .put(vlm.getVolumeDefinition().getVolumeNumber(), vlm);
                loadedVlmDfnMap.get(vlm.getVolumeDefinition()).getVlmMap()
                    .put(Resource.getStringId(vlm.getResource()), vlm);
            }

            // temporary volume map
            Map<Tripple<NodeName, ResourceName, VolumeNumber>, VolumeData> tmpVlmMap =
                mapByName(loadedVolumes, vlm -> new Tripple<>(
                    vlm.getResource().getAssignedNode().getName(),
                    vlm.getResourceDefinition().getName(),
                    vlm.getVolumeDefinition().getVolumeNumber()
                )
            );

            List<VolumeConnectionData> loadedVlmConns = vlmConnDriver.loadAll(tmpVlmMap);
            for (VolumeConnectionData vlmConn : loadedVlmConns)
            {
                Volume sourceVolume = vlmConn.getSourceVolume(dbCtx);
                Volume targetVolume = vlmConn.getTargetVolume(dbCtx);
                loadedVolumes.get(sourceVolume).getVolumeConnections().put(targetVolume, vlmConn);
                loadedVolumes.get(targetVolume).getVolumeConnections().put(sourceVolume, vlmConn);
            }

            nodesMap.putAll(tmpNodesMap);
            rscDfnMap.putAll(tmpRscDfnMap);
            storPoolDfnMap.putAll(tmpStorPoolDfnMap);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("dbCtx has not enough privileges", exc);
        }
    }

    private <NAME, DATA> TreeMap<NAME, DATA> mapByName(
        Map<DATA, ?> map, Function<? super DATA, NAME> nameMapper
    )
    {
        return map.keySet().stream().collect(
            Collectors.toMap(nameMapper, Function.identity(), throwingMerger(), TreeMap::new));
    }

    @Override
    public ServiceName getDefaultServiceInstanceName()
    {
        return DFLT_SERVICE_INSTANCE_NAME;
    }

    public static void handleAccessDeniedException(AccessDeniedException accDeniedExc)
        throws ImplementationError
    {
        throw new ImplementationError(
            "Database's access context has insufficient permissions",
            accDeniedExc
        );
    }

    public static void runSql(final Connection con, final String script)
        throws SQLException
    {
        StringBuilder cmdBuilder = new StringBuilder();
        Scanner scanner = new Scanner(script);
        while (scanner.hasNextLine())
        {
            String trimmedLine = scanner.nextLine().trim();
            if (!trimmedLine.startsWith("--"))
            {
                cmdBuilder.append("\n").append(trimmedLine);
                if (trimmedLine.endsWith(";"))
                {
                    cmdBuilder.setLength(cmdBuilder.length() - 1); // cut the ;
                    String cmd = cmdBuilder.toString();
                    cmdBuilder.setLength(0);
                    executeStatement(con, cmd);
                }
            }
        }
        con.commit();
        scanner.close();
    }

    public static void runSql(Connection con, BufferedReader br)
        throws IOException, SQLException
    {
        StringBuilder cmdBuilder = new StringBuilder();
        for (String line = br.readLine(); line != null; line = br.readLine())
        {
            String trimmedLine = line.trim();
            if (!trimmedLine.startsWith("--"))
            {
                cmdBuilder.append("\n").append(trimmedLine);
                if (trimmedLine.endsWith(";"))
                {
                    cmdBuilder.setLength(cmdBuilder.length() - 1); // cut the ;
                    String cmd = cmdBuilder.toString();
                    cmdBuilder.setLength(0);
                    executeStatement(con, cmd);
                }
            }
        }
        con.commit();
    }

    public static int executeStatement(final Connection con, final String statement)
        throws SQLException
    {
        int ret = 0;
        try (PreparedStatement stmt = con.prepareStatement(statement))
        {
            ret = stmt.executeUpdate();
        }
        catch (SQLException throwable)
        {
            System.err.println("Error: " + statement);
            throw throwable;
        }
        return ret;
    }

    private static <T> BinaryOperator<T> throwingMerger()
    {
        return (key, value) ->
        {
            throw new ImplementationError("At least two objects have the same name.\n" +
                "That should have caused an sql exception when inserting. Key: '" + key + "'",
                null
            );
        };
    }
}

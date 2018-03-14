package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeDataDerbyDriver;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDataDerbyDriver;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionDataDerbyDriver;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDataDerbyDriver;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataDerbyDriver;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeDataDerbyDriver;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

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

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DerbyDriver implements DatabaseDriver
{
    public static final ServiceName DFLT_SERVICE_INSTANCE_NAME;

    static
    {
        try
        {
            DFLT_SERVICE_INSTANCE_NAME = new ServiceName("DerbyDatabaseService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The builtin default service instance name is not a valid ServiceName",
                nameExc
            );
        }
    }

    private final NodeDataDerbyDriver nodeDriver;
    private final ResourceDefinitionDataDerbyDriver resesourceDefinitionDriver;
    private final ResourceDataDerbyDriver resourceDriver;
    private final VolumeDataDerbyDriver volumeDriver;
    private final StorPoolDefinitionDataDerbyDriver storPoolDefinitionDriver;
    private final StorPoolDataDerbyDriver storPoolDriver;

    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    @Inject
    public DerbyDriver(
        NodeDataDerbyDriver nodeDriverRef,
        ResourceDefinitionDataDerbyDriver resesourceDefinitionDriverRef,
        ResourceDataDerbyDriver resourceDriverRef,
        VolumeDataDerbyDriver volumeDriverRef,
        StorPoolDefinitionDataDerbyDriver storPoolDefinitionDriverRef,
        StorPoolDataDerbyDriver storPoolDriverRef,
        @Uninitialized CoreModule.NodesMap nodesMapRef,
        @Uninitialized CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Uninitialized CoreModule.StorPoolDefinitionMap storPoolDfnMapRef
    )
    {
        nodeDriver = nodeDriverRef;
        resesourceDefinitionDriver = resesourceDefinitionDriverRef;
        resourceDriver = resourceDriverRef;
        volumeDriver = volumeDriverRef;
        storPoolDefinitionDriver = storPoolDefinitionDriverRef;
        storPoolDriver = storPoolDriverRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
    }

    @Override
    public void loadAll() throws SQLException
    {
        // order is somewhat important here, storage pool definitions should be loaded first
        // and added to the storPoolDfnMap, otherwise the later node loading will not correctly
        // link its storage pools with the definitions.

        List<StorPoolDefinitionData> storPoolDfnList = storPoolDefinitionDriver.loadAll();
        for (StorPoolDefinition curStorPoolDfn : storPoolDfnList)
        {
            storPoolDfnMap.put(curStorPoolDfn.getName(), curStorPoolDfn);
        }

        List<NodeData> nodeList = nodeDriver.loadAll();
        for (Node curNode : nodeList)
        {
            nodesMap.put(curNode.getName(), curNode);
        }

        List<ResourceDefinitionData> rscDfnList = resesourceDefinitionDriver.loadAll();

        nodeDriver.clearCache();
        resesourceDefinitionDriver.clearCache();
        resourceDriver.clearCache();
        volumeDriver.clearCache();
        storPoolDriver.clearCache();

        for (ResourceDefinition curRscDfn : rscDfnList)
        {
            rscDfnMap.put(curRscDfn.getName(), curRscDfn);
        }
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
}

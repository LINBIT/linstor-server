package com.linbit.linstor.test.factories;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Node.Type;
import com.linbit.linstor.core.objects.NodeControllerFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Singleton
public class NodeTestFactory
{
    private final NodeControllerFactory fact;
    private final AtomicInteger nextId = new AtomicInteger(0);

    private final HashMap<String, Node> nodesMap = new HashMap<>();

    private String dfltNodeNamePattern = "node-%d";
    private Supplier<String> dfltNodeNameSupplier = () -> String.format(dfltNodeNamePattern, nextId.incrementAndGet());
    private Type dfltNodeType = Node.Type.SATELLITE;
    private Node.Flags[] dfltFlags = new Node.Flags[0];

    @Inject
    public NodeTestFactory(NodeControllerFactory factRef)
    {
        fact = factRef;
    }

    public Node get(String nodeNameRef, boolean createIfNotExists)
        throws DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        Node node = nodesMap.get(nodeNameRef.toUpperCase());
        if (node == null && createIfNotExists)
        {
            node = create(nodeNameRef);
            nodesMap.put(nodeNameRef.toUpperCase(), node);
        }
        return node;
    }

    public NodeTestFactory setDfltNodeNamePattern(String dfltNodeNamePatternRef)
    {
        dfltNodeNamePattern = dfltNodeNamePatternRef;
        return this;
    }

    public NodeTestFactory setDfltNodeNameSupplier(Supplier<String> dfltNodeNameSupplierRef)
    {
        dfltNodeNameSupplier = dfltNodeNameSupplierRef;
        return this;
    }

    public NodeTestFactory setDfltFlags(Node.Flags[] dfltFlagsRef)
    {
        dfltFlags = dfltFlagsRef;
        return this;
    }

    public NodeTestFactory setDfltNodeType(Type dfltNodeTypeRef)
    {
        dfltNodeType = dfltNodeTypeRef;
        return this;
    }

    public Node createNext()
        throws DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder().build();
    }

    public Node create(String nodeName)
        throws DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder(nodeName).build();
    }

    public NodeBuilder builder()
    {
        return new NodeBuilder(dfltNodeNameSupplier.get());
    }

    public NodeBuilder builder(String nodeName)
    {
        return new NodeBuilder(nodeName);
    }

    public class NodeBuilder
    {
        private String nodeName;
        private Type nodeType;
        private Node.Flags[] flags;

        public NodeBuilder(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            nodeType = dfltNodeType;
            flags = dfltFlags;
        }

        public NodeBuilder setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        public NodeBuilder setNodeType(Type nodeTypeRef)
        {
            nodeType = nodeTypeRef;
            return this;
        }

        public NodeBuilder setFlags(Node.Flags[] flagsRef)
        {
            flags = flagsRef;
            return this;
        }

        public Node build()
            throws DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
        {
            Node node = fact.create(
                new NodeName(nodeName),
                nodeType,
                flags
            );
            nodesMap.put(nodeName.toUpperCase(), node);
            return node;
        }
    }
}

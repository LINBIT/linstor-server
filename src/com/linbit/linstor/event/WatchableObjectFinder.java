package com.linbit.linstor.event;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Singleton
public class WatchableObjectFinder
{
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap resourceDefinitionMap;

    @Inject
    public WatchableObjectFinder(
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap resourceDefinitionMapRef
    )
    {
        nodesMap = nodesMapRef;
        resourceDefinitionMap = resourceDefinitionMapRef;
    }

    /**
     * Requires the nodes-map and resource-definition-map read locks to be held.
     */
    public Map<WatchableObject, List<ObjectIdentifier>> findDescendantObjects(
        AccessContext accCtx,
        ObjectIdentifier objectIdentifier
    )
        throws AccessDeniedException
    {
        ObjectCollector objectCollector = new ObjectCollector(accCtx);

        if (objectIdentifier.getNodeName() == null)
        {
            // No node specified
            if (objectIdentifier.getResourceName() == null)
            {
                // No resource specified
                objectCollector.addAll(nodesMap.values(), resourceDefinitionMap.values());
            }
            else
            {
                // Resource specified
                if (objectIdentifier.getVolumeNumber() == null)
                {
                    // No volume specified
                    objectCollector.addResourceDefinitionDescendants(findResourceDefinition(objectIdentifier));
                }
                else
                {
                    // Volume specified
                    objectCollector.addVolumeDefinitionDescendants(findVolumeDefinition(accCtx, objectIdentifier));
                }
            }
        }
        else
        {
            // Node specified
            if (objectIdentifier.getResourceName() == null)
            {
                // No resource specified
                objectCollector.addNodeDescendants(findNode(objectIdentifier));
            }
            else
            {
                // Resource specified
                if (objectIdentifier.getVolumeNumber() == null)
                {
                    // No volume specified
                    objectCollector.addResourceDescendants(findResource(accCtx, objectIdentifier));
                }
                else
                {
                    // Volume specified
                    objectCollector.addVolumeDescendants(findVolume(accCtx, objectIdentifier));
                }
            }
        }

        return objectCollector.getObjectMap();
    }

    private ResourceDefinition findResourceDefinition(ObjectIdentifier objectIdentifier)
    {
        return resourceDefinitionMap.get(objectIdentifier.getResourceName());
    }

    private VolumeDefinition findVolumeDefinition(
        AccessContext accCtx,
        ObjectIdentifier objectIdentifier
    )
        throws AccessDeniedException
    {
        return findResourceDefinition(objectIdentifier).getVolumeDfn(accCtx, objectIdentifier.getVolumeNumber());
    }

    private Node findNode(ObjectIdentifier objectIdentifier)
    {
        return nodesMap.get(objectIdentifier.getNodeName());
    }

    private Resource findResource(AccessContext accCtx, ObjectIdentifier objectIdentifier)
        throws AccessDeniedException
    {
        return findResourceDefinition(objectIdentifier).getResource(accCtx, objectIdentifier.getNodeName());
    }

    private Volume findVolume(AccessContext accCtx, ObjectIdentifier objectIdentifier)
        throws AccessDeniedException
    {
        return findResource(accCtx, objectIdentifier).getVolume(objectIdentifier.getVolumeNumber());
    }

    private static class ObjectCollector
    {
        private final AccessContext accCtx;
        private Map<WatchableObject, List<ObjectIdentifier>> objectMap;

        ObjectCollector(AccessContext accCtxRef)
        {
            accCtx = accCtxRef;
            objectMap = new HashMap<>();
            Stream.of(WatchableObject.values()).forEach(objectType -> objectMap.put(objectType, new ArrayList<>()));
        }

        public void addAll(Collection<Node> nodes, Collection<ResourceDefinition> resourceDefinitions)
            throws AccessDeniedException
        {
            add(
                WatchableObject.ROOT,
                new ObjectIdentifier(
                    null,
                    null,
                    null
                )
            );

            nodes.forEach(this::addNode);

            for (ResourceDefinition resourceDefinition : resourceDefinitions)
            {
                addResourceDefinitionDescendants(resourceDefinition);
            }
        }

        public void addNodeDescendants(Node node)
            throws AccessDeniedException
        {
            addNode(node);

            node.streamResources(accCtx).forEach(this::addResourceDescendants);
        }

        private void addNode(Node node)
        {
            add(
                WatchableObject.NODE,
                new ObjectIdentifier(
                    node.getName(),
                    null,
                    null
                )
            );
        }

        public void addResourceDefinitionDescendants(ResourceDefinition resourceDefinition)
            throws AccessDeniedException
        {
            add(
                WatchableObject.RESOURCE_DEFINITION,
                new ObjectIdentifier(
                    null,
                    resourceDefinition.getName(),
                    null
                )
            );

            Iterator<VolumeDefinition> iterator = resourceDefinition.iterateVolumeDfn(accCtx);
            while (iterator.hasNext())
            {
                addVolumeDefinitionDescendants(iterator.next());
            }

            resourceDefinition.streamResource(accCtx).forEach(this::addResource);
        }

        public void addVolumeDefinitionDescendants(VolumeDefinition volumeDefinition)
            throws AccessDeniedException
        {
            add(
                WatchableObject.VOLUME_DEFINITION,
                new ObjectIdentifier(
                    null,
                    volumeDefinition.getResourceDefinition().getName(),
                    volumeDefinition.getVolumeNumber()
                )
            );

            volumeDefinition.streamVolumes(accCtx).forEach(this::addVolumeDescendants);
        }

        public void addResourceDescendants(Resource resource)
        {
            addResource(resource);

            resource.streamVolumes().forEach(this::addVolumeDescendants);
        }

        private void addResource(Resource resource)
        {
            add(
                WatchableObject.RESOURCE,
                new ObjectIdentifier(
                    resource.getAssignedNode().getName(),
                    resource.getDefinition().getName(),
                    null
                )
            );
        }

        public void addVolumeDescendants(Volume volume)
        {
            add(
                WatchableObject.VOLUME,
                new ObjectIdentifier(
                    volume.getResource().getAssignedNode().getName(),
                    volume.getResourceDefinition().getName(),
                    volume.getVolumeDefinition().getVolumeNumber()
                )
            );
        }

        private void add(WatchableObject objectType, ObjectIdentifier objectIdentifier)
        {
            objectMap.get(objectType).add(objectIdentifier);
        }

        public Map<WatchableObject, List<ObjectIdentifier>> getObjectMap()
        {
            return objectMap;
        }
    }
}

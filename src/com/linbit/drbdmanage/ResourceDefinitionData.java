package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import com.linbit.drbdmanage.ResourceDefinition.RscDfnFlags;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceDefinitionData implements ResourceDefinition
{
    // Object identifier
    private UUID objId;

    // Resource name
    private ResourceName resourceName;

    // Connections to the peer resources
    private Map<NodeName, Map<Integer, ConnectionDefinition>> connectionMap;

    // Volumes of the resource
    private Map<VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    private Map<NodeName, Resource> resourceMap;

    // State flags
    private StateFlags<RscDfnFlags> flags;

    // Object access controls
    private ObjectProtection objProt;

    // Properties container for this resource definition
    private Props rscDfnProps;

    ResourceDefinitionData(
        AccessContext accCtx,
        ResourceName resName,
        SerialGenerator srlGen
    )
    {
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ResourceName.class, resName);
        objId = UUID.randomUUID();
        resourceName = resName;
        connectionMap = new TreeMap<>();
        volumeMap = new TreeMap<>();
        resourceMap = new TreeMap<>();
        rscDfnProps = SerialPropsContainer.createRootContainer(srlGen);
        objProt = new ObjectProtection(accCtx);
        flags = new RscDfnFlagsImpl(objProt);
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ResourceName getName()
    {
        return resourceName;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        return PropsAccess.secureGetProps(accCtx, objProt, rscDfnProps);
    }

    @Override
    public ConnectionDefinition getConnectionDfn(AccessContext accCtx, NodeName clNodeName, Integer connNr)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        ConnectionDefinition connDfn = null;
        Map<Integer, ConnectionDefinition> nodeConnMap = connectionMap.get(clNodeName);
        if (nodeConnMap != null)
        {
            connDfn = nodeConnMap.get(connNr);
        }
        return connDfn;
    }

    @Override
    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.get(volNr);
    }

    @Override
    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().iterator();
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(clNodeName);
    }

    @Override
    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(resRef.getAssignedNode().getName(), resRef);
    }

    @Override
    public void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(resRef.getAssignedNode().getName());
    }

    @Override
    public StateFlags<RscDfnFlags> getFlags()
    {
        return flags;
    }

    private static final class RscDfnFlagsImpl extends StateFlagsBits<RscDfnFlags>
    {
        RscDfnFlagsImpl(ObjectProtection objProtRef)
        {
            super(objProtRef, StateFlagsBits.getMask(RscDfnFlags.ALL_FLAGS));
        }
    }
}

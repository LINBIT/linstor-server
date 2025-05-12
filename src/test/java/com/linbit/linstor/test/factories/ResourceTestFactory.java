package com.linbit.linstor.test.factories;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceControllerFactory;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.utils.Pair;

import static com.linbit.linstor.test.factories.TestFactoryUtils.copyOrNull;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Singleton
public class ResourceTestFactory
{
    private final ResourceControllerFactory rscFact;
    private final NodeTestFactory nodeFact;
    private final ResourceDefinitionTestFactory rscDfnFact;

    private final HashMap<Pair<String, String>, Resource> rscMap = new HashMap<>();

    private AccessContext dfltAccCtx = TestAccessContextProvider.PUBLIC_CTX;
    private LayerPayload dfltPayload = null;
    private Flags[] dfltFlags = new Flags[0];
    private List<DeviceLayerKind> dfltLayerStack = Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE);

    @Inject
    public ResourceTestFactory(
        ResourceControllerFactory rscFactRef,
        NodeTestFactory nodeFactRef,
        ResourceDefinitionTestFactory rscDfnFactRef
    )
    {
        rscFact = rscFactRef;
        nodeFact = nodeFactRef;
        rscDfnFact = rscDfnFactRef;
    }

    public Resource get(String nodeName, String rscName, boolean createIfNotExists)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, LinStorException
    {
        Resource rsc = rscMap.get(new Pair<>(nodeName.toUpperCase(), rscName.toUpperCase()));
        if (rsc == null && createIfNotExists)
        {
            rsc = create(nodeName, rscName);
        }
        return rsc;
    }

    public ResourceTestFactory setDfltAccCtx(AccessContext dfltAccCtxRef)
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public ResourceTestFactory setDfltPayload(LayerPayload dfltPayloadRef)
    {
        dfltPayload = dfltPayloadRef;
        return this;
    }

    public ResourceTestFactory setDfltFlags(Flags[] dfltFlagsRef)
    {
        dfltFlags = dfltFlagsRef;
        return this;
    }

    public ResourceTestFactory setDfltLayerStack(List<DeviceLayerKind> dfltLayerStackRef)
    {
        dfltLayerStack = dfltLayerStackRef;
        return this;
    }

    public Resource create(Node node, ResourceDefinition rscDfn)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, LinStorException
    {
        return builder(node.getName().displayValue, rscDfn.getName().displayValue).build();
    }

    public Resource create(String nodeName, String rscName)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, LinStorException
    {
        return builder(nodeName, rscName).build();
    }

    public ResourceBuilder builder(String nodeName, String rscName)
    {
        return new ResourceBuilder(nodeName, rscName);
    }

    public class ResourceBuilder
    {
        private AccessContext accCtx;
        private String nodeName;
        private String rscName;
        private LayerPayload payload;
        private Flags[] flags;
        private List<DeviceLayerKind> layerStack;

        public ResourceBuilder(String nodeNameRef, String rscNameRef)
        {
            nodeName = nodeNameRef;
            rscName = rscNameRef;

            accCtx = dfltAccCtx;
            payload = dfltPayload;
            flags = dfltFlags;
            layerStack = copyOrNull(dfltLayerStack);
        }

        public ResourceBuilder setFlags(Flags[] flagsRef)
        {
            flags = flagsRef;
            return this;
        }

        public ResourceBuilder setLayerStack(List<DeviceLayerKind> layerStackRef)
        {
            layerStack = layerStackRef;
            return this;
        }

        public Resource build()
            throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException,
            ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, InvalidNameException,
            LinStorException
        {
            Resource rsc = rscFact.create(
                accCtx,
                rscDfnFact.get(rscName, true),
                nodeFact.get(nodeName, true),
                payload,
                flags,
                layerStack
            );
            rscMap.put(new Pair<>(nodeName.toUpperCase(), rscName.toUpperCase()), rsc);
            return rsc;
        }
    }
}

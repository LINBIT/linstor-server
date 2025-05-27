package com.linbit.linstor.test.factories;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinition.Flags;
import com.linbit.linstor.core.objects.ResourceDefinitionControllerFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import static com.linbit.linstor.test.factories.TestFactoryUtils.copyOrNull;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Singleton
public class ResourceDefinitionTestFactory
{
    private final ResourceDefinitionControllerFactory rscDfnFact;
    private final ResourceGroupTestFactory rscGrpFact;

    private final HashMap<String, ResourceDefinition> rscDfnMap = new HashMap<>();

    private final AtomicInteger nextId = new AtomicInteger();
    private final AtomicInteger nextSecret = new AtomicInteger(0);

    private String dfltRscNamePattern = "rsc-%02d";
    private String dfltSecretPattern = "superSecret-%02d";
    private String dfltRscGroupName = InternalApiConsts.DEFAULT_RSC_GRP_NAME;

    private AccessContext dfltAccCtx = TestAccessContextProvider.PUBLIC_CTX;
    private Supplier<String> dfltRscNameSupplier = () -> String.format(dfltRscNamePattern, nextId.incrementAndGet());
    private byte[] dfltExtName = null;
    private Flags[] dfltFlags = new Flags[0];
    private Supplier<String> dfltSecret = () -> String.format(dfltSecretPattern, nextSecret.incrementAndGet());
    private TransportType dfltTransType = TransportType.IP;
    private List<DeviceLayerKind> dfltLayerStack = Collections.emptyList();
    private LayerPayload dfltPayload = new LayerPayload();
    private Short dfltPeerSlots = null;

    @Inject
    public ResourceDefinitionTestFactory(
        ResourceGroupTestFactory rscGrpFactRef,
        ResourceDefinitionControllerFactory rscDfnFactRef
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException,
            LinStorException
    {
        rscGrpFact = rscGrpFactRef;
        rscDfnFact = rscDfnFactRef;
    }

    public ResourceDefinition get(String rscNameRef, boolean createIfNotExists)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, LinStorException
    {
        ResourceDefinition rscDfn = rscDfnMap.get(rscNameRef.toUpperCase());
        if (rscDfn == null && createIfNotExists)
        {
            rscDfn = create(rscNameRef);
        }
        return rscDfn;
    }

    public ResourceDefinitionTestFactory setDfltAccCtx(AccessContext dfltAccCtxRef)
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltRscNamePattern(String dfltRscNamePatternRef)
    {
        dfltRscNamePattern = dfltRscNamePatternRef;
        return this;
    }

    public void setDfltRscNameSupplier(Supplier<String> dfltRscNameSupplierRef)
    {
        dfltRscNameSupplier = dfltRscNameSupplierRef;
    }

    public ResourceDefinitionTestFactory setDfltExtName(byte[] dfltExtNameRef)
    {
        dfltExtName = dfltExtNameRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltFlags(Flags[] dfltFlagsRef)
    {
        dfltFlags = dfltFlagsRef;
        return this;
    }

    public void setDfltSecretPattern(String dfltSecretPatternRef)
    {
        dfltSecretPattern = dfltSecretPatternRef;
    }

    public ResourceDefinitionTestFactory setDfltSecret(Supplier<String> dfltSecretRef)
    {
        dfltSecret = dfltSecretRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltTransType(TransportType dfltTransTypeRef)
    {
        dfltTransType = dfltTransTypeRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltLayerStack(List<DeviceLayerKind> dfltLayerStackRef)
    {
        dfltLayerStack = dfltLayerStackRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltPeerSlots(Short dfltPeerSlotsRef)
    {
        dfltPeerSlots = dfltPeerSlotsRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltRscGroup(String dfltRscGroupNameRef)
    {
        dfltRscGroupName = dfltRscGroupNameRef;
        return this;
    }

    public ResourceDefinition createNext()
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, LinStorException
    {
        return builder().build();
    }

    public ResourceDefinition create(String rscName)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, LinStorException
    {
        return builder(rscName).build();
    }

    public ResourceDefinitionBuilder builder()
        throws LinStorException
    {
        return new ResourceDefinitionBuilder(dfltRscNameSupplier.get());
    }

    public ResourceDefinitionBuilder builder(String rscName)
        throws LinStorException
    {
        return new ResourceDefinitionBuilder(rscName);
    }

    public class ResourceDefinitionBuilder
    {
        private String rscName;
        private AccessContext accCtx;
        private byte[] extName;
        private Flags[] flags;
        private List<DeviceLayerKind> layerStack;
        private LayerPayload payload;
        private String rscGroupName;

        public ResourceDefinitionBuilder(String rscNameRef)
            throws LinStorException
        {
            rscName = rscNameRef;
            accCtx = dfltAccCtx;
            extName = dfltExtName;
            flags = dfltFlags;
            payload = TestFactoryUtils.createCopy(dfltPayload);
            layerStack = copyOrNull(dfltLayerStack);
            rscGroupName = dfltRscGroupName;

            payload.drbdRscDfn.sharedSecret = dfltSecret.get();
            payload.drbdRscDfn.transportType = dfltTransType;
            payload.drbdRscDfn.peerSlotsNewResource = dfltPeerSlots;
        }

        public ResourceDefinitionBuilder setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
            return this;
        }

        public ResourceDefinitionBuilder setAccCtx(AccessContext accCtxRef)
        {
            accCtx = accCtxRef;
            return this;
        }

        public ResourceDefinitionBuilder setExtName(byte[] extNameRef)
        {
            extName = extNameRef;
            return this;
        }

        public ResourceDefinitionBuilder setFlags(Flags[] flagsRef)
        {
            flags = flagsRef;
            return this;
        }

        public ResourceDefinitionBuilder setSecret(String secretRef)
        {
            payload.drbdRscDfn.sharedSecret = secretRef;
            return this;
        }

        public ResourceDefinitionBuilder setTransType(TransportType transTypeRef)
        {
            payload.drbdRscDfn.transportType = transTypeRef;
            return this;
        }

        public ResourceDefinitionBuilder setLayerStack(List<DeviceLayerKind> layerStackRef)
        {
            layerStack = layerStackRef;
            return this;
        }

        public ResourceDefinitionBuilder setPeerSlotsRef(Short peerSlotsRefRef)
        {
            payload.drbdRscDfn.peerSlotsNewResource = peerSlotsRefRef;
            return this;
        }

        public ResourceDefinitionBuilder setRscGroupName(String rscGrpNameRef)
        {
            rscGroupName = rscGrpNameRef;
            return this;
        }

        public ResourceDefinition build()
            throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException,
            ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, InvalidNameException,
            LinStorException
        {
            ResourceDefinition rscDfn = rscDfnFact.create(
                accCtx,
                new ResourceName(rscName),
                extName,
                flags,
                layerStack,
                payload,
                rscGrpFact.get(rscGroupName, true)
            );
            rscDfnMap.put(rscName.toUpperCase(), rscDfn);
            return rscDfn;
        }
    }
}

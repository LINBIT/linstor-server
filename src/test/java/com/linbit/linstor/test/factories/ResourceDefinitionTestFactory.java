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
    private AtomicInteger dfltPort = new AtomicInteger(9000);
    private Flags[] dfltFlags = new Flags[0];
    private Supplier<String> dfltSecret = () -> String.format(dfltSecretPattern, nextSecret.incrementAndGet());
    private TransportType dfltTransType = TransportType.IP;
    private List<DeviceLayerKind> dfltLayerStack = Collections.emptyList();
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
        throws LinStorException
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltRscNamePattern(String dfltRscNamePatternRef)
        throws LinStorException
    {
        dfltRscNamePattern = dfltRscNamePatternRef;
        return this;
    }

    public void setDfltRscNameSupplier(Supplier<String> dfltRscNameSupplierRef)
        throws LinStorException
    {
        dfltRscNameSupplier = dfltRscNameSupplierRef;
    }

    public ResourceDefinitionTestFactory setDfltExtName(byte[] dfltExtNameRef)
        throws LinStorException
    {
        dfltExtName = dfltExtNameRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltPort(AtomicInteger dfltPortRef)
        throws LinStorException
    {
        dfltPort = dfltPortRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltFlags(Flags[] dfltFlagsRef)
        throws LinStorException
    {
        dfltFlags = dfltFlagsRef;
        return this;
    }

    public void setDfltSecretPattern(String dfltSecretPatternRef)
        throws LinStorException
    {
        dfltSecretPattern = dfltSecretPatternRef;
    }

    public ResourceDefinitionTestFactory setDfltSecret(Supplier<String> dfltSecretRef)
        throws LinStorException
    {
        dfltSecret = dfltSecretRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltTransType(TransportType dfltTransTypeRef)
        throws LinStorException
    {
        dfltTransType = dfltTransTypeRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltLayerStack(List<DeviceLayerKind> dfltLayerStackRef)
        throws LinStorException
    {
        dfltLayerStack = dfltLayerStackRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltPeerSlots(Short dfltPeerSlotsRef)
        throws LinStorException
    {
        dfltPeerSlots = dfltPeerSlotsRef;
        return this;
    }

    public ResourceDefinitionTestFactory setDfltRscGroup(String dfltRscGroupNameRef)
        throws LinStorException
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
        private Integer port;
        private Flags[] flags;
        private String secret;
        private TransportType transType;
        private List<DeviceLayerKind> layerStack;
        private Short peerSlots;
        private String rscGroupName;

        public ResourceDefinitionBuilder(String rscNameRef)
            throws LinStorException
        {
            rscName = rscNameRef;
            accCtx = dfltAccCtx;
            extName = dfltExtName;
            port = dfltPort.getAndIncrement();
            flags = dfltFlags;
            secret = dfltSecret.get();
            transType = dfltTransType;
            layerStack = copyOrNull(dfltLayerStack);
            peerSlots = dfltPeerSlots;
            rscGroupName = dfltRscGroupName;
        }

        public ResourceDefinitionBuilder setRscName(String rscNameRef)
            throws LinStorException
        {
            rscName = rscNameRef;
            return this;
        }

        public ResourceDefinitionBuilder setAccCtx(AccessContext accCtxRef)
            throws LinStorException
        {
            accCtx = accCtxRef;
            return this;
        }

        public ResourceDefinitionBuilder setExtName(byte[] extNameRef)
            throws LinStorException
        {
            extName = extNameRef;
            return this;
        }

        public ResourceDefinitionBuilder setPort(Integer portRef)
            throws LinStorException
        {
            port = portRef;
            return this;
        }

        public ResourceDefinitionBuilder setFlags(Flags[] flagsRef)
            throws LinStorException
        {
            flags = flagsRef;
            return this;
        }

        public ResourceDefinitionBuilder setSecret(String secretRef)
            throws LinStorException
        {
            secret = secretRef;
            return this;
        }

        public ResourceDefinitionBuilder setTransType(TransportType transTypeRef)
            throws LinStorException
        {
            transType = transTypeRef;
            return this;
        }

        public ResourceDefinitionBuilder setLayerStack(List<DeviceLayerKind> layerStackRef)
            throws LinStorException
        {
            layerStack = layerStackRef;
            return this;
        }

        public ResourceDefinitionBuilder setPeerSlotsRef(Short peerSlotsRefRef)
            throws LinStorException
        {
            peerSlots = peerSlotsRefRef;
            return this;
        }

        public ResourceDefinitionBuilder setRscGroupName(String rscGrpNameRef)
            throws LinStorException
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
                port,
                flags,
                secret,
                transType,
                layerStack,
                peerSlots,
                rscGrpFact.get(rscGroupName, true)
            );
            rscDfnMap.put(rscName.toUpperCase(), rscDfn);
            return rscDfn;
        }

    }
}

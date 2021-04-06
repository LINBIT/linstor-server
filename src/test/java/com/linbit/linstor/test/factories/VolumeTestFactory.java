package com.linbit.linstor.test.factories;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.Volume.Flags;
import com.linbit.linstor.core.objects.VolumeControllerFactory;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

@Singleton
public class VolumeTestFactory
{
    private final VolumeControllerFactory vlmFact;
    private final ResourceDefinitionTestFactory rscDfnFact;
    private final VolumeDefinitionTestFactory vlmDfnFact;
    private final ResourceTestFactory rscFact;
    private final StorPoolTestFactory spFact;

    private final HashMap<Triple<String, String, Integer>, Volume> vlmMap = new HashMap<>();

    private AccessContext dfltAccCtx = TestAccessContextProvider.PUBLIC_CTX;
    private Flags[] dfltFlags = new Flags[0];
    private Map<String, String> dfltStorPoolMap = new TreeMap<>();
    private Long dfltVlmSize = null;

    @Inject
    public VolumeTestFactory(
        VolumeControllerFactory vlmFactRef,
        ResourceDefinitionTestFactory rscDfnFactRef,
        VolumeDefinitionTestFactory vlmDfnFactRef,
        ResourceTestFactory rscFactRef,
        StorPoolTestFactory spFactRef
    )
    {
        vlmFact = vlmFactRef;
        rscDfnFact = rscDfnFactRef;
        vlmDfnFact = vlmDfnFactRef;
        rscFact = rscFactRef;
        spFact = spFactRef;
    }

    public Volume get(String nodeName, String rscName, int vlmNr, boolean createIfNotExists)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, MdException, IllegalStorageDriverException,
        LinStorException
    {
        Volume vlm = vlmMap.get(new Triple<>(nodeName.toUpperCase(), rscName.toUpperCase(), vlmNr));
        if (vlm == null && createIfNotExists)
        {
            vlm = create(nodeName, rscName, vlmNr);
        }
        return vlm;
    }

    public VolumeTestFactory setDfltAccCtx(AccessContext dfltAccCtxRef)
        throws LinStorException
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public VolumeTestFactory setDfltFlags(Flags[] dfltFlagsRef)
        throws LinStorException
    {
        dfltFlags = dfltFlagsRef;
        return this;
    }

    public VolumeTestFactory setDfltStorPoolMap(Map<String, String> dfltStorPoolMapRef)
        throws LinStorException
    {
        dfltStorPoolMap = dfltStorPoolMapRef;
        return this;
    }

    public VolumeTestFactory setDfltStorPoolData(String spName)
        throws LinStorException
    {
        dfltStorPoolMap.put("", spName);
        return this;
    }

    public VolumeTestFactory setDfltVlmSize(Long vlmSizeRef)
        throws LinStorException
    {
        dfltVlmSize = vlmSizeRef;
        return this;
    }

    public Volume create(Node node, VolumeDefinition vlmDfn)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, MdException, IllegalStorageDriverException,
        LinStorException
    {
        return builder(node, vlmDfn).build();
    }

    public Volume create(String nodeName, String rscName)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, MdException, IllegalStorageDriverException,
        LinStorException
    {
        return builder(nodeName, rscName).build();
    }

    public Volume create(String nodeName, String rscName, int vlmNr)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, MdException, IllegalStorageDriverException,
        LinStorException
    {
        return builder(nodeName, rscName, vlmNr).build();
    }

    public VolumeBuilder builder(Node node, VolumeDefinition vlmDfn)
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException,
        IllegalStorageDriverException, InvalidNameException, LinStorException
    {
        return new VolumeBuilder(
            node.getName().displayValue,
            vlmDfn.getResourceDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().getValue()
        );
    }

    public VolumeBuilder builder(Node node, String rscName)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, IllegalStorageDriverException,
        LinStorException
    {
        return new VolumeBuilder(
            node.getName().displayValue,
            rscName,
            VolumeDefinitionTestFactory.getNextVlmNr(rscDfnFact.get(rscName, true))
        );
    }

    public VolumeBuilder builder(String nodeName, String rscName)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, ValueOutOfRangeException,
        ValueInUseException, ExhaustedPoolException, InvalidNameException, IllegalStorageDriverException,
        LinStorException
    {
        return new VolumeBuilder(
            nodeName,
            rscName,
            VolumeDefinitionTestFactory.getNextVlmNr(rscDfnFact.get(rscName, true))
        );
    }

    public VolumeBuilder builder(String nodeName, String rscName, int vlmNr)
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException,
        IllegalStorageDriverException, InvalidNameException, LinStorException
    {
        return new VolumeBuilder(nodeName, rscName, vlmNr);
    }

    public class VolumeBuilder
    {
        private String nodeName;
        private String rscName;
        private int vlmNr;

        private AccessContext accCtx;
        private Flags[] flags;
        private Map<String, StorPool> storPoolMap;
        private Long vlmSize;

        public VolumeBuilder(String nodeNameRef, String rscNameRef, int vlmNrRef) throws AccessDeniedException,
            DatabaseException, LinStorDataAlreadyExistsException, IllegalStorageDriverException, InvalidNameException,
            LinStorException
        {
            nodeName = nodeNameRef;
            rscName = rscNameRef;
            vlmNr = vlmNrRef;

            accCtx = dfltAccCtx;
            flags = dfltFlags;

            storPoolMap = new TreeMap<>();
            for (Entry<String, String> dfltStorPoolEntry : dfltStorPoolMap.entrySet())
            {
                storPoolMap.put(
                    dfltStorPoolEntry.getKey(),
                    spFact.get(nodeName, dfltStorPoolEntry.getValue(), true)
                );
            }
            vlmSize = dfltVlmSize;
        }

        public VolumeBuilder setNodeName(String nodeNameRef)
            throws LinStorException
        {
            nodeName = nodeNameRef;
            return this;
        }

        public VolumeBuilder setRscName(String rscNameRef)
            throws LinStorException
        {
            rscName = rscNameRef;
            return this;
        }

        public VolumeBuilder setVlmNr(int vlmNrRef)
            throws LinStorException
        {
            vlmNr = vlmNrRef;
            return this;
        }

        public VolumeBuilder setAccCtx(AccessContext accCtxRef)
            throws LinStorException
        {
            accCtx = accCtxRef;
            return this;
        }

        public VolumeBuilder setFlags(Flags[] flagsRef)
            throws LinStorException
        {
            flags = flagsRef;
            return this;
        }

        public VolumeBuilder setStorPoolMap(Map<String, StorPool> storPoolMapRef)
            throws LinStorException
        {
            storPoolMap = storPoolMapRef;
            return this;
        }

        public VolumeBuilder setStorPoolData(StorPool storPool)
            throws LinStorException
        {
            return putStorPool("", storPool);// data-storPool
        }

        public VolumeBuilder putStorPool(String key, StorPool sp)
            throws LinStorException
        {
            if (storPoolMap == null)
            {
                storPoolMap = new TreeMap<>();
            }
            if (key.equals(RscLayerSuffixes.SUFFIX_DRBD_META))
            {
                try
                {
                    // not perfect, but should work
                    vlmDfnFact.get(rscName, vlmNr, vlmSize, true).getProps(accCtx)
                        .setProp(
                            ApiConsts.KEY_STOR_POOL_DRBD_META_NAME,
                            sp.getName().displayValue
                        );
                }
                catch (Exception exc)
                {
                    throw new ImplementationError(exc);
                }
            }

            storPoolMap.put(key, sp);
            return this;
        }


        public Volume build()
            throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException,
            ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, InvalidNameException, MdException,
            LinStorException
        {
            Volume vlm = vlmFact.create(
                accCtx,
                rscFact.get(nodeName, rscName, true),
                vlmDfnFact.get(rscName, vlmNr, vlmSize, true),
                flags,
                storPoolMap,
                null
            );
            vlmMap.put(new Triple<>(nodeName.toUpperCase(), rscName.toUpperCase(), vlmNr), vlm);
            return vlm;
        }

    }
}

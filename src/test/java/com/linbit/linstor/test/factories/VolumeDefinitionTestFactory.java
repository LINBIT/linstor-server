package com.linbit.linstor.test.factories;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.NumberAlloc;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition.Flags;
import com.linbit.linstor.core.objects.VolumeDefinitionControllerFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Singleton
public class VolumeDefinitionTestFactory
{
    private final VolumeDefinitionControllerFactory vlmDfnFact;
    private final ResourceDefinitionTestFactory rscDfnFact;

    private final HashMap<Pair<String, Integer>, VolumeDefinition> vlmDfnMap = new HashMap<>();

    private final AtomicInteger nextMinor = new AtomicInteger(0);

    private AccessContext dfltAccCtx = TestAccessContextProvider.PUBLIC_CTX;
    private Supplier<Integer> dfltMinorNrSupplier = () -> nextMinor.incrementAndGet();
    private long dfltSize = 100 * 1024; // size in kib
    private Flags[] dfltFlags = new Flags[0];

    @Inject
    public VolumeDefinitionTestFactory(
        VolumeDefinitionControllerFactory vlmDfnFactRef,
        ResourceDefinitionTestFactory rscDfnFactRef
    )
    {
        vlmDfnFact = vlmDfnFactRef;
        rscDfnFact = rscDfnFactRef;
    }

    public VolumeDefinition get(String rscName, int vlmNr, Long vlmSize, boolean createIfNotExists)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, MdException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, InvalidNameException,
        LinStorException
    {
        VolumeDefinition vlmDfn = vlmDfnMap.get(new Pair<>(rscName.toUpperCase(), vlmNr));
        if (vlmDfn == null && createIfNotExists)
        {
            VolumeDefinitionBuilder builder = builder(rscName, vlmNr);
            if (vlmSize != null)
            {
                builder.size = vlmSize;
            }
            vlmDfn = builder.build();
        }
        return vlmDfn;
    }

    public VolumeDefinitionTestFactory setDfltAccCtx(AccessContext dfltAccCtxRef)
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public VolumeDefinitionTestFactory setDfltMinorNrSupplier(Supplier<Integer> dfltMinorNrSupplierRef)
    {
        dfltMinorNrSupplier = dfltMinorNrSupplierRef;
        return this;
    }

    public VolumeDefinitionTestFactory setDfltSize(long dfltSizeRef)
    {
        dfltSize = dfltSizeRef;
        return this;
    }

    public VolumeDefinitionTestFactory setDfltFlags(Flags[] dfltFlagsRef)
    {
        dfltFlags = dfltFlagsRef;
        return this;
    }

    public VolumeDefinition create(ResourceDefinition rscDfn)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, MdException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, InvalidNameException,
        LinStorException
    {
        return builder(rscDfn).build();
    }

    public VolumeDefinition create(String rscName, int vlmNr)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, MdException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, InvalidNameException,
        LinStorException
    {
        return builder(rscName, vlmNr).build();
    }

    public VolumeDefinitionBuilder builder(ResourceDefinition rscDfn)
        throws AccessDeniedException, DatabaseException, InvalidNameException, LinStorException
    {
        return new VolumeDefinitionBuilder(rscDfn.getName().displayValue, getNextVlmNr(rscDfn));
    }

    public VolumeDefinitionBuilder builder(ResourceDefinition rscDfn, int vlmNr)
        throws AccessDeniedException, DatabaseException, InvalidNameException, LinStorException
    {
        return new VolumeDefinitionBuilder(rscDfn.getName().displayValue, vlmNr);
    }

    public VolumeDefinitionBuilder builder(String rscName)
        throws AccessDeniedException, DatabaseException, InvalidNameException, LinStorDataAlreadyExistsException,
        ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, LinStorException
    {
        return new VolumeDefinitionBuilder(rscName, getNextVlmNr(rscDfnFact.get(rscName, true)));
    }

    public VolumeDefinitionBuilder builder(String rscName, int vlmNr)
        throws AccessDeniedException, DatabaseException, InvalidNameException, LinStorException
    {
        return new VolumeDefinitionBuilder(rscName, vlmNr);
    }

    public static int getNextVlmNr(ResourceDefinition rscDfn) throws LinStorException
    {
        int nextId;
        try
        {
            int[] occupied = new int[rscDfn.getVolumeDfnCount(TestAccessContextProvider.SYS_CTX)];
            int occupiedIdx = 0;
            Iterator<VolumeDefinition> itVlmDfn = rscDfn.iterateVolumeDfn(TestAccessContextProvider.SYS_CTX);
            while (itVlmDfn.hasNext())
            {
                VolumeDefinition vlmDfn = itVlmDfn.next();
                occupied[occupiedIdx++] = vlmDfn.getVolumeNumber().value;
            }
            Arrays.sort(occupied);
            nextId = NumberAlloc.getFreeNumber(occupied, VolumeNumber.VOLUME_NR_MIN, VolumeNumber.VOLUME_NR_MAX);
        }
        catch (AccessDeniedException | ExhaustedPoolException exc)
        {
            throw new ImplementationError(exc);
        }
        return nextId;
    }

    public class VolumeDefinitionBuilder
    {
        private AccessContext accCtx;
        private String rscName;
        private int vlmNr;
        private Integer minorNr;
        private Long size;
        private Flags[] flags;

        public VolumeDefinitionBuilder(String rscNameRef, int vlmNrRef)
            throws LinStorException
        {
            rscName = rscNameRef;
            vlmNr = vlmNrRef;

            accCtx = dfltAccCtx;
            minorNr = dfltMinorNrSupplier.get();
            size = dfltSize;
            flags = dfltFlags;
        }

        public void setAccCtx(AccessContext accCtxRef)
        {
            accCtx = accCtxRef;
        }

        public void setRscName(String rscNameRef)
        {
            rscName = rscNameRef;
        }

        public void setVlmNr(int vlmNrRef)
        {
            vlmNr = vlmNrRef;
        }

        public void setVlmNrToNext()
            throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException,
            ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, InvalidNameException,
            LinStorException
        {
            vlmNr = getNextVlmNr(rscDfnFact.get(rscName, true));
        }

        public void setMinorNr(Integer minorNrRef)
        {
            minorNr = minorNrRef;
        }

        public void setSize(Long sizeRef)
        {
            size = sizeRef;
        }

        public void setFlags(Flags[] flagsRef)
        {
            flags = flagsRef;
        }

        public VolumeDefinition build()
            throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, MdException,
            ValueOutOfRangeException, ValueInUseException, ExhaustedPoolException, InvalidNameException,
            LinStorException
        {
            VolumeDefinition vlmDfn = vlmDfnFact.create(
                accCtx,
                rscDfnFact.get(rscName, true),
                new VolumeNumber(vlmNr),
                minorNr,
                size,
                flags
            );
            vlmDfnMap.put(new Pair<>(rscName.toUpperCase(), vlmNr), vlmDfn);
            return vlmDfn;
        }
    }
}

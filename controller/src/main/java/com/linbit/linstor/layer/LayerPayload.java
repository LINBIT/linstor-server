package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.utils.Pair;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

public class LayerPayload
{
    public DrbdRscPayload drbdRsc;
    public DrbdRscDfnPayload drbdRscDfn;
    public DrbdVlmDfnPayload drbdVlmDfn;

    public Map<Integer, String> luksVlmPasswords;

    public Map<Pair<String, Integer>, StorageVlmPayload> storagePayload;

    public LayerPayload()
    {
        drbdRsc = new DrbdRscPayload();
        drbdRscDfn = new DrbdRscDfnPayload();
        drbdVlmDfn = new DrbdVlmDfnPayload();

        luksVlmPasswords = new TreeMap<>();

        storagePayload = new TreeMap<>();
    }

    public class DrbdRscPayload
    {
        public @Nullable Integer nodeId;
        public @Nullable Short peerSlots;
        public @Nullable Integer alStripes;
        public @Nullable Long alStripeSize;
        public boolean needsNewNodeId = false;
        public @Nullable Long rscFlags;
    }

    public class DrbdRscDfnPayload
    {
        public @Nullable Integer tcpPort;
        public @Nullable TransportType transportType;
        public @Nullable String sharedSecret;
        public @Nullable Short peerSlotsNewResource;
        public @Nullable Integer alStripes;
        public @Nullable Long alStripeSize;
    }

    public class DrbdVlmDfnPayload
    {
        public @Nullable Integer minorNr;
    }

    public DrbdRscPayload getDrbdRsc()
    {
        return drbdRsc;
    }

    public DrbdRscDfnPayload getDrbdRscDfn()
    {
        return drbdRscDfn;
    }

    public DrbdVlmDfnPayload getDrbdVlmDfn()
    {
        return drbdVlmDfn;
    }

    public class StorageVlmPayload
    {
        public StorPool storPool;

        public StorageVlmPayload(StorPool storPoolRef)
        {
            storPool = storPoolRef;
        }
    }

    public @Nullable StorageVlmPayload getStorageVlmPayload(String rscNameSuffix, int vlmNr)
    {
        return storagePayload.get(new Pair<>(rscNameSuffix, vlmNr));
    }

    public LayerPayload putStorageVlmPayload(String rscNameSuffix, int vlmNr, StorPool storPool)
    {
        storagePayload.put(new Pair<>(rscNameSuffix, vlmNr), new StorageVlmPayload(storPool));
        return this;
    }

    public LayerPayload extractFrom(ReadOnlyProps props)
    {
        return extractFrom(new PriorityProps(props));
    }

    public LayerPayload extractFrom(PriorityProps prioProps)
    {
        try
        {
            drbdRscDfn.peerSlotsNewResource = asShort(prioProps.getProp(ApiConsts.KEY_PEER_SLOTS_NEW_RESOURCE));
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return this;
    }

    private Short asShort(String str)
    {
        return parseNumber(Short::parseShort, str);
    }

    private <T> T parseNumber(Function<String, T> func, String str)
    {
        T ret = null;
        if (str != null)
        {
            ret = func.apply(str);
        }
        return ret;
    }
}

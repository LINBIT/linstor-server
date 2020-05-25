package com.linbit.linstor.layer.storage.openflex.rest;

import com.linbit.linstor.PriorityProps;

import static com.linbit.linstor.layer.storage.openflex.rest.OpenflexRestClient.API_HOST;
import static com.linbit.linstor.layer.storage.openflex.rest.OpenflexRestClient.API_PORT;
import static com.linbit.linstor.layer.storage.openflex.rest.OpenflexRestClient.STOR_DEV;
import static com.linbit.linstor.layer.storage.openflex.rest.OpenflexRestClient.STOR_POOL;
import static com.linbit.linstor.layer.storage.openflex.rest.OpenflexRestClient.get;

public class OpenflexUrlBuilder
{
    private OpenflexUrlBuilder()
    {
    }

    public static StringBuilder getProtocol(PriorityProps prioProps)
    {
        return new StringBuilder("http://"); // for now...
    }

    /**
     * @param prioPropsRef
     *
     * @return "${host}:${port}/Storage/Devices/${storDev}/"
     */
    public static StringBuilder getStorDevs(PriorityProps prioPropsRef)
    {
        return getProtocol(prioPropsRef)
            .append(get(prioPropsRef, API_HOST)).append(":")
            .append(get(prioPropsRef, API_PORT)).append("/")
            .append("Storage/Devices/")
            .append(get(prioPropsRef, STOR_DEV)).append("/");
    }

    /**
     * @param prioPropsRef
     *
     * @return {@link #getStorDevs()} + "Pools/${storPool}/"
     */
    public static StringBuilder getPool(PriorityProps prioPropsRef)
    {
        return getStorDevs(prioPropsRef)
            .append("Pools/")
            .append(get(prioPropsRef, STOR_POOL)).append("/");
    }

    public static StringBuilder getVolumeCollection(PriorityProps prioPropsRef)
    {
        return getStorDevs(prioPropsRef)
            .append("Volumes/");
    }

    public static StringBuilder getVolume(PriorityProps prioPropsRef, String vlmId)
    {
        return getVolumeCollection(prioPropsRef)
            .append(vlmId).append("/");
    }
}

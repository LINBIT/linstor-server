package com.linbit.linstor.prometheus;

import io.prometheus.client.Histogram;

public class LinstorControllerMetrics
{
    /**
     * Histogram for Client <-> Controller REST API requests.
     */
    public static final Histogram requestDurationHistogram = Histogram.build()
        .name("linstor_restapi_request_duration_seconds")
        .help("LINSTOR REST API Request duration in seconds")
        .unit("seconds")
        .buckets(LinstorServerMetrics.DEFAULT_BUCKETS)
        .labelNames("apicall")
        .register();
}

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
        .buckets(0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, Double.POSITIVE_INFINITY)
        .labelNames("apicall")
        .register();
}

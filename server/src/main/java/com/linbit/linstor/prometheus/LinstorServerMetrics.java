package com.linbit.linstor.prometheus;

import io.prometheus.client.Histogram;

public class LinstorServerMetrics
{
    /**
     * Histogram buckets in seconds. We have calls that should only take a few milliseconds, and calls that may
     * need multiple seconds, so we use a larger spread then the default buckets.
     */
    public static double[] DEFAULT_BUCKETS = new double[]{
        0.001, 0.0025, 0.005, 0.0075,
        0.01, 0.025, 0.05, 0.075,
        0.1, 0.25, 0.5, 0.75,
        1.0, 2.5, 5.0, 7.5,
        10.0, 25.0, 50.0,
        Double.POSITIVE_INFINITY
    };

    /**
     * Histogram for Controller <-> Satellite API calls.
     */
    public static final Histogram apiCallHistogram = Histogram.build()
        .name("linstor_apicall_duration_seconds")
        .help("LINSTOR internal API call duration in seconds")
        .unit("seconds")
        .buckets(DEFAULT_BUCKETS)
        .labelNames("apicall", "peer")
        .register();
}

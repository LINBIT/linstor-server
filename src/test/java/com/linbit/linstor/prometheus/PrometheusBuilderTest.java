package com.linbit.linstor.prometheus;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.logging.StderrErrorReporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class PrometheusBuilderTest
{

    @Test
    public void testNullMetrics() throws IOException {
        long start = System.currentTimeMillis();
        StderrErrorReporter errReporter = new StderrErrorReporter("Test");
        PrometheusBuilder pmb = new PrometheusBuilder(errReporter);

        final String promText = pmb.build(
                null,
                null,
                new ResourceList(),
                null,
                null,
                1,
                start);
        Assert.assertNotNull(promText);
        Assert.assertTrue(promText.contains("linstor_scrape_requests_count"));
    }

    @Test
    public void testSampleMetrics() throws IOException {
        long start = System.currentTimeMillis();
        StderrErrorReporter errReporter = new StderrErrorReporter("Test");
        PrometheusBuilder pmb = new PrometheusBuilder(errReporter);

        ArrayList<NodeApi> nodeList = new ArrayList<>();
        nodeList.add(
            new NodePojo(
                UUID.randomUUID(),
                "testnode",
                "SATELLITE",
                0,
                Collections.emptyList(),
                null,
                Collections.emptyList(),
                Collections.emptyMap(),
                ApiConsts.ConnectionStatus.ONLINE,
                null,
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                3
            )
        );

        final RscGrpPojo dfltRscGrp = new RscGrpPojo(
            UUID.randomUUID(),
            "DfltRscGrp",
            "",
            Collections.emptyMap(),
            Collections.emptyList(),
            null,
            null
        );

        ArrayList<ResourceDefinitionApi> rscDfns = new ArrayList<>();
        rscDfns.add(
            new RscDfnPojo(
                UUID.randomUUID(),
                dfltRscGrp,
                "testrsc",
                null,
                0,
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList())
        );

        final String promText = pmb.build(
            nodeList,
            rscDfns,
            new ResourceList(),
            null,
            null,
            1,
            start);
        Assert.assertNotNull(promText);
        Assert.assertTrue(promText.contains("linstor_scrape_requests_count"));
        Assert.assertTrue(promText.contains("linstor_node_state"));
        Assert.assertTrue(promText.contains("linstor_resource_definition_count 1.0"));
        Assert.assertTrue(promText.contains("linstor_node_reconnect_attempt_count"));
    }
}

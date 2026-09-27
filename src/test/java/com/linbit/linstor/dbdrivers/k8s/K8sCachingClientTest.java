package com.linbit.linstor.dbdrivers.k8s;

import java.net.HttpURLConnection;
import java.util.Collections;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class K8sCachingClientTest
{
    private static final String NAME = "d5719b801db29e976a14ca6a354cc17460452b0b126b0630f12ef9e1aa22552a";

    private MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>> client;
    private Resource<ConfigMap> resource;
    private K8sCachingClient<ConfigMap, ConfigMapList, Resource<ConfigMap>> cachingClient;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp()
    {
        client = mock(MixedOperation.class);
        resource = mock(Resource.class);
        ConfigMapList emptyList = new ConfigMapList();
        emptyList.setItems(Collections.emptyList());
        when(client.list()).thenReturn(emptyList);
        when(client.withName(NAME)).thenReturn(resource);
        cachingClient = new K8sCachingClient<>(client);
    }

    private static ConfigMap item(String resourceVersion)
    {
        return new ConfigMapBuilder()
            .withNewMetadata().withName(NAME).withResourceVersion(resourceVersion).endMetadata()
            .addToData("value", "false")
            .build();
    }

    private static KubernetesClientException httpError(int code)
    {
        return new KubernetesClientException("error " + code, code, null);
    }

    @Test
    public void createAdoptsObjectPersistedByRetriedRequest()
    {
        ConfigMap wanted = item(null);
        ConfigMap replaced = item("43");
        when(client.create(wanted)).thenThrow(httpError(HttpURLConnection.HTTP_CONFLICT));
        when(resource.get()).thenReturn(item("42"));
        when(client.replace(wanted)).thenReturn(replaced);

        assertSame(replaced, cachingClient.create(wanted));
        assertEquals("42", wanted.getMetadata().getResourceVersion());
        assertSame(replaced, cachingClient.get(NAME));
    }

    @Test
    public void createRethrowsConflictWhenObjectIsGone()
    {
        ConfigMap wanted = item(null);
        KubernetesClientException conflict = httpError(HttpURLConnection.HTTP_CONFLICT);
        when(client.create(wanted)).thenThrow(conflict);
        when(resource.get()).thenReturn(null);

        try
        {
            cachingClient.create(wanted);
            fail("expected the original conflict");
        }
        catch (KubernetesClientException exc)
        {
            assertSame(conflict, exc);
        }
        verify(client, never()).replace(any());
        assertEquals(null, cachingClient.get(NAME));
    }

    @Test
    public void createRethrowsOtherErrorsUnchanged()
    {
        ConfigMap wanted = item(null);
        KubernetesClientException serverError = httpError(HttpURLConnection.HTTP_INTERNAL_ERROR);
        when(client.create(wanted)).thenThrow(serverError);

        try
        {
            cachingClient.create(wanted);
            fail("expected the original error");
        }
        catch (KubernetesClientException exc)
        {
            assertSame(serverError, exc);
        }
        verify(client, never()).withName(any());
        verify(client, never()).replace(any());
    }
}

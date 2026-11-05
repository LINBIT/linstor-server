package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;

import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ResourceDefinitionsCloneResponseTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void errorsAreAnApiCallRcList() throws Exception
    {
        Response response = ResourceDefinitions.cloneStartedResponse(
            objectMapper,
            "src",
            "dst",
            ApiCallRcImpl.singleApiCallRc(ApiConsts.FAIL_UNKNOWN_ERROR, "(node B) Failed to access DRBD super-block")
        );

        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        JsonNode body = objectMapper.readTree((String) response.getEntity());
        assertTrue(body.toString(), body.isArray());
        assertEquals("(node B) Failed to access DRBD super-block", body.get(0).get("message").asText());
    }

    @Test
    public void notFoundIsA404List() throws Exception
    {
        Response response = ResourceDefinitions.cloneStartedResponse(
            objectMapper,
            "src",
            "dst",
            ApiCallRcImpl.singleApiCallRc(ApiConsts.FAIL_NOT_FOUND_RSC_DFN, "Resource definition 'src' not found.")
        );

        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertTrue(objectMapper.readTree((String) response.getEntity()).isArray());
    }

    @Test
    public void successIsTheCloneStartedObject() throws Exception
    {
        Response response = ResourceDefinitions.cloneStartedResponse(
            objectMapper,
            "src",
            "dst",
            ApiCallRcImpl.singleApiCallRc(ApiConsts.CREATED | ApiConsts.MASK_RSC_DFN, "Clone started")
        );

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        JsonNode body = objectMapper.readTree((String) response.getEntity());
        assertTrue(body.toString(), body.isObject());
        assertEquals("src", body.get("source_name").asText());
        assertEquals("dst", body.get("clone_name").asText());
        assertEquals("Clone started", body.get("messages").get(0).get("message").asText());
    }

    @Test
    public void anErrorAfterInfoEntriesStillListsAllEntries() throws Exception
    {
        ApiCallRcImpl rc = new ApiCallRcImpl();
        rc.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.MASK_INFO, "Suspend IO on clone source src"));
        rc.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, "No connection to satellite 'C'"));

        Response response = ResourceDefinitions.cloneStartedResponse(objectMapper, "src", "dst", rc);

        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        JsonNode body = objectMapper.readTree((String) response.getEntity());
        assertEquals(2, body.size());
        assertEquals("No connection to satellite 'C'", body.get(1).get("message").asText());
    }

    @Test
    public void noEntriesIsStillTheCloneStartedObject() throws Exception
    {
        Response response = ResourceDefinitions.cloneStartedResponse(objectMapper, "src", "", new ApiCallRcImpl());

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        JsonNode body = objectMapper.readTree((String) response.getEntity());
        assertEquals("__invalid_name__", body.get("clone_name").asText());
        // unchanged behaviour: with no entries the serializer leaves "messages" out
        assertTrue(body.toString(), body.get("messages") == null || body.get("messages").isEmpty());
    }
}

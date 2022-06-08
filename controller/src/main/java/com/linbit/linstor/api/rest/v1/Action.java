package com.linbit.linstor.api.rest.v1;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * This class is meant to contain endpoints that need to access multiple objects at once
 * (similar to GET-requests) but can't be added to the already existing path due to
 * possible name conflicts.
 * While GET-requests fitting these constraints should most likely be added to /v1/view,
 * this endpoint is for all the POST, PUT, and DELETE requests one might need.
 * (e.g. one DELETE endpoint to delete all snapshots at once)
 */

@Path("v1/action")
@Produces(MediaType.APPLICATION_JSON)
public class Action
{

}

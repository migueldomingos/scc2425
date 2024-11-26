package srv;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.MediaType;

/**
 * Class with control endpoints.
 */
@Path("/ctrl")
public class ControlResource
{

	private static final String USER = "user";

	/**
	 * This methods just prints a string. It may be useful to check if the current 
	 * version is running on Azure.
	 */
	@Path("/version/{" + USER + "}")
	@GET
	@Produces(MediaType.TEXT_HTML)
	public String version(@PathParam(USER) String userId) {
		var session = Authentication.validateSession(userId);
		
		var sb = new StringBuilder("<html>");

		sb.append("<p>version: 0001</p>");
		sb.append(String.format("<p>session:%s, user:%s</p>", session.uid(), session.uid()));
		
		System.getProperties().forEach( (k,v) -> {
			sb.append("<p><pre>").append(k).append("  =  ").append( v ).append("</pre></p>");
		});
		sb.append("</hmtl>");
		return sb.toString();
	}

	@Path("/version2/{" + USER + "}")
	@GET
	@Produces(MediaType.TEXT_HTML)
	public String version2( @CookieParam(Authentication.COOKIE_KEY) Cookie cookie, @PathParam(USER) String userId) {

		var session = Authentication.validateSession(cookie, userId);
		
		var sb = new StringBuilder("<html>");

		sb.append("<p>version: 0002</p>");
		sb.append(String.format("<p>session:%s, user:%s</p>", session.uid(), session.uid()));
		
		System.getProperties().forEach( (k,v) -> {
			sb.append("<p><pre>").append(k).append("  =  ").append( v ).append("</pre></p>");
		});
		sb.append("</hmtl>");
		return sb.toString();
	}

}

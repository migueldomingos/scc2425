package srv.auth;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;

import java.io.IOException;

@Provider
public class RequestCookiesFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext reqCtx) throws IOException {    	
    	RequestCookies.set( reqCtx.getCookies() );
    }
}

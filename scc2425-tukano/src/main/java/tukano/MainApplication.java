package tukano;

import jakarta.ws.rs.core.Application;
import tukano.impl.Token;
import tukano.impl.rest.RestBlobsResource;
import tukano.impl.rest.RestShortsResource;
import tukano.impl.rest.RestUsersResource;
import utils.Args;
import utils.Hibernate;
import utils.Props;

import java.util.HashSet;
import java.util.Set;

public class MainApplication extends Application {
    private Set<Object> singletons = new HashSet<>();

    private Set<Class<?>> resources = new HashSet<>();

    //public static String serverURI = "http://127.0.0.1:8080/project1_SCC/rest";
    public static String serverURI = "https://project1scc24256018360431.azurewebsites.net/rest";

    public MainApplication () {
        resources.add(RestBlobsResource.class);
        resources.add(RestShortsResource.class);
        resources.add(RestUsersResource.class);

        singletons.add(Hibernate.class);

        Token.setSecret(Args.valueOf("-secret", ""));
        Props.load("azurekeys-region.props");
    }

    @Override
    public Set<Class<?>> getClasses() {
        return resources;
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }
}

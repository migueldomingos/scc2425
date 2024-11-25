package srv.auth;

import jakarta.ws.rs.core.Cookie;

import java.util.Map;

public class RequestCookies {

    private static final ThreadLocal<Map<String, Cookie>> requestCookiesThreadLocal = new ThreadLocal<>();

    public static void set(Map<String, Cookie> cookies) {
    	requestCookiesThreadLocal.set(cookies);
    }

    public static  Map<String, Cookie> get() {
        return requestCookiesThreadLocal.get();
    }

    public static void clear() {
    	requestCookiesThreadLocal.remove();
    }
}

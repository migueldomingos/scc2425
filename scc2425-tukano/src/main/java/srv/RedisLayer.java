package srv;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import storageConnections.RedisCache;
import utils.JSON;

import java.util.logging.Logger;

public class RedisLayer {
	private final Jedis redisInstance = RedisCache.getCachePool().getResource();
	private static final String SESSION_CACHE_PREFIX = "session:";
	private static RedisLayer instance;
	private static final Logger Log = Logger.getLogger(RedisLayer.class.getName());

	synchronized public static RedisLayer getInstance() {
		if(instance == null ) {
			instance = new RedisLayer();
		}
		return instance;
	}

	
	public void putSession(Session s) {
		try {
			redisInstance.set(SESSION_CACHE_PREFIX + s.uid(), JSON.encode(s));
		} catch(JedisException e) {
			Log.warning("Redis access failed, unable to set cached session.");
		}
	}
	
	public Session getSession(String uid) {
		try {
			return JSON.decode(redisInstance.get(SESSION_CACHE_PREFIX + uid), Session.class);
		} catch(JedisException e) {
			Log.warning("Redis access failed, unable to set cached session.");
			return null;
		}
	}

}

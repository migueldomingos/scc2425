package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.*;

import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.azure.cosmos.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.util.CosmosPagedIterable;
import storageConnections.AzureCosmosDB_NoSQL;
import storageConnections.RedisCache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import tukano.api.Result;
import tukano.api.Shorts;
import tukano.api.User;
import tukano.api.Users;
import tukano.api.rest.RestShorts;
import utils.JSON;

public class JavaUsers implements Users {
	
	private static final Logger Log = Logger.getLogger(JavaUsers.class.getName());

	private static final String USER_CACHE_PREFIX = "user:";

	private static Users instance;

    private final CosmosContainer container;

	private final Shorts shorts;

	synchronized public static Users getInstance() {
		if( instance == null )
			instance = new JavaUsers();
		return instance;
	}
	
	private JavaUsers() {

		container = AzureCosmosDB_NoSQL.getContainer(Users.NAME);
		shorts = JavaShorts.getInstance();
	}
	
	@Override
	public Result<String> createUser(User user) {
		Log.info(() -> format("createUser : %s\n", user));

		if( badUserInfo( user ) )
				return error(BAD_REQUEST);

		return tryCatch(() -> {
			String userId = container.createItem(user).getItem().getUserId();
			cacheUser(user);
			return userId;
		});
	}

	@Override
	public Result<User> getUser(String userId, String pwd) {
		Log.info( () -> format("getUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null)
			return error(BAD_REQUEST);

		User user = getCachedUser(userId);
		if (user != null && user.getPwd().equals(pwd)) {
			return ok(user);
		}

		Result<User> userRes = tryCatch( () -> container.readItem(userId, new PartitionKey(userId), User.class).getItem());

		if (userRes.isOK()) {
			if (!userRes.value().getPwd().equals(pwd)) {
				return Result.error(FORBIDDEN);
			}
			cacheUser(userRes.value());
		}

		return userRes;
	}

	@Override
	public Result<User> updateUser(String userId, String pwd, User other) {
		Log.info(() -> format("updateUser : userId = %s, pwd = %s, user: %s\n", userId, pwd, other));

		if (badUpdateUserInfo(userId, pwd, other))
			return error(BAD_REQUEST);

		Result<User> oldUserResult = getUser(userId, pwd);
		if (!oldUserResult.isOK())
			return oldUserResult;

		if (other.getPwd() == null)
			other.setPwd(oldUserResult.value().getPwd());
		if (other.getEmail() == null)
			other.setEmail(oldUserResult.value().getEmail());
		if (other.getDisplayName() == null)
			other.setDisplayName(oldUserResult.value().getDisplayName());

		Result<User> updatedUserResult = tryCatch(() -> container.upsertItem(other).getItem());
		if (updatedUserResult.isOK()) {
			cacheUser(updatedUserResult.value());
		}

		return updatedUserResult;
	}

	@Override
	public Result<User> deleteUser(String userId, String pwd) {
		Log.info(() -> format("deleteUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null || pwd == null )
			return error(BAD_REQUEST);

		Result<User> oldUserResult = getUser(userId, pwd);
		if (!oldUserResult.isOK())
			return oldUserResult;

		Result<Object> result = tryCatch( () -> container.deleteItem(oldUserResult.value(), new CosmosItemRequestOptions()).getItem());

		if(result.isOK()){
			shorts.deleteAllShorts(userId, pwd, RestShorts.TOKEN);
			removeCachedUser(userId);

			return oldUserResult;
		}
		else
			return error(BAD_REQUEST);
	}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		Log.info(() -> format("searchUsers : patterns = %s\n", pattern));

		if (pattern == null || pattern.trim().isEmpty()) {
			return error(BAD_REQUEST);
		}

		String cacheKey = "user_search_" + pattern.toUpperCase();
		List<User> users;

		// Try to get the cached results from Redis
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			String cachedUsersJson = jedis.get(cacheKey);

			if (cachedUsersJson != null) {
				users = JSON.decode(cachedUsersJson, new TypeReference<List<User>>() {});
				if (users != null) {
					return ok(users);
				}
			}
		} catch (JedisException e) {
			Log.warning("Redis cache access failed, proceeding with database query.");
		}

		try {
			String query = format("SELECT * FROM User u WHERE CONTAINS(UPPER(u.userId), '%s')", pattern.toUpperCase());
			CosmosPagedIterable<User> results = container.queryItems(query, new CosmosQueryRequestOptions(), User.class);

			users = results.stream()
					.map(User::copyWithoutPassword)
					.collect(Collectors.toList());

			// Cache the results in Redis for future requests
			try (Jedis jedis = RedisCache.getCachePool().getResource()) {
				jedis.setex(cacheKey, 3600, JSON.encode(users));  // Cache with TTL of 1 hour (3600 seconds)
			} catch (JedisException e) {
				Log.warning("Failed to cache search results in Redis.");
			}

			return ok(users);
		} catch (CosmosException e) {
			return error(INTERNAL_ERROR);
		}
	}
	
	private boolean badUserInfo( User user) {
		return (user.userId() == null || user.pwd() == null || user.displayName() == null || user.email() == null);
	}
	
	private boolean badUpdateUserInfo( String userId, String pwd, User info) {
		return (userId == null || pwd == null || info.getUserId() != null && ! userId.equals( info.getUserId()));
	}

	private <T> Result<T> tryCatch( Supplier<T> supplierFunc) {
		try {
			return Result.ok(supplierFunc.get());
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error(INTERNAL_ERROR);
		}
	}

	private static Result.ErrorCode errorCodeFromStatus( int status ) {
		return switch( status ) {
			case 200 -> OK;
			case 404 -> NOT_FOUND;
			case 409 -> CONFLICT;
			default -> INTERNAL_ERROR;
		};
	}

	private void cacheUser(User user) {
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			jedis.set(USER_CACHE_PREFIX + user.getUserId(), JSON.encode(user));
		}
	}

	private User getCachedUser(String userId) {
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			String userJson = jedis.get(USER_CACHE_PREFIX + userId);
			return userJson != null ? JSON.decode(userJson, User.class) : null;
		}
	}

	private void removeCachedUser(String userId) {
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			jedis.del(USER_CACHE_PREFIX + userId);
		}
	}
}

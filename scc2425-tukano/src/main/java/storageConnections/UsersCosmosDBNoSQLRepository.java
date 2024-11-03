package storageConnections;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.core.type.TypeReference;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import tukano.api.Result;
import tukano.api.Shorts;
import tukano.api.User;
import tukano.api.Users;
import tukano.api.rest.RestShorts;
import tukano.impl.JavaShorts;
import tukano.impl.JavaUsers;
import utils.JSON;

import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.error;
import static tukano.api.Result.ok;

public class UsersCosmosDBNoSQLRepository implements UsersRepository{
    private static final Logger Log = Logger.getLogger(JavaUsers.class.getName());
    private static final String USER_CACHE_PREFIX = "user:";
    private final CosmosContainer container;
    private final Shorts shorts;

    public UsersCosmosDBNoSQLRepository() {
        container = AzureCosmosDB_NoSQL.getContainer(Users.NAME);
        shorts = JavaShorts.getInstance();
    }

    @Override
    public Result<String> createUser(User user) {
        return tryCatch(() -> {
            String userId = container.createItem(user).getItem().getid();
            System.out.println("User created with ID: " + userId);

            cacheUser(user);
            System.out.println("User cached successfully.");

            return userId;
        });
    }


    @Override
    public Result<User> getUser(String userId, String pwd) {
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
        Result<User> oldUserResult = getUser(userId, pwd);
        if (!oldUserResult.isOK())
            return oldUserResult;

        User newUser = oldUserResult.value().updateFrom(other);

        Result<User> updatedUserResult = tryCatch(() -> container.upsertItem(newUser).getItem());
        if (updatedUserResult.isOK()) {
            cacheUser(updatedUserResult.value());
        }

        return updatedUserResult;
    }

    @Override
    public Result<User> deleteUser(String userId, String pwd) {
        Result<User> oldUserResult = getUser(userId, pwd);
        if (!oldUserResult.isOK())
            return oldUserResult;

        Result<Object> result = tryCatch( () -> container.deleteItem(oldUserResult.value(), new CosmosItemRequestOptions()).getItem());

        if(result.isOK()){
            shorts.deleteAllShorts(userId, pwd, RestShorts.TOKEN);
            Log.info("fez o deleteAllShorts");
            removeCachedUser(userId);

            return oldUserResult;
        }
        else
            return error(BAD_REQUEST);
    }

    @Override
    public Result<List<User>> searchUsers(String pattern) {
        String cacheKey = "user_search_" + pattern.toUpperCase();
        List<User> users;

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
            String query = format("SELECT * FROM %s u WHERE UPPER(u.id) LIKE '%%%s%%'", container.getId(), pattern.toUpperCase());
            CosmosPagedIterable<User> results = container.queryItems(query, new CosmosQueryRequestOptions(), User.class);

            users = results.stream()
                    .map(User::copyWithoutPassword)
                    .collect(Collectors.toList());

            try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                jedis.setex(cacheKey, 3600, JSON.encode(users));  // TTL of 1 hour (3600 seconds)
            } catch (JedisException e) {
                Log.warning("Failed to cache search results in Redis.");
            }

            return ok(users);
        } catch (CosmosException e) {
            return error(INTERNAL_ERROR);
        }
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
            jedis.set(USER_CACHE_PREFIX + user.getid(), JSON.encode(user));
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

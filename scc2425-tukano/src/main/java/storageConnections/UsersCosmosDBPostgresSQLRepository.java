package storageConnections;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrResult;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.FORBIDDEN;

import java.util.List;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;
import tukano.api.Result;
import tukano.api.User;
import tukano.impl.JavaBlobs;
import tukano.impl.Token;
import utils.DB;
import tukano.impl.JavaShorts;
import utils.JSON;

public class UsersCosmosDBPostgresSQLRepository implements UsersRepository {

    private static final String USER_CACHE_PREFIX = "user:";

    public UsersCosmosDBPostgresSQLRepository() {}

    @Override
    public Result<String> createUser(User user) {
        return errorOrValue( DB.insertOne( user), user.getid() );
    }

    @Override
    public Result<User> getUser(String userId, String pwd) {

        User user = getCachedUser(userId);
        if (user != null && user.getPwd().equals(pwd)) {
            return ok(user);
        } else if (user != null && !user.getPwd().equals(pwd)) {
            return Result.error(FORBIDDEN);
        }

        Result<User> userResult = validatedUserOrError( DB.getOne( userId, User.class), pwd);

        if (userResult.isOK()) {
            cacheUser(userResult.value());
            System.out.println("User cached successfully.");
        }

        return userResult;
    }

    @Override
    public Result<User> updateUser(String userId, String pwd, User other) {

        Result<User> resUser = errorOrResult( validatedUserOrError(DB.getOne( userId, User.class), pwd), user -> DB.updateOne( user.updateFrom(other)));

        if (resUser.isOK())
            cacheUser(resUser.value());

        return resUser;
    }

    @Override
    public Result<User> deleteUser(String userId, String pwd) {
        return errorOrResult( validatedUserOrError(DB.getOne( userId, User.class), pwd), user -> {

            Executors.defaultThreadFactory().newThread( () -> {
                    JavaBlobs.getInstance().deleteAllBlobs(userId, Token.get(userId));
                    JavaShorts.getInstance().deleteAllShorts(userId, pwd, Token.get(userId));
                }).start();

            Result<User> userResult = DB.deleteOne( user);

            if (userResult.isOK()){
                removeCachedUser(userId);
            }

            return userResult;
        });
    }

    @Override
    public Result<List<User>> searchUsers(String pattern) {

        var query = format("SELECT * FROM Users u WHERE UPPER(u.id) LIKE '%%%s%%'", pattern.toUpperCase());
        var hits = DB.sql(query, User.class)
                .stream()
                .map(User::copyWithoutPassword)
                .toList();

        return ok(hits);
    }


    private Result<User> validatedUserOrError( Result<User> res, String pwd ) {
        if( res.isOK())
            return res.value().getPwd().equals( pwd ) ? res : error(FORBIDDEN);
        else
            return res;
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
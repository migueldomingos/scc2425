package storageConnections;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.ok;
import static utils.DB.getOne;

import java.util.List;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import tukano.api.Result;
import tukano.api.Short;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import utils.DB;
import tukano.impl.Token;
import tukano.impl.JavaBlobs;
import utils.JSON;

public class ShortsCosmosDBPostgresSQLRepository implements ShortsRepository {

    private static final Logger Log = Logger.getLogger(ShortsCosmosDBPostgresSQLRepository.class.getName());
    private static final String SHORT_CACHE_PREFIX = "short:";
    private static final String GETSHORTS_CACHE_PREFIX = "shorts_user:";
    private static final String FOLLOWERS_CACHE_PREFIX = "followers_user:";
    private static final String LIKES_CACHE_PREFIX = "likes_short:";

    public ShortsCosmosDBPostgresSQLRepository() {}

    @Override
    public Result<Short> createShort(Short shrt) {

        Result<Short> shortResult = errorOrValue(DB.insertOne(shrt), s -> s.copyWithLikes_And_Token(0));
        if (shortResult.isOK()){
            removeCachedShort(shrt.id(), shrt.ownerId());
        }

        return shortResult;
    }

    @Override
    public Result<Short> getShort(String shortId) {
        Short cachedShort = getCachedShort(shortId);
        if (cachedShort != null) {
            return ok(cachedShort.copyWithLikes_And_Token(cachedShort.totalLikes()));
        }

        var query = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
        var likes = DB.sql(query, Long.class);
        Result<Short> shortResult = errorOrValue( getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( likes.get(0)));

        if (shortResult.isOK()) {
            cacheShort(shortResult.value());
        }

        return shortResult;
    }

    @Override
    public Result<Void> deleteShort(Short shrt) {
        Result<Void> resultDelete = DB.transaction( hibernate -> {

            hibernate.remove( shrt);

            var query = format("DELETE FROM Likes l WHERE l.shortId = '%s'", shrt.getid());
            hibernate.createNativeQuery( query, Likes.class).executeUpdate();

            JavaBlobs.getInstance().delete(shrt.getBlobUrl(), Token.get());
        });

        if (resultDelete.isOK())
            removeCachedShort(shrt.getid(), shrt.ownerId());

        return resultDelete;
    }

    @Override
    public Result<List<String>> getShorts(String userId) {
        String cacheKey = GETSHORTS_CACHE_PREFIX + userId;
        List<String> shortIds = getCachedListFromCache(cacheKey);

        if (shortIds != null && !shortIds.isEmpty())
            return Result.ok(shortIds);

        var query = format("SELECT s.id FROM Shorts s WHERE s.ownerId = '%s'", userId);
        shortIds = DB.sql( query, String.class);

        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.setex(cacheKey, 3600, JSON.encode(shortIds));
        } catch (JedisException e) {
            Log.warning("Failed to store the results on Redis.");
        }

        return ok(shortIds);
    }

    @Override
    public Result<Void> follow(String userId1, String userId2, boolean isFollowing) {
        var f = new Following(userId1, userId2);
        Result<Following> followingResult = isFollowing ? DB.insertOne(f) : DB.deleteOne(f);

        if (followingResult.isOK()){
            try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                jedis.del(FOLLOWERS_CACHE_PREFIX + userId2);
            } catch (JedisException e) {
                Log.warning("Failed to remove cached Followers from Redis: " + e.getMessage());
            }
            return ok();
        }
        else
            return error(followingResult.error());

    }

    @Override
    public Result<List<String>> followers(String userId) {
        String cacheKey = FOLLOWERS_CACHE_PREFIX + userId;
        List<String> followers;

        followers = getCachedListFromCache(cacheKey);
        if (followers != null && !followers.isEmpty()) {
            return Result.ok(followers);
        }

        var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);


        followers = DB.sql(query, String.class);
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.setex(cacheKey, 3600, JSON.encode(followers));
        } catch (JedisException e) {
            Log.warning("Failed to store followers in Redis cache.");
        }
        return ok(followers);
    }

    @Override
    public Result<Void> like(String userId, boolean isliked, Short shrt) {
        var l = new Likes(userId, shrt.getid(), shrt.getOwnerId());
        Result<Likes> likesResult;
        if (isliked) {
            likesResult = DB.insertOne(l);
            if (likesResult.isOK()) {
                shrt.setTotalLikes(shrt.getTotalLikes() + 1);
                DB.updateOne(shrt);
                cacheShort(shrt);

                try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                    jedis.del(LIKES_CACHE_PREFIX + shrt.getid());
                } catch (JedisException e) {
                    Log.warning("Failed to store likes in Redis cache.");
                }
                return ok();
            }
            else
                return Result.error(likesResult.error());
        }
        else {
            likesResult = DB.deleteOne(l);
            if (likesResult.isOK()) {
                shrt.setTotalLikes(shrt.getTotalLikes() - 1);
                DB.updateOne(shrt);
                cacheShort(shrt);

                try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                    jedis.del(LIKES_CACHE_PREFIX + shrt.getid());
                } catch (JedisException e) {
                    Log.warning("Failed to store likes in Redis cache.");
                }
                return ok();
            }
            else
                return Result.error(likesResult.error());
        }
    }

    @Override
    public Result<List<String>> likes(String shortId) {
        String cacheKey = LIKES_CACHE_PREFIX + shortId;
        List<String> likedUserIds = getCachedListFromCache(cacheKey);
        if (likedUserIds != null && !likedUserIds.isEmpty()) {
            return ok(likedUserIds);
        }

        var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);

        likedUserIds = DB.sql(query, String.class);

        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.setex(cacheKey, 3600, JSON.encode(likedUserIds));
        } catch (JedisException e) {
            Log.warning("Failed to store likes in Redis cache.");
        }

        return ok(likedUserIds);
    }

    @Override
    public Result<List<String>> getFeed(String userId) {
        final var QUERY_FMT = """
				SELECT s.id, s.timestamp FROM Shorts s WHERE s.ownerId = '%s'
				UNION
				SELECT s.id, s.timestamp FROM Shorts s, Following f
					WHERE
						f.followee = s.ownerId AND f.follower = '%s'
				ORDER BY timestamp DESC""";

        return ok(DB.sql(format(QUERY_FMT, userId, userId), String.class));
    }

    @Override
    public Result<Void> deleteAllShorts(String userId) {
        return DB.transaction( (hibernate) -> {

            //delete shorts
            var query1 = format("DELETE FROM Shorts s WHERE s.ownerId = '%s'", userId);
            hibernate.createNativeQuery(query1, Short.class).executeUpdate();

            //delete follows
            var query2 = format("DELETE FROM Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);
            hibernate.createNativeQuery(query2, Following.class).executeUpdate();

            //delete likes
            var query3 = format("DELETE FROM Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);
            hibernate.createNativeQuery(query3, Likes.class).executeUpdate();

            invalidateCacheForUser(userId);
        });
    }

    private void cacheShort(Short shrt) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.set(SHORT_CACHE_PREFIX + shrt.getid(), JSON.encode(shrt));
        } catch (JedisException e) {
            Log.warning("Failed to cache short in Redis: " + e.getMessage());
        }
    }

    private Short getCachedShort(String shortId) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            String shortJson = jedis.get(SHORT_CACHE_PREFIX + shortId);
            return shortJson != null ? JSON.decode(shortJson, Short.class) : null;
        } catch (JedisException e) {
            Log.warning("Redis access failed, unable to retrieve cached short.");
            return null;
        }
    }

    private void removeCachedShort(String shortId, String userId) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.del(SHORT_CACHE_PREFIX + shortId);
            jedis.del(GETSHORTS_CACHE_PREFIX + userId);
        } catch (JedisException e) {
            Log.warning("Failed to remove cached short from Redis: " + e.getMessage());
        }
    }

    private List<String> getCachedListFromCache(String cacheKey) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            // Try retrieving from Redis cache
            String cachedJson = jedis.get(cacheKey);
            if (cachedJson != null) {
                List<String> cachedList = JSON.decode(cachedJson, new TypeReference<List<String>>() {});
                if (cachedList != null) {
                    return cachedList;
                }
            }

        } catch (JedisException e) {
            Log.warning("Failed to access or update Redis cache for key: " + cacheKey);
        }
        return null;
    }

    private void invalidateCacheForUser(String userId) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {

            // Invalida o cache dos shorts do usuário
            String shortsCacheKey = GETSHORTS_CACHE_PREFIX + userId;
            jedis.del(shortsCacheKey);

            // Invalida o cache dos seguidores do usuário
            String followersCacheKey = FOLLOWERS_CACHE_PREFIX + userId;
            jedis.del(followersCacheKey);

            // Obtém todos os shorts do usuário e invalida o cache de likes para cada um
            String queryUserShorts = format("SELECT s.id FROM shorts s WHERE s.ownerId = '%s'", userId);
            List<String> userShorts = DB.sql(queryUserShorts, String.class);
            userShorts.forEach(shrt -> {
                String likesCacheKey = LIKES_CACHE_PREFIX + shrt;
                jedis.del(likesCacheKey);
            });

            Log.info("Cache invalidated for user: " + userId);
        } catch (JedisException e) {
            Log.warning("Failed to invalidate cache for user " + userId + ": " + e.getMessage());
        }

    }

}
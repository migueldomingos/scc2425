package storageConnections;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.core.type.TypeReference;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import tukano.api.*;
import tukano.api.Short;
import tukano.api.rest.RestShorts;
import tukano.impl.JavaBlobs;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import utils.JSON;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.ok;

public class ShortsCosmosDBNoSQLRepository implements ShortsRepository {

    private static final Logger Log = Logger.getLogger(ShortsCosmosDBNoSQLRepository.class.getName());
    private final CosmosContainer container;
    private static final String SHORT_CACHE_PREFIX = "short:";

    public ShortsCosmosDBNoSQLRepository() {
        container = AzureCosmosDB_NoSQL.getContainer(Shorts.NAME);
    }

    @Override
    public Result<Short> createShort(Short shrt) {

        return tryCatch(() -> {
            Short createdShort = container.createItem(shrt).getItem();
            cacheShort(createdShort);
            return createdShort;
        });
    }

    @Override
    public Result<Short> getShort(String shortId) {
        Short cachedShort = getCachedShort(shortId);
        if (cachedShort != null) {
            return ok(cachedShort);
        }

        Result<Short> shortResult = tryCatch(() -> container.readItem(shortId, new PartitionKey(shortId), Short.class).getItem());

        if (shortResult.isOK()) {
            cacheShort(shortResult.value());
        }

        return shortResult;
    }

    @Override
    public Result<Void> deleteShort(Short shrt) {
        String queryLikesToDelete = format("SELECT * FROM Likes l WHERE l.shortId = '%s'", shrt.getShortId());
        CosmosPagedIterable<Likes> likesToDelete = container.queryItems(queryLikesToDelete, new CosmosQueryRequestOptions(), Likes.class);

        likesToDelete.forEach(like -> {
            tryCatch(() -> container.deleteItem(like, new CosmosItemRequestOptions()).getItem());
        });

        Result<Object> deleteResult = tryCatch(() -> container.deleteItem(shrt, new CosmosItemRequestOptions()).getItem());
        if (deleteResult.isOK()) {
            removeCachedShort(shrt.getShortId());
            JavaBlobs.getInstance().delete(shrt.getShortId(), RestShorts.TOKEN);
        }

        return deleteResult.isOK() ? Result.ok() : Result.error(deleteResult.error());
    }

    @Override
    public Result<List<String>> getShorts(String userId) {
        String cacheKey = "shorts_user:" + userId;
        List<String> shortIds;

        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            String cachedShortIdsJson = jedis.get(cacheKey);
            if (cachedShortIdsJson != null) {
                shortIds = JSON.decode(cachedShortIdsJson, new TypeReference<List<String>>() {});
                if (shortIds != null) {
                    return Result.ok(shortIds);
                }
            }
        } catch (JedisException e) {
            Log.warning("Failed to access Redis Cache.");
        }

        String query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
        shortIds = new ArrayList<>();

        try {
            CosmosPagedIterable<String> results = container.queryItems(query, new CosmosQueryRequestOptions(), String.class);
            results.forEach(shortIds::add);

            try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                jedis.setex(cacheKey, 3600, JSON.encode(shortIds));  // Cache com TTL de 1 hora (3600 segundos)
            } catch (JedisException e) {
                Log.warning("Failed to store the results on Redis.");
            }

            return Result.ok(shortIds);
        } catch (CosmosException e) {
            Log.warning("Error on querying the results: " + e.getMessage());
            return Result.error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> follow(String userId1, String userId2, boolean isFollowing) {

        Following f = new Following(userId1, userId2);

        if (isFollowing) {
            //we want to follow the person

            Result<Following> res = tryCatch( () -> container.createItem(f).getItem());
            if (res.isOK())
                return Result.ok();
            else
                return Result.error(res.error());
        } else {
            //we want to unfollow the person
            String cacheKey = "feed_user:" + userId1;
            try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                jedis.del(cacheKey);
            } catch (JedisException e) {
                Log.warning("Failed to delete feed in Redis cache.");
            }
            Result<Object> res = tryCatch( () -> container.deleteItem(f, new CosmosItemRequestOptions()).getItem());
            if (res.isOK())
                return Result.ok();
            else
                return Result.error(res.error());
        }
    }

    @Override
    public Result<List<String>> followers(String userId) {
        // Define the Redis cache key for followers
        String cacheKey = "followers_user:" + userId;
        List<String> followers;

        followers = getCachedListFromCache(cacheKey);
        if (followers != null) {
            return Result.ok(followers);
        }

        // Query CosmosDB if cache is empty
        String query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);
        followers = new ArrayList<>();

        try {
            CosmosPagedIterable<String> results = container.queryItems(query, new CosmosQueryRequestOptions(), String.class);
            results.forEach(followers::add);

            // Store in cache with a TTL of 1 hour
            try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                jedis.setex(cacheKey, 3600, JSON.encode(followers));
            } catch (JedisException e) {
                Log.warning("Failed to store followers in Redis cache.");
            }

            return Result.ok(followers);
        } catch (CosmosException e) {
            return Result.error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> like(String userId, boolean isLiked, Short shrt) {
        Likes l = new Likes(userId, shrt.getShortId(), shrt.getOwnerId());

        if (isLiked) {
            Result<Likes> res = tryCatch( () -> container.createItem(l).getItem());
            if (res.isOK())
                return Result.ok();
            else
                return Result.error(res.error());
        } else {
            Result<Object> res = tryCatch( () -> container.deleteItem(l, new CosmosItemRequestOptions()).getItem());
            if (res.isOK()){
                try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                    jedis.del("likes_short:" + shrt.getShortId());
                } catch (JedisException e) {
                    Log.warning("Failed to store likes in Redis cache.");
                }
                return Result.ok();
            }
            else
                return Result.error(res.error());
        }
    }

    @Override
    public Result<List<String>> likes(String shortId) {
        // Define the Redis cache key for likes
        String cacheKey = "likes_short:" + shortId;
        List<String> likedUserIds = getCachedListFromCache(cacheKey);
        if (likedUserIds != null) {
            return Result.ok(likedUserIds);
        }


        // Query CosmosDB if cache is empty
        String query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);
        likedUserIds = new ArrayList<>();

        try {
            CosmosPagedIterable<String> results = container.queryItems(query, new CosmosQueryRequestOptions(), String.class);
            results.forEach(likedUserIds::add);

            // Store in cache with a TTL of 1 hour
            try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                jedis.setex(cacheKey, 3600, JSON.encode(likedUserIds));
            } catch (JedisException e) {
                Log.warning("Failed to store likes in Redis cache.");
            }

            return Result.ok(likedUserIds);
        } catch (CosmosException e) {
            return Result.error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<List<String>> getFeed(String userId) {
        // Define the Redis cache key for the feed
        String cacheKey = "feed_user:" + userId;
        List<String> feedShortIds = getCachedListFromCache(cacheKey);
        if (feedShortIds != null) {
            return Result.ok(feedShortIds);
        }

        // Get following user IDs from followers method (not cached individually here)
        List<String> followingUserIds = followers(userId).value();

        // Query feed if following list is not empty
        feedShortIds = new ArrayList<>();
        if (!followingUserIds.isEmpty()) {
            String feedQuery = format(
                    "SELECT s.shortId FROM Short s WHERE ARRAY_CONTAINS(%s, s.ownerId) ORDER BY s.creationDate DESC",
                    followingUserIds
            );

            try {
                CosmosPagedIterable<String> feedResults = container.queryItems(feedQuery, new CosmosQueryRequestOptions(), String.class);
                feedResults.forEach(feedShortIds::add);

                // Store feed in cache with a TTL of 1 hour
                try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                    jedis.setex(cacheKey, 3600, JSON.encode(feedShortIds));
                } catch (JedisException e) {
                    Log.warning("Failed to store feed in Redis cache.");
                }
            } catch (CosmosException e) {
                Log.warning("Error consulting the feed shorts: " + e.getMessage());
                return Result.error(INTERNAL_ERROR);
            }
        }

        return Result.ok(feedShortIds);
    }

    @Override
    public Result<Void> deleteAllShorts(String userId) {
        invalidateCacheForUser(userId);

        //delete shorts
        String queryDeleteShorts = format("SELECT * FROM Short s WHERE s.ownerId = '%s'", userId);
        CosmosPagedIterable<Short> shorts = container.queryItems(queryDeleteShorts, new CosmosQueryRequestOptions(), Short.class);
        shorts.forEach(shrt -> {
            tryCatch( () -> container.deleteItem(shrt, new CosmosItemRequestOptions()));
            JavaBlobs.getInstance().delete(shrt.getShortId(), RestShorts.TOKEN);
        });

        //delete follows
        String queryDeleteFollows = format("SELECT * FROM Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);
        CosmosPagedIterable<Following> follows = container.queryItems(queryDeleteFollows, new CosmosQueryRequestOptions(), Following.class);
        follows.forEach(follow -> tryCatch( () -> container.deleteItem(follow, new CosmosItemRequestOptions())));

        //delete likes
        String queryDeleteLikes = format("SELECT * FROM Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);
        CosmosPagedIterable<Likes> likes = container.queryItems(queryDeleteLikes, new CosmosQueryRequestOptions(), Likes.class);
        likes.forEach(like -> tryCatch( () -> container.deleteItem(like, new CosmosItemRequestOptions())));



        return Result.ok();
    }

    // Métodos auxiliares de cache

    private void invalidateCacheForUser(String userId) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            // Invalida o cache dos shorts do usuário
            String shortsCacheKey = "shorts_user:" + userId;
            jedis.del(shortsCacheKey);

            // Invalida o cache dos seguidores do usuário
            String followersCacheKey = "followers_user:" + userId;
            jedis.del(followersCacheKey);

            // Invalida o cache do feed do usuário
            String feedCacheKey = "feed_user:" + userId;
            jedis.del(feedCacheKey);

            // Obtém todos os shorts do usuário e invalida o cache de likes para cada um
            String queryUserShorts = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
            CosmosPagedIterable<String> userShorts = container.queryItems(queryUserShorts, new CosmosQueryRequestOptions(), String.class);
            userShorts.forEach(shortId -> {
                String likesCacheKey = "likes_short:" + shortId;
                jedis.del(likesCacheKey);
            });

            Log.info("Cache invalidated for user: " + userId);
        } catch (JedisException e) {
            Log.warning("Failed to invalidate cache for user " + userId + ": " + e.getMessage());
        }

    }

    private void cacheShort(Short shrt) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.set(SHORT_CACHE_PREFIX + shrt.getShortId(), JSON.encode(shrt));
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

    private void removeCachedShort(String shortId) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.del(SHORT_CACHE_PREFIX + shortId);
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
}

package storageConnections;

import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.ok;

import com.fasterxml.jackson.core.type.TypeReference;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import tukano.api.Result;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import tukano.api.Short;
import tukano.api.rest.RestShorts;
import tukano.impl.JavaBlobs;
import utils.JSON;

public class ShortsCosmosDBPostgresSQLRepository implements ShortsRepository{

    private static final Logger Log = Logger.getLogger(ShortsCosmosDBPostgresSQLRepository.class.getName());
    private static final String SHORT_CACHE_PREFIX = "short:";
    private static final String FOLLOWERS_CACHE_PREFIX = "followers_user:";
    private final Connection connection;


    public ShortsCosmosDBPostgresSQLRepository() {
        try {
            connection = AzureCosmosDB_PostgresSQL.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<Short> createShort(Short shrt) {
        String insertSQL = "INSERT INTO shorts (short_id, user_id, blob_url) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setString(1, shrt.getid());
            pstmt.setString(2, shrt.getOwnerId());
            pstmt.setString(3, shrt.getBlobUrl());
            pstmt.executeUpdate();
            cacheShort(shrt);
            return ok(shrt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<Short> getShort(String shortId) {
        Short cachedShort = getCachedShort(shortId);
        if (cachedShort != null) {
            return ok(cachedShort);
        }

        String querySQL = "SELECT * FROM shorts WHERE short_id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
            pstmt.setString(1, shortId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Short shrt = new Short(
                            rs.getString("short_id"),
                            rs.getString("user_id"),
                            rs.getString("blob_url")
                    );
                    cacheShort(shrt);
                    return ok(shrt);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Result.error(NOT_FOUND);

    }

    @Override
    public Result<Void> deleteShort(Short shrt) {
        String deleteLikesSQL = "DELETE FROM likes WHERE short_id = ?";
        String deleteShortSQL = "DELETE FROM shorts WHERE short_id = ?";

        try (PreparedStatement pstmtLikes = connection.prepareStatement(deleteLikesSQL);
             PreparedStatement pstmtShort = connection.prepareStatement(deleteShortSQL)) {
            pstmtLikes.setString(1, shrt.id());
            pstmtLikes.executeUpdate();

            pstmtShort.setString(1, shrt.id());
            pstmtShort.executeUpdate();

            removeCachedShort(shrt.id());
            JavaBlobs.getInstance().delete(shrt.id(), RestShorts.TOKEN);
            return ok();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<List<String>> getShorts(String userId) {
        String cacheKey = "shorts_user:" + userId;
        List<String> shortIds = getCachedListFromCache(cacheKey);

        if (shortIds != null) {
            return ok(shortIds);
        }

        String querySQL = "SELECT short_id FROM shorts WHERE user_id = ?";
        List<String> finalShortIds = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
            pstmt.setString(1, userId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    finalShortIds.add(rs.getString("short_id"));
                }
            }
            cacheListInCache(cacheKey, finalShortIds);
            return ok(finalShortIds);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<Void> follow(String userId1, String userId2, boolean isFollowing) {
        String sql = isFollowing ? "INSERT INTO following (follower, followee) VALUES (?, ?)"
                : "DELETE FROM following WHERE follower = ? AND followee = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, userId1);
            pstmt.setString(2, userId2);
            pstmt.executeUpdate();
            unfollowCacheForUser(userId1);
            return ok(null);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<List<String>> followers(String userId) {
        String cacheKey = "followers_user:" + userId;
        List<String> followers = getCachedListFromCache(cacheKey);
        if (followers != null) {
            return ok(followers);
        }

        String querySQL = "SELECT follower FROM following WHERE followee = ?";
        List<String> finalFollowers = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
            pstmt.setString(1, userId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    finalFollowers.add(rs.getString("follower"));
                }
            }
            cacheListInCache(cacheKey, finalFollowers);
            return ok(finalFollowers);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<Void> like(String userId, boolean isliked, Short shrt) {
        String sql = isliked ? "INSERT INTO likes (user_id, short_id, owner_id) VALUES (?, ?, ?)"
                : "DELETE FROM likes WHERE user_id = ? AND short_id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            pstmt.setString(2, shrt.getid());
            if (isliked) {
                pstmt.setString(3, shrt.getOwnerId());
            }
            pstmt.executeUpdate();

            try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                jedis.del("likes_short:" + shrt.getid());
            } catch (JedisException e) {
                Log.warning("Failed to update Redis cache.");
            }
            return ok(null);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<List<String>> likes(String shortId) {
        String cacheKey = "likes_short:" + shortId;
        List<String> likedUserIds = getCachedListFromCache(cacheKey);
        if (likedUserIds != null) {
            return ok(likedUserIds);
        }

        String querySQL = "SELECT user_id FROM likes WHERE short_id = ?";
        List<String> finalLikedUserIds = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
            pstmt.setString(1, shortId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    finalLikedUserIds.add(rs.getString("user_id"));
                }
            }
            cacheListInCache(cacheKey, finalLikedUserIds);
            return ok(finalLikedUserIds);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<List<String>> getFeed(String userId) {
        String cacheKey = "feed_user:" + userId;
        List<String> feed = getCachedListFromCache(cacheKey);
        if (feed != null) {
            return ok(feed);
        }

        String queryFeedSQL = """
            SELECT short_id FROM shorts
            WHERE user_id IN (SELECT followee FROM following WHERE follower = ?)
            ORDER BY created_at DESC
        """;
        List<String> finalFeed = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(queryFeedSQL)) {
            pstmt.setString(1, userId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    finalFeed.add(rs.getString("short_id"));
                }
            }
            cacheListInCache(cacheKey, finalFeed);
            return ok(finalFeed);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<Void> deleteAllShorts(String userId) {
        String deleteLikesSQL = "DELETE FROM likes WHERE short_id IN (SELECT short_id FROM shorts WHERE user_id = ?)";
        String deleteShortsSQL = "DELETE FROM shorts WHERE user_id = ?";
        String deleteFollowingAsFollowerSQL = "DELETE FROM following WHERE follower = ?";
        String deleteFollowingAsFolloweeSQL = "DELETE FROM following WHERE followee = ?";

        try (PreparedStatement pstmtLikes = connection.prepareStatement(deleteLikesSQL);
             PreparedStatement pstmtShorts = connection.prepareStatement(deleteShortsSQL);
             PreparedStatement pstmtFollowingAsFollower = connection.prepareStatement(deleteFollowingAsFollowerSQL);
             PreparedStatement pstmtFollowingAsFollowee = connection.prepareStatement(deleteFollowingAsFolloweeSQL)) {

            pstmtLikes.setString(1, userId);
            pstmtLikes.executeUpdate();

            pstmtShorts.setString(1, userId);
            pstmtShorts.executeUpdate();

            pstmtFollowingAsFollower.setString(1, userId);
            pstmtFollowingAsFollower.executeUpdate();

            pstmtFollowingAsFollowee.setString(1, userId);
            pstmtFollowingAsFollowee.executeUpdate();

            invalidateCacheForUser(userId);
            return ok();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    //Auxiliary Methods

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

    private void cacheListInCache(String cacheKey, List<String> list) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.setex(cacheKey, 3600, JSON.encode(list));
        } catch (JedisException e) {
            Log.warning("Failed to cache list in Redis: " + e.getMessage());
        }
    }

    private void unfollowCacheForUser(String userId) {
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.del("feed_user:" + userId);
            jedis.del(FOLLOWERS_CACHE_PREFIX + userId);
        } catch (JedisException e) {
            Log.warning("Failed to invalidate user cache in Redis: " + e.getMessage());
        }
    }

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
            String queryUserShortsSQL = "SELECT short_id FROM shorts WHERE user_id = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(queryUserShortsSQL)) {
                pstmt.setString(1, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String shortId = rs.getString("short_id");
                        String likesCacheKey = "likes_short:" + shortId;
                        jedis.del(likesCacheKey);
                    }
                }
            }

            Log.info("Cache invalidated for user: " + userId);
        } catch (JedisException e) {
            Log.warning("Failed to invalidate cache for user " + userId + ": " + e.getMessage());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

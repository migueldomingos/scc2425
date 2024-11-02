package storageConnections;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.ok;

import com.fasterxml.jackson.core.type.TypeReference;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import tukano.api.Result;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Logger;

import tukano.api.Short;
import tukano.impl.JavaBlobs;
import tukano.impl.Token;
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

        return tryCatch(() -> {
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                pstmt.setString(1, shrt.getShortId());
                pstmt.setString(2, shrt.getOwnerId());
                pstmt.setString(3, shrt.getBlobUrl());
                pstmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            cacheShort(shrt);
            return shrt;
        });
    }

    @Override
    public Result<Short> getShort(String shortId) {
        Short cachedShort = getCachedShort(shortId);
        if (cachedShort != null) {
            return ok(cachedShort);
        }

        String querySQL = "SELECT * FROM shorts WHERE short_id = ?";

        return tryCatch(() -> {
            Short shrt;
            try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
                pstmt.setString(1, shortId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        shrt = new Short(
                                rs.getString("short_id"),
                                rs.getString("user_id"),
                                rs.getString("blob_url")
                        );
                        cacheShort(shrt);
                    } else {
                        return null;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return shrt;
        });

    }

    @Override
    public Result<Void> deleteShort(Short shrt) {
        String deleteLikesSQL = "DELETE FROM likes WHERE short_id = ?";
        String deleteShortSQL = "DELETE FROM shorts WHERE short_id = ?";

        return tryCatch(() -> {
            try (PreparedStatement pstmtLikes = connection.prepareStatement(deleteLikesSQL);
                 PreparedStatement pstmtShort = connection.prepareStatement(deleteShortSQL)) {
                pstmtLikes.setString(1, shrt.getShortId());
                pstmtLikes.executeUpdate();

                pstmtShort.setString(1, shrt.getShortId());
                pstmtShort.executeUpdate();

                removeCachedShort(shrt.getShortId());
                JavaBlobs.getInstance().delete(shrt.getShortId(), Token.get(shrt.getShortId()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    @Override
    public Result<List<String>> getShorts(String userId) {
        String cacheKey = "shorts_user:" + userId;
        List<String> shortIds = getCachedListFromCache(cacheKey);

        if (shortIds != null) {
            return Result.ok(shortIds);
        }

        String querySQL = "SELECT short_id FROM shorts WHERE user_id = ?";
        shortIds = new ArrayList<>();

        List<String> finalShortIds = shortIds;
        return tryCatch(() -> {
            try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
                pstmt.setString(1, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        finalShortIds.add(rs.getString("short_id"));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            cacheListInCache(cacheKey, finalShortIds);
            return finalShortIds;
        });
    }

    @Override
    public Result<Void> follow(String userId1, String userId2, boolean isFollowing) {
        String insertSQL = "INSERT INTO following (follower, followee) VALUES (?, ?)";
        String deleteSQL = "DELETE FROM following WHERE follower = ? AND followee = ?";

        return tryCatch(() -> {
            try (PreparedStatement pstmt = connection.prepareStatement(isFollowing ? insertSQL : deleteSQL)) {
                pstmt.setString(1, userId1);
                pstmt.setString(2, userId2);
                pstmt.executeUpdate();
                unfollowCacheForUser(userId1);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    @Override
    public Result<List<String>> followers(String userId) {
        String cacheKey = "followers_user:" + userId;
        List<String> followers = getCachedListFromCache(cacheKey);
        if (followers != null) {
            return Result.ok(followers);
        }

        String querySQL = "SELECT follower FROM following WHERE followee = ?";
        followers = new ArrayList<>();

        List<String> finalFollowers = followers;
        return tryCatch(() -> {
            try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
                pstmt.setString(1, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        finalFollowers.add(rs.getString("follower"));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            cacheListInCache(cacheKey, finalFollowers);
            return finalFollowers;
        });
    }

    @Override
    public Result<Void> like(String userId, boolean isliked, Short shrt) {
        String insertLikeSQL = "INSERT INTO likes (user_id, short_id, owner_id) VALUES (?, ?, ?)";
        String deleteLikeSQL = "DELETE FROM likes WHERE user_id = ? AND short_id = ?";

        return tryCatch(() -> {
            try (PreparedStatement pstmt = connection.prepareStatement(isliked ? insertLikeSQL : deleteLikeSQL)) {
                pstmt.setString(1, userId);
                pstmt.setString(2, shrt.getShortId());
                pstmt.setString(3, shrt.getOwnerId());
                pstmt.executeUpdate();
                if (isliked) {
                    pstmt.executeUpdate();
                    Log.info(() -> format("Curtida adicionada para shortId = %s pelo userId = %s\n", shrt.getShortId(), userId));
                } else {
                    pstmt.executeUpdate();
                    Log.info(() -> format("Curtida removida para shortId = %s pelo userId = %s\n", shrt.getShortId(), userId));
                }

                try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                    jedis.del("likes_short:" + shrt.getShortId());
                } catch (JedisException e) {
                    Log.warning("Failed to store likes in Redis cache.");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    @Override
    public Result<List<String>> likes(String shortId) {
        String cacheKey = "likes_short:" + shortId;
        List<String> likedUserIds = getCachedListFromCache(cacheKey);
        if (likedUserIds != null) {
            return Result.ok(likedUserIds);
        }

        String querySQL = "SELECT user_id FROM likes WHERE short_id = ?";
        likedUserIds = new ArrayList<>();

        List<String> finalLikedUserIds = likedUserIds;
        return tryCatch(() -> {
            try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
                pstmt.setString(1, shortId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        finalLikedUserIds.add(rs.getString("user_id"));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            // Cache the list of user IDs for 1 hour
            cacheListInCache(cacheKey, finalLikedUserIds);
            return finalLikedUserIds;
        });
    }

    @Override
    public Result<List<String>> getFeed(String userId) {
        String cacheKey = "feed_user:" + userId;
        List<String> feed = getCachedListFromCache(cacheKey);
        if (feed != null) {
            return Result.ok(feed);
        }

        String queryFeedSQL = """
            SELECT short_id FROM shorts
            WHERE user_id IN (SELECT followee FROM following WHERE follower = ?)
            ORDER BY created_at DESC
        """;

        feed = new ArrayList<>();

        List<String> finalFeed = feed;
        return tryCatch(() -> {
            try (PreparedStatement pstmt = connection.prepareStatement(queryFeedSQL)) {
                pstmt.setString(1, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        finalFeed.add(rs.getString("short_id"));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            cacheListInCache(cacheKey, finalFeed);
            return finalFeed;
        });
    }

    @Override
    public Result<Void> deleteAllShorts(String userId) {
        String deleteLikesSQL = "DELETE FROM likes WHERE short_id IN (SELECT short_id FROM shorts WHERE user_id = ?)";
        String deleteShortsSQL = "DELETE FROM shorts WHERE user_id = ?";
        String deleteFollowingAsFollowerSQL = "DELETE FROM following WHERE follower = ?";
        String deleteFollowingAsFolloweeSQL = "DELETE FROM following WHERE followee = ?";

        return tryCatch(() -> {
            try (PreparedStatement pstmtLikes = connection.prepareStatement(deleteLikesSQL);
                 PreparedStatement pstmtShorts = connection.prepareStatement(deleteShortsSQL);
                 PreparedStatement pstmtFollowingAsFollower = connection.prepareStatement(deleteFollowingAsFollowerSQL);
                 PreparedStatement pstmtFollowingAsFollowee = connection.prepareStatement(deleteFollowingAsFolloweeSQL)) {

                // Delete all likes associated with the user's shorts
                pstmtLikes.setString(1, userId);
                pstmtLikes.executeUpdate();

                // Delete all shorts associated with the user
                pstmtShorts.setString(1, userId);
                pstmtShorts.executeUpdate();

                // Delete all following relationships where the user is the follower
                pstmtFollowingAsFollower.setString(1, userId);
                pstmtFollowingAsFollower.executeUpdate();

                // Delete all following relationships where the user is the followee
                pstmtFollowingAsFollowee.setString(1, userId);
                pstmtFollowingAsFollowee.executeUpdate();

                // Invalidate related cache entries
                invalidateCacheForUser(userId);
                JavaBlobs.getInstance().deleteAllBlobs(userId, Token.get(userId));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }


    //Auxiliary Methods

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
            String queryUserShorts = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
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

    private <T> Result<T> tryCatch(Supplier<T> supplierFunc) {
        try {
            return Result.ok(supplierFunc.get());
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }
}

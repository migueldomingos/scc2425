package storageConnections;

import redis.clients.jedis.Jedis;
import tukano.api.Result;
import tukano.api.Shorts;
import tukano.api.User;
import tukano.api.rest.RestShorts;
import tukano.impl.JavaShorts;
import tukano.impl.Token;
import utils.JSON;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import com.fasterxml.jackson.core.type.TypeReference;

public class UsersCosmosDBPostgresSQLRepository implements UsersRepository {

    private final Connection connection;
    private static final Logger Log = Logger.getLogger(UsersCosmosDBPostgresSQLRepository.class.getName());
    private static final String USER_CACHE_PREFIX = "user:";
    private final Shorts shorts;


    public UsersCosmosDBPostgresSQLRepository() {
        try {
            shorts = JavaShorts.getInstance();
            connection = AzureCosmosDB_PostgresSQL.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<String> createUser(User user) {
        String insertSQL = "INSERT INTO users (user_id, pwd, email, display_name) VALUES (?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setString(1, user.getUserId());
            pstmt.setString(2, user.getPwd());
            pstmt.setString(3, user.getEmail());
            pstmt.setString(4, user.getDisplayName());
            pstmt.executeUpdate();

            cacheUser(user);  // Cache the user after creation
            Log.info("User created successfully: " + user.getUserId());
            return Result.ok(user.getUserId());
        } catch (SQLException e) {
            Log.warning("Failed to create user: " + e.getMessage());
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<User> getUser(String userId, String pwd) {
        // Check cache first
        User cachedUser = getCachedUser(userId);
        if (cachedUser != null && cachedUser.getPwd().equals(pwd)) {
            return Result.ok(cachedUser);
        }

        // If not in cache, retrieve from the database
        String querySQL = "SELECT * FROM users WHERE user_id = ? AND pwd = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
            pstmt.setString(1, userId);
            pstmt.setString(2, pwd);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    User user = new User(
                            rs.getString("user_id"),
                            rs.getString("pwd"),
                            rs.getString("email"),
                            rs.getString("display_name")
                    );
                    cacheUser(user);  // Cache the user after retrieval
                    return Result.ok(user);
                } else {
                    Log.info("User not found or incorrect password: " + userId);
                    return Result.error(Result.ErrorCode.NOT_FOUND);
                }
            }
        } catch (SQLException e) {
            Log.warning("Failed to get user: " + e.getMessage());
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<User> updateUser(String userId, String pwd, User other) {
        String updateSQL = "UPDATE users SET pwd = ?, email = ?, display_name = ? WHERE user_id = ? AND pwd = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setString(1, other.getPwd());
            pstmt.setString(2, other.getEmail());
            pstmt.setString(3, other.getDisplayName());
            pstmt.setString(4, userId);
            pstmt.setString(5, pwd);

            int rowsUpdated = pstmt.executeUpdate();
            if (rowsUpdated > 0) {
                cacheUser(other);  // Update the cache after updating the user
                Log.info("User updated successfully: " + userId);
                return Result.ok(other);
            } else {
                Log.info("User not found or incorrect password for update: " + userId);
                return Result.error(Result.ErrorCode.NOT_FOUND);
            }
        } catch (SQLException e) {
            Log.warning("Failed to update user: " + e.getMessage());
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<User> deleteUser(String userId, String pwd) {

        String deleteSQL = "DELETE FROM users WHERE user_id = ? AND pwd = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
            pstmt.setString(1, userId);
            pstmt.setString(2, pwd);

            int rowsDeleted = pstmt.executeUpdate();

            if (rowsDeleted > 0) {
                removeCachedUser(userId);  // Remover do cache após a exclusão
                Log.info("User deleted successfully: " + userId);
                shorts.deleteAllShorts(userId, pwd, RestShorts.TOKEN);
                return Result.ok(); // Retornar um sucesso simples
            } else {
                Log.info("User not found or incorrect password for deletion: " + userId);
                return Result.error(Result.ErrorCode.NOT_FOUND);
            }
        } catch (SQLException e) {
            Log.warning("Failed to delete user: " + e.getMessage());
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<List<User>> searchUsers(String pattern) {
        String cacheKey = "user_search_" + pattern.toUpperCase();
        List<User> users;

        // Try to get cached search results
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            String cachedUsersJson = jedis.get(cacheKey);
            if (cachedUsersJson != null) {
                users = JSON.decode(cachedUsersJson, new TypeReference<List<User>>() {});
                if (users != null) {
                    return Result.ok(users);
                }
            }
        } catch (Exception e) {
            Log.warning("Failed to access Redis cache for search, proceeding with database query.");
        }

        String searchSQL = "SELECT * FROM users WHERE display_name ILIKE ? OR email ILIKE ?";
        users = new ArrayList<>();

        try (PreparedStatement pstmt = connection.prepareStatement(searchSQL)) {
            pstmt.setString(1, "%" + pattern + "%");
            pstmt.setString(2, "%" + pattern + "%");
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    User user = new User(
                            rs.getString("user_id"),
                            rs.getString("pwd"),
                            rs.getString("email"),
                            rs.getString("display_name")
                    );
                    users.add(user);
                }
            }

            // Cache the search results in Redis
            try (Jedis jedis = RedisCache.getCachePool().getResource()) {
                jedis.setex(cacheKey, 3600, JSON.encode(users));
            } catch (Exception e) {
                Log.warning("Failed to cache search results in Redis.");
            }

            Log.info("Search completed for pattern: " + pattern);
            return Result.ok(users);
        } catch (SQLException e) {
            Log.warning("Failed to search users: " + e.getMessage());
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
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

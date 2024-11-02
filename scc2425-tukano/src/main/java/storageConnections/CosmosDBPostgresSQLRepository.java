package storageConnections;

import tukano.api.Result;
import tukano.api.Short;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

public class CosmosDBPostgresSQLRepository implements ShortsRepository{

    private static final Logger Log = Logger.getLogger(CosmosDBPostgresSQLRepository.class.getName());

    public CosmosDBPostgresSQLRepository() {
    }

    @Override
    public Result<Short> createShort(Short shrt) {
        return null;
    }

    @Override
    public Result<Short> getShort(String shortId) {
        return null;
    }

    @Override
    public Result<Void> deleteShort(Short shrt) {
        return null;
    }

    @Override
    public Result<List<String>> getShorts(String userId) {
        return null;
    }

    @Override
    public Result<Void> follow(String userId1, String userId2, boolean isFollowing) {
        return null;
    }

    @Override
    public Result<List<String>> followers(String userId) {
        return null;
    }

    @Override
    public Result<Void> like(String userId, boolean isliked, Short shrt) {
        return null;
    }

    @Override
    public Result<List<String>> likes(String shortId) {
        return null;
    }

    @Override
    public Result<List<String>> getFeed(String userId) {
        return null;
    }

    @Override
    public Result<Void> deleteAllShorts(String userId) {
        return null;
    }
}

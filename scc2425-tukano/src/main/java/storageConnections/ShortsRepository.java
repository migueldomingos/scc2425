package storageConnections;

import tukano.api.Result;
import tukano.api.Short;
import tukano.api.Shorts;

import java.util.List;

public interface ShortsRepository {
    Result<Short> createShort(Short shrt);

    Result<Short> getShort(String shortId);

    Result<Void> deleteShort(Short shrt);

    Result<List<String>> getShorts(String userId);

    Result<Void> follow(String userId1, String userId2, boolean isFollowing);

    Result<List<String>> followers(String userId);

    Result<Void> like(String userId, boolean isliked, Short shrt);

    Result<List<String>> likes(String shortId);

    Result<List<String>> getFeed(String userId, String password);
}

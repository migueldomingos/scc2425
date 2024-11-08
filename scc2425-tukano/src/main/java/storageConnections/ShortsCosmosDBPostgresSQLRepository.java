package storageConnections;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.ok;
import static utils.DB.getOne;

import java.util.List;
import tukano.api.Result;
import tukano.api.Short;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import utils.DB;
import tukano.impl.Token;
import tukano.impl.JavaBlobs;

public class ShortsCosmosDBPostgresSQLRepository implements ShortsRepository {
    public ShortsCosmosDBPostgresSQLRepository() {}

    @Override
    public Result<Short> createShort(Short shrt) {
        return errorOrValue(DB.insertOne(shrt), s -> s.copyWithLikes_And_Token(0));
    }

    @Override
    public Result<Short> getShort(String shortId) {
        var query = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
        var likes = DB.sql(query, Long.class);
        return errorOrValue( getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( likes.get(0)));
    }

    @Override
    public Result<Void> deleteShort(Short shrt) {
        return DB.transaction( hibernate -> {

            hibernate.remove( shrt);

            var query = format("DELETE FROM Likes l WHERE l.shortId = '%s'", shrt.getid());
            hibernate.createNativeQuery( query, Likes.class).executeUpdate();

            JavaBlobs.getInstance().delete(shrt.getBlobUrl(), Token.get() );
        });
    }

    @Override
    public Result<List<String>> getShorts(String userId) {
        var query = format("SELECT s.id FROM Shorts s WHERE s.ownerId = '%s'", userId);
        return ok(DB.sql( query, String.class));
    }

    @Override
    public Result<Void> follow(String userId1, String userId2, boolean isFollowing) {
        var f = new Following(userId1, userId2);
        Result<Following> followingResult = isFollowing ? DB.insertOne(f) : DB.deleteOne(f);
        return followingResult.isOK() ? ok() : error(followingResult.error());
    }

    @Override
    public Result<List<String>> followers(String userId) {
        var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);
        return ok(DB.sql(query, String.class));
    }

    @Override
    public Result<Void> like(String userId, boolean isliked, Short shrt) {
        var l = new Likes(userId, shrt.getid(), shrt.getOwnerId());
        Result<Likes> likesResult = isliked ? DB.insertOne(l) : DB.deleteOne(l);
        return likesResult.isOK() ? ok() : error(likesResult.error());
    }

    @Override
    public Result<List<String>> likes(String shortId) {
        var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);

        return ok(DB.sql(query, String.class));    }

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

        });
    }
}
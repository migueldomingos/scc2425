package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.error;
import static tukano.api.Result.ok;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import tukano.api.Result;
import tukano.api.User;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.logging.Logger;

import com.azure.cosmos.*;
import tukano.api.Blobs;
import tukano.api.Short;
import tukano.api.Shorts;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;

public class JavaShorts implements Shorts {

	private static final Logger Log = Logger.getLogger(JavaShorts.class.getName());
	
	private static Shorts instance;

    private final CosmosContainer container;

	synchronized public static Shorts getInstance() {
		if( instance == null )
			instance = new JavaShorts();
		return instance;
	}
	
	private JavaShorts() {
        //.directMode()
        // replace by .directMode() for better performance
        CosmosClient client = new CosmosClientBuilder()
                .endpoint(System.getProperty("COSMOSDB_URL"))
                .key(System.getProperty("COSMOSDB_KEY"))
                //.directMode()
                .gatewayMode()
                // replace by .directMode() for better performance
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .contentResponseOnWriteEnabled(true)
                .buildClient();

        CosmosDatabase db = client.getDatabase(System.getProperty("COSMOSDB_DATABASE"));
		container = db.getContainer(Shorts.NAME);
	}
	
	
	@Override
	public Result<Short> createShort(String userId, String password) {
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

		var shortId = format("%s+%s", userId, UUID.randomUUID());
		var blobUrl = format("%s/%s", Blobs.LINK, shortId);
		var shrt = new Short(shortId, userId, blobUrl);

		Result<User> user = okUser(userId, password);

		if (user.error().equals(NOT_FOUND))
			return Result.error(NOT_FOUND);
		else if (user.error().equals(FORBIDDEN))
			return Result.error(FORBIDDEN);
		else if (!user.isOK())
			return Result.error(BAD_REQUEST);

		return tryCatch( () -> container.createItem(shrt).getItem());
	}

	@Override
	public Result<Short> getShort(String shortId) {
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);

		return tryCatch( () -> container.readItem(shortId, new PartitionKey(shortId), Short.class).getItem());
	}

	
	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));
		
		Result<Short> shrt = getShort(shortId);
		if (shrt.error().equals(NOT_FOUND))
			return Result.error(NOT_FOUND);

		Result<User> user = okUser(shrt.value().getOwnerId(), password);

		if (!user.isOK())
			return Result.error(FORBIDDEN);

		tryCatch( () -> container.deleteItem(shrt.value(), new CosmosItemRequestOptions()).getItem());
		return Result.ok();

	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		if (!okUser(userId).isOK()) {
			return Result.error(NOT_FOUND);
		}

		String query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
		List<String> shortIds = new ArrayList<>();

		try {
			CosmosPagedIterable<String> results = container.queryItems(query, new CosmosQueryRequestOptions(), String.class);
			results.forEach(shortIds::add);

			return Result.ok(shortIds);
		} catch (CosmosException e) {
			Log.warning("Error consulting the shorts: " + e.getMessage());
			return Result.error(INTERNAL_ERROR);
		}
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s, pwd = %s\n", userId1, userId2, isFollowing, password));

		Result<User> follower = okUser(userId1, password);

		if (follower.error().equals(NOT_FOUND)) {
			return Result.error(NOT_FOUND);
		}

		if (!follower.isOK()) {
			return Result.error(FORBIDDEN);
		}


		if (!okUser(userId2).isOK()) {
			return Result.error(NOT_FOUND);
		}

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

			Result<Object> res = tryCatch( () -> container.deleteItem(f, new CosmosItemRequestOptions()).getItem());
			if (res.isOK())
				return Result.ok();
			else
				return Result.error(res.error());
		}
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		Result<User> user = okUser(userId, password);

		if (user.error().equals(NOT_FOUND)) {
			return Result.error(NOT_FOUND);
		}

		if (!user.isOK()) {
			return Result.error(FORBIDDEN);
		}

		String query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);
		List<String> followers = new ArrayList<>();

		try {
			CosmosPagedIterable<String> results = container.queryItems(query, new CosmosQueryRequestOptions(), String.class);
			results.forEach(followers::add);

			return Result.ok(followers);
		} catch (CosmosException e) {
			return Result.error(INTERNAL_ERROR);
		}

	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

		Result<User> user = okUser(userId, password);

		if (user.error().equals(NOT_FOUND)) {
			return Result.error(NOT_FOUND);
		}

		if (!user.isOK()) {
			return Result.error(FORBIDDEN);
		}

		Result<Short> resShort = getShort(shortId);
		if (!resShort.isOK())
			return Result.error(NOT_FOUND);

		Likes l = new Likes(userId, shortId, resShort.value().getOwnerId());

		if (isLiked) {
			Result<Likes> res = tryCatch( () -> container.createItem(l).getItem());
			if (res.isOK())
				return Result.ok();
			else
				return Result.error(res.error());
		} else {
			Result<Object> res = tryCatch( () -> container.deleteItem(l, new CosmosItemRequestOptions()).getItem());
			if (res.isOK())
				return Result.ok();
			else
				return Result.error(res.error());
		}
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

		Result<Short> shrt = getShort(shortId);
		if (!shrt.isOK()) {
			return Result.error(NOT_FOUND);
		}

		Result<User> owner = okUser(shrt.value().getOwnerId(), password);
		if (owner.error().equals(FORBIDDEN)) {
			return Result.error(FORBIDDEN);
		} else if (!owner.isOK()) {
			return Result.error(BAD_REQUEST);
		}

		String query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);
		List<String> likedUserIds = new ArrayList<>();

		try {
			CosmosPagedIterable<String> results = container.queryItems(query, new CosmosQueryRequestOptions(), String.class);
			results.forEach(likedUserIds::add);

			return Result.ok(likedUserIds);
		} catch (CosmosException e) {
			Log.warning("Error consulting the likes: " + e.getMessage());
			return Result.error(INTERNAL_ERROR);
		}
	}

	//Com os seus shorts ou apenas do que segue
	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

		Result<User> user = okUser(userId, password);
		if (user.error().equals(NOT_FOUND)) {
			return Result.error(NOT_FOUND);
		}
		if (user.error().equals(FORBIDDEN)) {
			return Result.error(FORBIDDEN);
		}

		List<String> followingUserIds = followers(userId, password).value();

		List<String> feedShortIds = new ArrayList<>();
		if (!followingUserIds.isEmpty()) {
			String feedQuery = format(
					"SELECT s.shortId FROM Short s WHERE ARRAY_CONTAINS(%s, s.ownerId) ORDER BY s.creationDate DESC",
					followingUserIds
			);

			try {
				CosmosPagedIterable<String> feedResults = container.queryItems(feedQuery, new CosmosQueryRequestOptions(), String.class);
				feedResults.forEach(feedShortIds::add);
			} catch (CosmosException e) {
				Log.warning("Error consulting the feed shorts: " + e.getMessage());
				return Result.error(INTERNAL_ERROR);
			}
		}

		return Result.ok(feedShortIds);
	}
		
	protected Result<User> okUser( String userId, String pwd) {
		return JavaUsers.getInstance().getUser(userId, pwd);
	}
	
	private Result<Void> okUser( String userId ) {
		var res = okUser( userId, "");
		if( res.error() == FORBIDDEN )
			return ok();
		else
			return error( res.error() );
	}
	
	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {
		Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

		if (!Token.isValid(token, userId)) {
			return error(FORBIDDEN);
		}

		Result<User> user = okUser(userId, password);
		if (!user.isOK()) {
			return Result.error(FORBIDDEN);
		}

		//delete shorts
		String queryDeleteShorts = format("SELECT * FROM Short s WHERE s.ownerId = '%s'", userId);
		CosmosPagedIterable<Short> shorts = container.queryItems(queryDeleteShorts, new CosmosQueryRequestOptions(), Short.class);
		shorts.forEach(shrt -> tryCatch( () -> container.deleteItem(shrt, new CosmosItemRequestOptions())));

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
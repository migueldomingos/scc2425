package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.error;
import static tukano.api.Result.ok;

import storageConnections.ShortsCosmosDBNoSQLRepository;
import storageConnections.ShortsCosmosDBPostgresSQLRepository;
import storageConnections.ShortsRepository;
import tukano.api.Result;
import tukano.api.User;

import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import tukano.api.Blobs;
import tukano.api.Short;
import tukano.api.Shorts;

public class JavaShorts implements Shorts {

	private static final Logger Log = Logger.getLogger(JavaShorts.class.getName());
	private static final String SHORT_CACHE_PREFIX = "short:";
	private static Shorts instance;
	private final ShortsRepository repository;


	synchronized public static Shorts getInstance() {
		if( instance == null )
			instance = new JavaShorts();
		return instance;
	}
	
	private JavaShorts() {
		String sqlType = System.getProperty("COSMOSDB_SQL_TYPE");

		if (sqlType.equals("P")) {
			this.repository = new ShortsCosmosDBPostgresSQLRepository();
		} else {
			this.repository = new ShortsCosmosDBNoSQLRepository();
		}
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

		return repository.createShort(shrt);
	}

	@Override
	public Result<Short> getShort(String shortId) {
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);

		return repository.getShort(shortId);
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

		return repository.deleteShort(shrt.value());

	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		if (!okUser(userId).isOK()) {
			return Result.error(NOT_FOUND);
		}

		return repository.getShorts(userId);
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

		return repository.follow(userId1, userId2, isFollowing);
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

		return repository.followers(userId);
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

		return repository.like(userId, isLiked, resShort.value());
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

		return repository.likes(shortId);
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

		return repository.getFeed(userId);
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

		return repository.deleteAllShorts(userId);
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
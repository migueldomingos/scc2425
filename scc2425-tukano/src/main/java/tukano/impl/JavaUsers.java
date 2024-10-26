package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrResult;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.*;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.util.CosmosPagedIterable;
import tukano.api.Result;
import tukano.api.User;
import tukano.api.Users;
import utils.DB;

public class JavaUsers implements Users {
	
	private static Logger Log = Logger.getLogger(JavaUsers.class.getName());

	private static Users instance;

	private CosmosClient client;
	private CosmosDatabase db;
	private CosmosContainer container;
	
	synchronized public static Users getInstance() {
		if( instance == null )
			instance = new JavaUsers();
		return instance;
	}
	
	private JavaUsers() {

		client = new CosmosClientBuilder()
				.endpoint(System.getProperty("COSMOSDB_URL"))
				.key(System.getProperty("COSMOSDB_KEY"))
				//.directMode()
				.gatewayMode()
				// replace by .directMode() for better performance
				.consistencyLevel(ConsistencyLevel.SESSION)
				.connectionSharingAcrossClientsEnabled(true)
				.contentResponseOnWriteEnabled(true)
				.buildClient();

		db = client.getDatabase(System.getProperty("COSMOSDB_DATABASE"));
		container = db.getContainer(Users.NAME);
	}
	
	@Override
	public Result<String> createUser(User user) {
		Log.info(() -> format("createUser : %s\n", user));

		if( badUserInfo( user ) )
				return error(BAD_REQUEST);


		return tryCatch( () -> container.createItem(user).getItem().getUserId());
		//return errorOrValue( DB.insertOne( user), user.getUserId() );
	}

	@Override
	public Result<User> getUser(String userId, String pwd) {
		Log.info( () -> format("getUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null)
			return error(BAD_REQUEST);

		Result<User> user = tryCatch( () -> container.readItem(userId, new PartitionKey(userId), User.class).getItem());

		if (user.isOK() && !user.value().getPwd().equals(pwd)){
			return Result.error(FORBIDDEN);
		}

		return user;
		//return validatedUserOrError( DB.getOne( userId, User.class), pwd);
	}

	@Override
	public Result<User> updateUser(String userId, String pwd, User other) {
		Log.info(() -> format("updateUser : userId = %s, pwd = %s, user: %s\n", userId, pwd, other));

		if (badUpdateUserInfo(userId, pwd, other))
			return error(BAD_REQUEST);

		Result<User> oldUserResult = getUser(userId, pwd);
		if (!oldUserResult.isOK())
			return oldUserResult;

		if (other.getPwd() == null)
			other.setPwd(oldUserResult.value().getPwd());
		if (other.getEmail() == null)
			other.setEmail(oldUserResult.value().getEmail());
		if (other.getDisplayName() == null)
			other.setDisplayName(oldUserResult.value().getDisplayName());

		return tryCatch( () -> container.upsertItem(other).getItem());
	}

	@Override
	public Result<User> deleteUser(String userId, String pwd) {
		Log.info(() -> format("deleteUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null || pwd == null )
			return error(BAD_REQUEST);

		Result<User> oldUserResult = getUser(userId, pwd);
		if (!oldUserResult.isOK())
			return oldUserResult;

		Result<Object> result = tryCatch( () -> container.deleteItem(oldUserResult.value(), new CosmosItemRequestOptions()).getItem());

		if(result.isOK())
			return oldUserResult;
		else
			return error(BAD_REQUEST);
	}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		Log.info( () -> format("searchUsers : patterns = %s\n", pattern));

		if (pattern == null || pattern.trim().isEmpty()) {
			return error(BAD_REQUEST);
		}

		var query = format("SELECT * FROM User u WHERE CONTAINS(UPPER(u.userId), '%s')", pattern.toUpperCase());

		List<User> users = new ArrayList<>();

		try {
			CosmosPagedIterable<User> results = container.queryItems(
					query, new CosmosQueryRequestOptions(), User.class
			);

			// Process the results, copying the users without their password
			users = results.stream()
					.map(User::copyWithoutPassword)
					.collect(Collectors.toList());

		} catch (CosmosException e) {
			return error(INTERNAL_ERROR);
		}

		return ok(users);
	}

	
	private Result<User> validatedUserOrError( Result<User> res, String pwd ) {
		if( res.isOK())
			return res.value().getPwd().equals( pwd ) ? res : error(FORBIDDEN);
		else
			return res;
	}
	
	private boolean badUserInfo( User user) {
		return (user.userId() == null || user.pwd() == null || user.displayName() == null || user.email() == null);
	}
	
	private boolean badUpdateUserInfo( String userId, String pwd, User info) {
		return (userId == null || pwd == null || info.getUserId() != null && ! userId.equals( info.getUserId()));
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

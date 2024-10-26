package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.*;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrResult;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.errorOrVoid;
import static tukano.api.Result.ok;
import static utils.DB.getOne;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import tukano.api.Result;
import tukano.api.User;
import tukano.api.Users;

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
import utils.DB;

public class JavaShorts implements Shorts {

	private static Logger Log = Logger.getLogger(JavaShorts.class.getName());
	
	private static Shorts instance;

	private CosmosClient client;
	private CosmosDatabase db;
	private CosmosContainer container;

	private Users users;
	
	synchronized public static Shorts getInstance() {
		if( instance == null )
			instance = new JavaShorts();
		return instance;
	}
	
	private JavaShorts() {
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
		container = db.getContainer(Shorts.NAME);

		users = JavaUsers.getInstance();;
	}
	
	
	@Override
	public Result<Short> createShort(String userId, String password) {
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

		var shortId = format("%s+%s", userId, UUID.randomUUID());
		var blobUrl = format("%s/%s", Blobs.LINK, shortId);
		var shrt = new Short(shortId, userId, blobUrl);

		Result<User> user = users.getUser(userId, password);

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

		Result<User> user = users.getUser(shrt.value().getOwnerId(), password);

		if (!user.isOK())
			return Result.error(FORBIDDEN);

		tryCatch( () -> container.deleteItem(shrt.value(), new CosmosItemRequestOptions()).getItem());
		return Result.ok();

	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		// Verifica se o usuário existe
		Result<List<User>> lusers = users.searchUsers(userId);
		if (!lusers.isOK()) {
			return Result.error(BAD_REQUEST);
		}
		boolean aux = false;
		for (User u: lusers.value()){
			if (u.getUserId().equals(userId)) {
				aux = true;
				break;
			}
		}
		if (!aux) {
			return Result.error(NOT_FOUND);
		}

		// Define a query SQL para recuperar o shortId dos Shorts do usuário
		String query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
		List<String> shortIds = new ArrayList<>();

		try {
			// Executa a consulta e adiciona os IDs na lista
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
	
		
		return errorOrResult( okUser(userId1, password), user -> {
			var f = new Following(userId1, userId2);
			return errorOrVoid( okUser( userId2), isFollowing ? DB.insertOne( f ) : DB.deleteOne( f ));	
		});			
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);		
		return errorOrValue( okUser(userId, password), DB.sql(query, String.class));
	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

		
		return errorOrResult( getShort(shortId), shrt -> {
			var l = new Likes(userId, shortId, shrt.getOwnerId());
			return errorOrVoid( okUser( userId, password), isLiked ? DB.insertOne( l ) : DB.deleteOne( l ));	
		});
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

		return errorOrResult( getShort(shortId), shrt -> {
			
			var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);					
			
			return errorOrValue( okUser( shrt.getOwnerId(), password ), DB.sql(query, String.class));
		});
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

		final var QUERY_FMT = """
				SELECT s.shortId, s.timestamp FROM Short s WHERE	s.ownerId = '%s'				
				UNION			
				SELECT s.shortId, s.timestamp FROM Short s, Following f 
					WHERE 
						f.followee = s.ownerId AND f.follower = '%s' 
				ORDER BY s.timestamp DESC""";

		return errorOrValue( okUser( userId, password), DB.sql( format(QUERY_FMT, userId, userId), String.class));		
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

		if( ! Token.isValid( token, userId ) )
			return error(FORBIDDEN);
		
		return DB.transaction( (hibernate) -> {
						
			//delete shorts
			var query1 = format("DELETE Short s WHERE s.ownerId = '%s'", userId);		
			hibernate.createQuery(query1, Short.class).executeUpdate();
			
			//delete follows
			var query2 = format("DELETE Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);		
			hibernate.createQuery(query2, Following.class).executeUpdate();
			
			//delete likes
			var query3 = format("DELETE Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);		
			hibernate.createQuery(query3, Likes.class).executeUpdate();
			
		});
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
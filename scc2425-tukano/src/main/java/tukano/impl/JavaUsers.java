package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.ErrorCode.*;
import java.util.List;
import java.util.logging.Logger;
import storageConnections.*;
import tukano.api.Result;
import tukano.api.User;
import tukano.api.Users;

public class JavaUsers implements Users {
	
	private static final Logger Log = Logger.getLogger(JavaUsers.class.getName());
	private static Users instance;
    private final UsersRepository repository;

	synchronized public static Users getInstance() {
		if( instance == null )
			instance = new JavaUsers();
		return instance;
	}
	
	private JavaUsers() {
		String sqlType = System.getProperty("COSMOSDB_SQL_TYPE");

		if (sqlType.equals("P")) {
			this.repository = new UsersCosmosDBPostgresSQLRepository();
		} else {
			this.repository = new UsersCosmosDBNoSQLRepository();
		}
	}
	
	@Override
	public Result<String> createUser(User user) {
		Log.info(() -> format("createUser : %s\n", user));

		if( badUserInfo( user ) )
				return error(BAD_REQUEST);

		return repository.createUser(user);
	}

	@Override
	public Result<User> getUser(String userId, String pwd) {
		Log.info( () -> format("getUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null)
			return error(BAD_REQUEST);

		return repository.getUser(userId, pwd);
	}

	@Override
	public Result<User> updateUser(String userId, String pwd, User other) {
		Log.info(() -> format("updateUser : userId = %s, pwd = %s, user: %s\n", userId, pwd, other));

		if (badUpdateUserInfo(userId, pwd, other))
			return error(BAD_REQUEST);

		return repository.updateUser(userId, pwd, other);
	}

	@Override
	public Result<User> deleteUser(String userId, String pwd) {
		Log.info(() -> format("deleteUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null || pwd == null )
			return error(BAD_REQUEST);

		return repository.deleteUser(userId, pwd);
	}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		Log.info(() -> format("searchUsers : patterns = %s\n", pattern));

		if (pattern == null) {
			return error(BAD_REQUEST);
		}

		return repository.searchUsers(pattern);
	}
	
	private boolean badUserInfo( User user) {
		return (user.id() == null || user.pwd() == null || user.displayName() == null || user.email() == null);
	}
	
	private boolean badUpdateUserInfo( String userId, String pwd, User info) {
		return (userId == null || pwd == null || info.getid() != null && ! userId.equals( info.getid()));
	}
}

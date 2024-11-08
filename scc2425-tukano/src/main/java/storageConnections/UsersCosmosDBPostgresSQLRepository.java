package storageConnections;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrResult;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.FORBIDDEN;

import java.util.List;
import java.util.concurrent.Executors;

import tukano.api.Result;
import tukano.api.User;
import tukano.impl.JavaBlobs;
import tukano.impl.Token;
import utils.DB;
import tukano.impl.JavaShorts;

public class UsersCosmosDBPostgresSQLRepository implements UsersRepository {
    public UsersCosmosDBPostgresSQLRepository() {}

    @Override
    public Result<String> createUser(User user) {
        return errorOrValue( DB.insertOne( user), user.getid() );
    }

    @Override
    public Result<User> getUser(String userId, String pwd) {
        return validatedUserOrError( DB.getOne( userId, User.class), pwd);
    }

    @Override
    public Result<User> updateUser(String userId, String pwd, User other) {
        return errorOrResult( validatedUserOrError(DB.getOne( userId, User.class), pwd), user -> DB.updateOne( user.updateFrom(other)));
    }

    @Override
    public Result<User> deleteUser(String userId, String pwd) {
        return errorOrResult( validatedUserOrError(DB.getOne( userId, User.class), pwd), user -> {

            // Delete user shorts and related info asynchronously in a separate thread
            Executors.defaultThreadFactory().newThread( () -> {
                JavaShorts.getInstance().deleteAllShorts(userId, pwd, Token.get(userId));
                JavaBlobs.getInstance().deleteAllBlobs(userId, Token.get(userId));
            }).start();

            return DB.deleteOne( user);
        });
    }

    @Override
    public Result<List<User>> searchUsers(String pattern) {

        var query = format("SELECT * FROM Users u WHERE UPPER(u.id) LIKE '%%%s%%'", pattern.toUpperCase());
        var hits = DB.sql(query, User.class)
                .stream()
                .map(User::copyWithoutPassword)
                .toList();

        return ok(hits);
    }


    private Result<User> validatedUserOrError( Result<User> res, String pwd ) {
        if( res.isOK())
            return res.value().getPwd().equals( pwd ) ? res : error(FORBIDDEN);
        else
            return res;
    }
}
package storageConnections;

import tukano.api.Result;
import tukano.api.User;
import java.util.List;


public interface UsersRepository {

    Result<String> createUser(User user);

    Result<User> getUser(String userId, String pwd);

    Result<User> updateUser(String userId, String pwd, User other);

    Result<User> deleteUser(String userId, String pwd);

    Result<List<User>> searchUsers(String pattern);

}

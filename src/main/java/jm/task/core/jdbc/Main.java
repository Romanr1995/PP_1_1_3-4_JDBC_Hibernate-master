package jm.task.core.jdbc;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();

        userService.createUsersTable();

        User user1 = new User("user1", "userov1", (byte) 22);
        User user2 = new User("user2", "userov2", (byte) 24);
        User user3 = new User("user3", "userov3", (byte) 26);
        User user4 = new User("user4", "userov4", (byte) 28);

        List<User> users = Arrays.asList(user1, user2, user3, user4);

        for (User user : users) {
            userService.saveUser(user.getName(), user.getLastName(), user.getAge());
        }

        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}

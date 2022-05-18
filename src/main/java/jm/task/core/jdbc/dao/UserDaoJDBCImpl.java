package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static jm.task.core.jdbc.util.Util.getConnection;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        String sql = "Create Table If Not Exists users " +
                "(id BigInt Primary Key AUTO_INCREMENT, name varchar(100), lastName varchar(100), age TINYINT)";

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            getConnection().commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropUsersTable() {
        String sql = "Drop Table If Exists users";

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            getConnection().commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String sql = "Insert Into users (name, lastName, age) Values (?, ?, ?)";

        try (PreparedStatement pstm = getConnection().prepareStatement(sql)) {

            pstm.setString(1, name);
            pstm.setString(2, lastName);
            pstm.setByte(3, age);

            pstm.execute();
            getConnection().commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        String sql = "Delete From users Where id = ?";

        try (PreparedStatement pstm = getConnection().prepareStatement(sql)) {
            pstm.setLong(1, id);
            pstm.executeUpdate();

            getConnection().commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        String sql = "Select * From users";
        List<User> users = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();) {
            ResultSet resultSet = stmt.executeQuery(sql);

            while (resultSet.next()) {
                Long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                String lastName = resultSet.getString("lastName");
                Byte age = resultSet.getByte("age");

                users.add(new User(id, name, lastName, age));
            }

            getConnection().commit();
        } catch (SQLException e) {
            try {
                getConnection().rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
        return users;
    }

    public void cleanUsersTable() {
        String sql = "Delete From users";

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            getConnection().commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

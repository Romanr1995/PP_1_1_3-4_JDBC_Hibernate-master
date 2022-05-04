package jm.task.core.jdbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Util {
    private static Connection connection;

    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Драйвер для соединения с базой данных не найден");
        }

        Properties prop = new Properties();

        try (InputStream in = Files.newInputStream(Paths.get("src\\main\\resources\\database.properties"))) {
            prop.load(in);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        try {
            connection = DriverManager.getConnection(prop.getProperty("url"),
                    prop.getProperty("user"), prop.getProperty("password"));
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            System.out.println("Не удалось установить соединение с базой данных");
        }

        return connection;
    }
}

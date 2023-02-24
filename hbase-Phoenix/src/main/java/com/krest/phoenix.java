package com.krest;

import java.sql.*;
import java.util.Properties;

public class phoenix {

    /**
     * 如果没有对应的实际
     *
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        // 标准的 JDBC 代码
        Connection connection = DriverManager.getConnection("jdbc:phoenix:hadoop100,hadoop101,hadoop102:2181");
        // 5.编译 SQL 语句
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");
        // 6.执行语句
        ResultSet resultSet = preparedStatement.executeQuery();
        // 7.输出结果
        System.out.println("---------------------------------------------");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + " |  "
                    + resultSet.getString(2) + "  |  "
                    + resultSet.getString(3) + "  |  "
                    + resultSet.getString(4));
        }
        System.out.println("---------------------------------------------");
        // 8.关闭资源
        connection.close();
        // 由于 Phoenix 框架内部需要获取一个 HBase 连接,所以会延迟关闭

        // 不影响后续的代码执行
        System.out.println("结束执行");
    }
}

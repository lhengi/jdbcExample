package com.lhengi.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.flogger.FluentLogger;

public class Main {
  public static final String MYSQLURL = "jdbc:mysql://localhost:3306/homework4db?useUnicode=true" +
    "&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
  public static final String USERNAME = "hw4";
  public static final String PASSWORD = "password";

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  public static void main(String args[]) {
    // Main method

    logger.atInfo().log("Connecting to database: %s", MYSQLURL);
    Connection connection = null;
    try{
      connection = DriverManager.getConnection(MYSQLURL, USERNAME, PASSWORD);
      logger.atInfo().log("Database Connected");
    } catch (SQLException e) {
      logger.atWarning().withCause(e);
      throw new IllegalStateException("Cannot connect the database!", e);
    }

    Statement statement = null
    try{
      statement = connection.createStatement();
    } catch (SQLException e) {
      logger.atWarning().withCause(e);
      throw new IllegalStateException("Cannot create statement!",e);
    }

  }

  private static void createTables(Statement statement) {

  }
}

package com.lhengi.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;

import com.google.common.flogger.FluentLogger;

public class Main {
  private static final String MYSQLURL =
      "jdbc:mysql://localhost:3306/homework4db?useUnicode=true"
          + "&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
  private static final String USERNAME = "hw4";
  private static final String PASSWORD = "password";
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static Connection connection;
  private static Statement statement;

  public static void main(String[] args) {
    logger.atInfo().log("Connecting to database: %s", MYSQLURL);
    try {
      connection = DriverManager.getConnection(MYSQLURL, USERNAME, PASSWORD);
      logger.atInfo().log("Database Connected");
    } catch (SQLException e) {
      logger.atWarning().withCause(e);
      throw new IllegalStateException("Cannot connect the database!", e);
    }

    try {
      statement = connection.createStatement();
    } catch (SQLException e) {
      logger.atWarning().withCause(e);
      throw new IllegalStateException("Cannot create statement!", e);
    }

    // Create employee and supervisor tables
    createEmployeeTables();
    createSupervisorTable();
    insertEmployee(/* eid = */1, /* name = */"Heng", /* salary = */120000, /* sid = */100);
    insertEmployee(/* eid = */2, /* name = */"John", /* salary = */100000, /* sid = */100);
    insertEmployee(/* eid = */3, /* name = */"Kyle", /* salary = */110000, /* sid = */100);
    insertEmployee(/* eid = */11, /* name = */"Sam", /* salary = */50000, /* sid = */1);
    insertEmployee(/* eid = */100, /* name = */"Li", /* salary = */160000, /* sid = */0);
    getALL();
    getAvgSalary();
    getEmployeeName(0);
    getAvgSalary(100);
    dropAll();
  }

  /**
   * Create a tuple in the employee table for the new employee.
   * Create a tuple in the supervisor table to associate the new employee with supervisor.
   * @param eid
   * @param name
   * @param salary
   * @param sid
   */
  private static void insertEmployee(int eid, String name, int salary, int sid) {
    // TODO: implement
    String employeeQuery =
      "INSERT INTO homework4db.employee values( " +
        eid + ", '" + name + "', " + salary + ");";
    try {
      statement.execute(employeeQuery);
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when insert to employee table!");
    }

    String supervisorQuery =
      "INSERT INTO homework4db.supervisor values( " + eid + ", " + sid + ");";
    try {
      statement.execute(supervisorQuery);
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when insert to supervisor table!");
    }
  }

  private static void getALL() {
    String employeeQuery = "SELECT * FROM homework4db.employee;";
    String supervisorQuery = "SELECT * FROM homework4db.supervisor;";
    try {
      ResultSet resultSet = statement.executeQuery(employeeQuery);
      System.out.println("Employee Table");
      while (resultSet.next()) {
        int eid = resultSet.getInt(/* columnLabel = */"eid" );
        String name = resultSet.getString(/* columnLabel = */"name");
        int salary = resultSet.getInt(/* columnLabel = */"salary");
        System.out.println("eid: " + eid + "\tname: " + name + "\tsalary: " + salary);
      }
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when select * from employee!");
    }

    try {
      ResultSet resultSet = statement.executeQuery(supervisorQuery);
      System.out.println("Supervisor Table");
      while (resultSet.next()) {
        int eid = resultSet.getInt(/* columnLabel = */"eid");
        int sid = resultSet.getInt(/* columnLabel = */"sid");
        System.out.println("eid: " + eid + " sid: " + sid);
      }
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when select * from supervisor!");
    }

  }

  /**
   * Delete the corresponding tuple from employee table.
   * Set supervisor to NULL for the employee in the supervisor table
   * @param eid
   */
  private static void deleteEmployee(int eid) {
    // TODO: implement
    String employeeQuery = "DELETE FROM homework4db.employee WHERE eid = " + eid + ";";
    try {
      statement.execute(employeeQuery);
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when deleting from employee!");
    }

    String supervisorQuery = "UPDATE homework4.db.supervisor " +
      "set sid = NULL WHERE eid = " + eid +";";
    try {
      statement.execute(supervisorQuery);
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when updating supervisor for deleted employee!");
    }
  }

  /**
   * Insert a new tuple to the supervisor table, if sid not exist then set sid to null
   * @param eid
   * @param sid
   */
  private static void insertSupervisor(int eid, int sid) {
    // TODO: implement
    String supervisorQuery = "INSERT INTO homework4db.supervisor " +
      "values(eid = " + eid + ", sid = " + sid +");";
    if (sid == -1) {
      supervisorQuery = "INSERT INTO homework4db.supervisor " +
        "values(eid = " + eid + ", " + sid + " = NULL);";
    }
    try {
      statement.execute(supervisorQuery);
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when inserting to supervisor table!");
    }
  }

  /**
   * Get average salary of all employees
   */
  private static void getAvgSalary() {
    // TODO: implement
    String employeeQuery = "SELECT avg(salary) AS avgSalary FROM homework4db.employee;";
    try {
      ResultSet resultSet = statement.executeQuery(employeeQuery);
      resultSet.next();
      logger.atInfo().log("Average Salary: %s", resultSet.getDouble("avgSalary"));
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when getting average of all employee!");
    }
  }

  /**
   * Get all employees under a supervisor
   * @param sid
   * @return list of employees
   */
  private static ArrayList<Employee> getEmployees(int sid) {
    ArrayList<Employee> employees = getEmployeeRec(sid, new HashSet<>());
    return employees;
  }

  /**
   * Recursively getting all employees under a supervisor
   * @param sid
   * @param visited
   * @return an ArrayList of employees
   */
  private static ArrayList<Employee> getEmployeeRec(int sid, HashSet<Integer> visited) {
    if (visited.contains(sid)) {
      return new ArrayList<>();
    }
    visited.add(sid);
    ArrayList<Employee> result = getDirectEmployees(sid);
    ArrayList<Employee> subResult = new ArrayList<>();
    for (Employee employee : result) {
      subResult.addAll(getEmployeeRec(employee.getEid(), visited));
    }
    result.addAll(subResult);
    return result;
  }

  /**
   * Retrieve the list of direct employees under a supervisor
   * @param sid
   * @return an ArrayList of employees
   */
  private static ArrayList<Employee> getDirectEmployees(int sid) {
    ArrayList<Employee> employeeList = new ArrayList<>();
    String query = "SELECT name, eid, salary FROM homework4db.employee NATURAL JOIN homework4db.supervisor WHERE " +
      "homework4db.supervisor.sid = " + sid + ";";
    try {
      ResultSet resultSet = statement.executeQuery(query);
      while (resultSet.next()) {
        Employee employee = new Employee
          (resultSet.getInt(/* columnLabel = */"eid"),
            resultSet.getString(/* columnLabel = */"name"),
            resultSet.getInt(/* columnLabel = */"salary"));
        employeeList.add(employee);
      }
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when getting direct employees!");
    }
    return employeeList;
  }

  /**
   * Display names of all employees under a supervisor
   * @param sid
   */
  private static void getEmployeeName(int sid) {
    System.out.println("Employee name under: " + sid);
    ArrayList<Employee> employeeList = getEmployees(sid);
    HashSet<Integer> hashSet = new HashSet<>();
    for (Employee employee : employeeList) {
      if (!hashSet.contains(employee.getEid())) {
        System.out.println("Name: " + employee.getName());
      }
      hashSet.add(employee.getEid());
    }
  }

  /**
   * Display average salary under a supervisor
   * @param sid
   */
  private static void getAvgSalary(int sid) {
    ArrayList<Employee> employeeList = getEmployees(sid);
    HashSet<Integer> hashSet = new HashSet<>();
    double total = 0;
    for (Employee employee : employeeList) {
      if (!hashSet.contains(employee.getEid())) {
        total += employee.getSalary();
      }
      hashSet.add(employee.getEid());
    }
    System.out.println("Average Employee salary under: " + sid + " is " + total/hashSet.size());
  }

  /**
   * Create employee table in the database
   */
  private static void createEmployeeTables() {
    String query =
      "CREATE TABLE employee ( " +
        "eid INTEGER not NULL, " +
        "name VARCHAR(255), " +
        "salary INTEGER, " +
        "PRIMARY KEY (eid))";
    try {
      statement.execute(query);
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when creating employee table!");
    }
  }

  /**
   * Create supervisor table in the database
   */
  private static void createSupervisorTable() {
    String query =
      "CREATE TABLE supervisor (" +
        "eid INTEGER not NULL, " +
        "sid INTEGER, " +
        "PRIMARY KEY (eid))";
    try {
      statement.execute(query);
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when creating supervisor table!");
    }
  }

  private static void dropAll() {
    String employeeQuery = "DROP TABLE homework4db.employee;";
    String supervisorQuery = "DROP TABLE homework4db.supervisor;";
    try {
      statement.execute(employeeQuery);
      statement.execute(supervisorQuery);
    } catch (SQLException e) {
      logger.atWarning().withCause(e).log("SQLException when dropping tables!");
    }
  }
}

package com.lhengi.jdbc;

public class Employee {
  private int eid;
  private String name;
  private int salary;

  public Employee(int eid, String name, int salary) {
    this.eid = eid;
    this.name = name;
    this.salary = salary;
  }

  public int getEid() {
    return eid;
  }

  public int getSalary() {
    return salary;
  }

  public String getName() {
    return name;
  }
}

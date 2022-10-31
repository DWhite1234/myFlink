package com.common.beans;

import lombok.Data;

import java.util.Date;

/**
 * @author zt
 */
@Data
public class Person {
    private String name;
    private Integer age;
    private String sex;
    private String className;
    private Date ts;
}

package com.prads.batch.csv.model;

import com.opencsv.bean.CsvBindByName;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class SomeDto {

    @CsvBindByName
    private String name;

    @CsvBindByName
    private int age;

    @CsvBindByName
    private String city;

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getCity() {
        return city;
    }

    public String[] toCsv() {
        Field[] fields = SomeDto.class.getDeclaredFields();
        List<String> fieldValues = new ArrayList<>();

        for (Field field : fields) {
            try {
                fieldValues.add(String.valueOf(field.get(this)));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        String[] string = fieldValues.toArray(new String[0]);
        return string;
    }

    @Override
    public String toString() {
        return "SomeDto{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", city='" + city + '\'' +
                '}';
    }

    public static String[] getFieldNames() {
        Field[] fields = SomeDto.class.getDeclaredFields();

        List<String> fieldNames = new ArrayList<>();

        for (Field field : fields) {
            fieldNames.add(field.getName());
        }

        return fieldNames.toArray(new String[0]);
    }
}

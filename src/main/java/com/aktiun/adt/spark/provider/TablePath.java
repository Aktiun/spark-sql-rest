package com.aktiun.adt.spark.provider;

import java.util.ArrayList;
import java.util.List;

public class TablePath {
    private String name;
    private List<String> paths = new ArrayList<String>();

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getPaths() {
        return this.paths;
    }

    public String[] getPathArray() {
        String[] array = this.paths.stream().toArray(String[]::new);
        return array;
    }

    public void addToPaths(String path) {
        this.paths.add(path);
    }
}
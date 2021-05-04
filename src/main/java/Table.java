import java.io.*;

import java.util.*;

public class Table implements Serializable, Comparable {
    private String name;
    private Vector<String> pageNames;
    private Vector<Comparable> minPageValue;
    // To be done : count element number in pages


    public Table(String name) {
        this.name = name;
        this.pageNames = new Vector<>();
        this.minPageValue = new Vector<>();
    }

    public String getName() {
        return name;
    }



    public Vector<String> getPageNames() {
        return pageNames;
    }

    public Vector<Comparable> getMinPageValue() {
        return minPageValue;
    }

    @Override
    public int compareTo(Object o) {
        return this.getName().compareTo(((Table) o).getName());
    }

    @Override
    public String toString() {
        return "Table{\n" +
                "name= '" + name + "'\n" +
                ", pageNames= " + pageNames + "\n" +
                ", minClusteringinAPage= " + minPageValue +
                "\n}";
    }
}

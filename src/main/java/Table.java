import java.io.*;

import java.util.*;

public class Table implements Serializable, Comparable {
    private String name;
    private Vector<String> pageNames,gridIndices;
    private Vector<Vector<String>> gridIndicesColumns;
    private Vector<Comparable> minPageValue;
    private String primarykey;
    // To be done : count element number in pages


    public Vector<String> getGridIndices() {
        return gridIndices;
    }

    public Vector<Vector<String>> getGridIndicesColumns() {
        return gridIndicesColumns;
    }

    public Table(String name, String pk) {
        this.name = name;
        this.pageNames = new Vector<>();
        this.gridIndices = new Vector<>();
        this.gridIndicesColumns = new Vector<>();
        this.minPageValue = new Vector<>();
        primarykey = pk;
    }

    public String getName() {
        return name;
    }

    public String getprimaryKey()
    {
            return primarykey;
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
        return "Table{" +
                "name='" + name + '\'' +
                ", pageNames=" + pageNames + "\n"+
                ", gridIndices=" + gridIndices.size()+gridIndices + "\n"+
                ", gridIndicesColumns="+ gridIndicesColumns.size()+ gridIndicesColumns +"\n"+
                ", minPageValue=" + minPageValue +"\n"+
                ", primarykey='" + primarykey + '\'' +"\n"+
                '}';
    }
}

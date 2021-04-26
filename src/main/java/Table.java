import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable, Comparable {
    private String name;
    private int numberOfPages;
    private Vector<String> pageNames;
    private Vector<Comparable> minPageValue;
    private Vector<Integer> pageSize;
    private Hashtable<String, Vector<String>> overflow;
    private Hashtable<String, Vector<Integer>> overflowSizes;
    // To be done : count element number in pages


    public Table(String name) {
        this.name = name;
        this.numberOfPages = 0;
        this.pageNames = new Vector<>();
        this.minPageValue = new Vector<>();
        this.pageSize = new Vector<>();
        this.overflow = new Hashtable<>();
        this.overflowSizes = new Hashtable<>();
    }

    public Vector<Integer> getPageSize() {
        return pageSize;
    }

    public Hashtable<String, Vector<Integer>> getOverflowSizes() {
        return overflowSizes;
    }


    public Hashtable<String, Vector<String>> getOverflow() {
        return overflow;
    }

    public String getName() {
        return name;
    }

    public int getNumberOfPages() {
        return numberOfPages;
    }

    public void setNumberOfPages(int increment) {
        this.numberOfPages = this.numberOfPages + increment;
    }

    public Vector getPageNames() {
        return pageNames;
    }

    public Vector<Comparable> getMinPageValue() {
        return minPageValue;
    }

    @Override
    public int compareTo(Object o) {
        return this.getName().compareTo(((Table) o).getName());
    }

    public static void main(String[] args) {
        Hashtable<String,String> t =  new Hashtable<>();
        System.out.println(t.get("asfa"));
    }
}

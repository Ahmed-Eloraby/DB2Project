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
        Vector<Integer> test = new Vector<>();
        test.add(0);
        test.add(1);
        test.add(2);
        test.add(4);
        test.add(5);

        System.out.println(test);
        test.insertElementAt(3,5);
        System.out.println(test);
        Vector<Integer> test1 = new Vector<>(test.subList(0,test.size()/2));
        System.out.println(test1);
        Vector<Integer> test2 = new Vector<>(test.subList(test.size()/2,test.size()));
        System.out.println(test2);
    }
}

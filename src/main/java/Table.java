import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable, Comparable {
    private String name;
    private int numberOfPages;
    private Vector<String> pageNames;
    private Vector<Comparable> minPageValue;
    private Hashtable<String, Vector<String>> overflow;

    public Table(String name) {
        this.name = name;
        numberOfPages = 0;
        pageNames = new Vector<String>();
        minPageValue = new Vector<Comparable>();
        overflow = new Hashtable<>();
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
        return this.getName().compareTo(((Table)o).getName());
    }
}

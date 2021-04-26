import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable, Comparable {
    private String name;
    private int numberOfPages;
    private Vector<String> pageNames;
    private Vector<Comparable> minPageValue;
    private Hashtable<String, Vector<String>> overflow;
    private Hashtable<String, Vector<Integer>> overflowSizes;
    // To be done : count element number in pages


    public Table(String name) {
        this.name = name;
        this.numberOfPages = 0;
        this.pageNames = new Vector<>();
        this.minPageValue = new Vector<>();
        this.overflow = new Hashtable<>();
        this.overflowSizes = new Hashtable<>();
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
        Integer key = 0;
        Vector<Integer>  keysInPage  = new Vector<>();
        keysInPage.addElement(0);
        keysInPage.addElement(1);
        keysInPage.addElement(2);
        keysInPage.addElement(3);
        keysInPage.addElement(4);
        keysInPage.addElement(5);
        int lo = 0;
        int hi = keysInPage.size() - 1;
        int i = (lo + hi) / 2;
        boolean f = false;
        while (lo < hi) {
            if (key.compareTo(keysInPage.elementAt(i)) == 0) {
                System.out.println(i);
                f=true;
                break;
            } else if (key.compareTo(keysInPage.elementAt(i)) < 0) {
                hi = i - 1;
            } else {
                lo = i + 1;
            }

            i = (lo + hi) / 2;
        }
        if (!f && key.compareTo(keysInPage.elementAt(i)) == 0) {
            System.out.println(i);
        }
        System.out.println(-1);


    }

}

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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

    public static void main(String[] args) {
        Object value = new Date();
        Class type = value.getClass();
        System.out.println("Type: "+ type.getName());
        Constructor constructor = null;
        try {

            constructor = type.getConstructor(String.class);

            Object min = constructor.newInstance("2011-02-02");
            Object max = constructor.newInstance("2013-02-02");
            SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
            Date dvalue = sdformat.parse(String.valueOf(value));
            Date dmin = null;
            dmin = sdformat.parse(String.valueOf(min));

            Date dmax = sdformat.parse(String.valueOf(max));
            if (dvalue.compareTo(dmax) > 0) {
                System.out.println("Date inserted Occurs after maximum allowable Date for column: ");
            } else if (dvalue.compareTo(dmin) < 0) {
                System.out.println("Date inserted Occurs before minimum allowable Date for column: ");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
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

}

import java.io.*;
import java.lang.reflect.Constructor;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;

public class DBApp implements DBAppInterface {
    static int N = 0;
    HashMap<String, HashMap<String, String>> tableData = new HashMap<>();

    public static void main(String[] args) throws DBAppException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp();
//        Hashtable htblColNameType = new Hashtable();
//
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.Double");
//
//        Hashtable min = new Hashtable();
//        min.put("id", "0");
//        min.put("name", "a");
//        min.put("gpa", "0.7");
//
//        Hashtable max = new Hashtable();
//        max.put("id", "9");
//        max.put("name", "z");
//        max.put("gpa", "4");
//
//
//
//        dbApp.createTable(strTableName, "id", htblColNameType, min, max);

//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("id", new Integer(2343432));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//       dbApp.insertIntoTable(strTableName, htblColNameValue);

//        Hashtable<String,Integer> hs = new Hashtable();
//        System.out.println(hs.get("sadasfd"));


    }

    @Override
    public void init() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is ;
        try {
            is = new FileInputStream(fileName);
            prop.load(is);
            N = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
            csvWriter.append("Table Name");
            csvWriter.append(",");
            csvWriter.append("Column Name");
            csvWriter.append(",");
            csvWriter.append("Column Type");
            csvWriter.append(",");
            csvWriter.append("ClusteringKey");
            csvWriter.append(",");
            csvWriter.append("Indexed");
            csvWriter.append(",");
            csvWriter.append("min");
            csvWriter.append(",");
            csvWriter.append("max");
            csvWriter.append("\n");
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        if (!tableExists(tableName)) {
            for (String s : colNameType.keySet()) {
                switch (colNameType.get(s)) {
                    case "java.lang.Integer":
                    case "java.lang.String":
                    case "java.lang.Double":
                    case "java.util.Date":
                        continue;
                    default:
                        throw new DBAppException("Column " + s + " has an invalid datatype.");
                }
            }

            try {
                FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
                for (String s : colNameType.keySet()) {
                    csvWriter.append(tableName);
                    csvWriter.append(",");
                    csvWriter.append(s);
                    csvWriter.append(",");
                    csvWriter.append(colNameType.get(s));
                    csvWriter.append(",");
                    csvWriter.append(clusteringKey.equals(s) ? "true" : "false");
                    csvWriter.append(",");
                    csvWriter.append("false");
                    csvWriter.append(",");
                    csvWriter.append(colNameMin.get(s));
                    csvWriter.append(",");
                    csvWriter.append(colNameMax.get(s));
                    csvWriter.append("\n");
                    Table temp = new Table(tableName);
                    serializeTableInfo(tableName, temp);
                }
                csvWriter.flush();
                csvWriter.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        } else
            throw new DBAppException("Table name exists before");

    }

    // Checking if a table exists in the database
    private boolean tableExists(String tableName) {
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String current = br.readLine();
            while (current != null) {
                String[] line = current.split(",");
                if (line[0].equals(tableName)) {
                    return true;
                }
                current = br.readLine();
            }
            br.close();
        } catch (FileNotFoundException e) {
            System.out.println("File is not right :(");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        if (tableExists(tableName)) {
            Hashtable<String, String> colType = new Hashtable();
            Hashtable<String, String> colmin = new Hashtable();
            Hashtable<String, String> colmax = new Hashtable();
            String primaryKey = "";

            try {
                BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String current = br.readLine();
                while (current != null) {
                    String[] line = current.split(",");
                    if (line[0].equals(tableName)) {
                        do {
                            if (line[3].equals("true")) {
                                primaryKey = line[1];
                            }
                            colType.put(line[1], line[2]);
                            colmin.put(line[1], line[5]);
                            colmax.put(line[1], line[6]);
                            current = br.readLine();
                            if (current != null) {
                                line = current.split(",");
                            }
                        } while (current != null && line[0].equals(tableName));
                        break;
                    }
                    current = br.readLine();
                }
                br.close();

            } catch (FileNotFoundException e) {
                System.out.println("File is not right :(");
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (!(colNameValue.keySet()).contains(primaryKey)) {
                throw new DBAppException("Primary Key Should Have a Value");
            }
            for (String columnName : colNameValue.keySet()) {
                Object value = colNameValue.get(columnName);
                if (colType.get(columnName) == null) {
                    throw new DBAppException("Column Does not Exist");
                }
                Class type = value.getClass();
                if (!(type.getName().equals(colType.get(columnName)))) {
                    throw new DBAppException("Type Miss-match of Column: " + columnName);
                }
                Object min, max;
                try {
                    Constructor constr = type.getConstructor(String.class);
                    min = constr.newInstance(colmin.get(columnName));
                    max = constr.newInstance(colmax.get(columnName));
                    if (value instanceof java.lang.Integer) {
                        int zvalue = (int) (value);
                        int zmin = (int) (min);
                        if (zvalue < zmin) {
                            throw new DBAppException("Value Inserted is less than minimum allowed" + columnName);

                        }
                        int zmax = (int) (max);
                        if (zvalue > zmax) {
                            throw new DBAppException("Value inserted is larger than maximum allowed" + columnName);
                        }


                    } else if (value instanceof java.lang.Double) {
                        double zvalue = (double) (value);
                        double zmin = (double) (min);
                        if (zvalue < zmin) {
                            throw new DBAppException("Value Inserted is less than minimum allowed for" + columnName);
                        }
                        double zmax = (double) (max);
                        if (zvalue > zmax) {
                            throw new DBAppException("Value inserted is larger than maximum allowed for" + columnName);
                        }
                    } else if (value instanceof java.lang.String) {
                        String svalue = (String) (value);
                        String smin = (String) (min);
                        if ((svalue.compareTo(smin)) < 0) {
                            throw new DBAppException("Value Inserted is less than minimum allowed for" + columnName);
                        }
                        if (smin.length() > svalue.length()) {
                            throw new DBAppException("Value length Inserted is less than minimum allowed for" + columnName);
                        }
                        String smax = (String) (max);
                        if ((svalue.compareTo(smax)) > 0) {
                            throw new DBAppException("Value inserted is larger than maximum allowed for" + columnName);
                        }
                        if (smax.length() < svalue.length()) {
                            throw new DBAppException("Value length Inserted is larger than minimum allowed for" + columnName);
                        }
                    } else if (value instanceof java.util.Date) {
                        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
                        Date dvalue = sdformat.parse(String.valueOf(value));
                        Date dmin = sdformat.parse(String.valueOf(min));
                        Date dmax = sdformat.parse(String.valueOf(max));
                        if (dvalue.compareTo(dmax) > 0) {
                            throw new DBAppException("Date inserted Occurs after maximum allowable Date");
                        } else if (dvalue.compareTo(dmin) < 0) {
                            throw new DBAppException("Date inserted Occurs before minimum allowable Date");
                        }
                    }
                } catch (Exception e) {
                }
            }
            Hashtable<String, Comparable> allColValues = new Hashtable<String, Comparable>();
            for (String s : colType.keySet()) {
                allColValues.put(s, (Comparable) colNameValue.get(s));
            }
            //CREATE tuple to be inserted
            Tuple newEntry = new Tuple((Comparable) allColValues.get(primaryKey), allColValues);
            //FETCH Table info
            Table t = deserializeTableInfo(tableName);
            if (t.getNumberOfPages() == 0) {
                try {
                    Vector<Tuple> newPageBody = new Vector<>();
                    newPageBody.addElement(newEntry);
                    String newPageName = createPage(t.getName());
                    t.getPageNames().addElement(newPageName);
                    serializePage(newPageName, newPageBody);
                    t.setNumberOfPages(1);
                    t.getMinPageValue().addElement(newEntry.getClusteringKey());
                    serializeTableInfo(tableName, t);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                int pageIndex = getPageIndex(newEntry.getClusteringKey(), t.getMinPageValue(), t.getNumberOfPages());
                String pageName = (String) t.getPageNames().elementAt(pageIndex);
                Vector<Tuple> page = deserializePage(pageName);
                int keyIndex = getKeyIndex(newEntry.getClusteringKey(), page);
                if (keyIndex != -1) {
                    throw new DBAppException("Primary Key Already Exists");
                }
                page.addElement(newEntry);
                Collections.sort(page);
                if (page.size() > N) {
                    Tuple temp = page.lastElement();
                    page.removeElementAt(N);
                    if (pageIndex < t.getNumberOfPages() - 1) {
                        Vector<Tuple> nextPage = deserializePage((String) t.getPageNames().elementAt(pageIndex));
                        if (nextPage.size() < N) {
                            nextPage.addElement(temp);
                            Collections.sort(nextPage);
                            serializePage((String) t.getPageNames().elementAt(pageIndex), nextPage);
                        } else {
                            //OverFlow Page Linkage
                        }
                    } else {
                        Vector<Tuple> newPageBody = new Vector<>();
                        newPageBody.addElement(temp);
                        try {
                            String newPageName = createPage(t.getName());
                            t.getPageNames().addElement(newPageName);
                            serializePage(newPageName, newPageBody);
                            t.setNumberOfPages(1);
                            t.getMinPageValue().addElement(temp.getClusteringKey());
                            serializeTableInfo(tableName, t);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                serializePage(pageName, page);
            }
        } else {
            throw new DBAppException("Table Does Not Exist");
        }
    }

    public int getKeyIndex(Comparable key, Vector<Tuple> keysInPage) {
        int lo = 0;
        int hi = keysInPage.size() - 1;
        int i = (lo + hi) / 2;

        while (lo < hi) {
            if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) == 0) {
                return i;
            } else if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) > 0) {
                hi = i - 1;
            } else {
                lo = i + 1;
            }

            i = (lo + hi) / 2;
        }
        if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) == 0) {
            return i;
        }
        return -1;
    }


    public int getPageIndex(Comparable key, Vector<Comparable> minimumValueInPage, int numberOfPages) {
        int lo = 0, hi = numberOfPages;
        int i = (lo + hi) / 2;
        while (lo < hi) {
            if (i != numberOfPages - 1) {
                if (((Comparable) (minimumValueInPage.elementAt(i))).compareTo(key) > 0) {
                    hi = i - 1;
                } else {
                    if (i != numberOfPages - 1) {
                        if (((Comparable) (minimumValueInPage.elementAt(i + 1))).compareTo(key) > 0) {
                            return i;
                        } else {
                            lo = hi + 1;
                        }
                    }
                }
            }
            i = (lo + hi) / 2;
        }
        return i;
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (tableExists(tableName)) {


        } else {
            throw new DBAppException("Table Does Not Exist");
        }
    }


    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        String clusteringKeyValue = validateInput(tableName, columnNameValue, false);
        Table table = deserializeTableInfo(tableName);
        Vector<String> pageNames = table.getPageNames();
        ArrayList<String> pagesToDelete = new ArrayList<>();
        //Looping over all pages
        page:
        for (String pageName : pageNames) {
            //OverFlow to be done
            Vector<Tuple> page = deserializePage(pageName);
            tuple:
            //Looping over all tuples in a page
            for (int i = 0; i<page.size(); i++ ) {
                Tuple tuple = page.elementAt(i);
                Hashtable<String, Comparable> entries = tuple.getEntries();
                //Looping over every column in a tuple
                field:
                for (String columnName : columnNameValue.keySet()) {
                    if (((Comparable) columnNameValue.get(columnName)).compareTo(entries.get(columnName)) != 0)
                        continue tuple;
                }
                //delete row:
                page.removeElementAt(i);
            }
            //update the page:
            if(page.isEmpty()){
                //delete the page:
                pagesToDelete.add(pageName);
            }
            else{
                //page is not empty
                int i = pageNames.indexOf(pageName);
                table.getMinPageValue().setElementAt( page.firstElement().getClusteringKey(),i);
                serializePage(pageName,page);
            }
        }
        for(String s : pagesToDelete){
            File f = new File("src/main/Pages/"+s+".class");
            f.delete();
            int i = pageNames.indexOf(s);
            pageNames.removeElementAt(i);
            table.getMinPageValue().removeElementAt(i);
            table.setNumberOfPages(-1);
        }
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    public String createPage(String TableName) throws IOException {
        LocalDateTime now = LocalDateTime.now();
        String pageName = TableName + now.getYear() + now.getDayOfYear() + now.getHour() + now.getMinute() + now.getSecond();
        try {
            ObjectOutputStream o = new
                    ObjectOutputStream(
                    new FileOutputStream("src/main/Pages/" + pageName + ".class"));
            o.close();
        } catch (IOException e) {
            System.out.println(e);
        }
        return pageName;
    }

    private void serializeTableInfo(String name, Table t) {

        try {
            FileOutputStream fileout = new FileOutputStream("src/main/TableInfo/" + name + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileout);
            out.writeObject(t);
            out.close();
            fileout.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Table deserializeTableInfo(String name) {
        Table t = null;
        try {
            FileInputStream filein = new FileInputStream("src/main/TableInfo/" + name + ".class");
            ObjectInputStream in = new ObjectInputStream(filein);
            t = (Table) in.readObject();
            in.close();
            filein.close();
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }

        return t;
    }

    private void serializePage(String name, Vector<Tuple> pageBody) {
        try {
            FileOutputStream fileout = new FileOutputStream("src/main/Pages/" + name + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileout);
            out.writeObject(pageBody);
            out.close();
            fileout.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public Vector<Tuple> deserializePage(String name) {
        Vector<Tuple> v = null;
        try {
            FileInputStream filein = new FileInputStream("src/main/TableInfo/" + name + ".class");
            ObjectInputStream in = new ObjectInputStream(filein);
            v = (Vector<Tuple>) in.readObject();
            in.close();
            filein.close();
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }

        return v;
    }

    public String validateInput(String tableName, Hashtable<String, Object> colNameValue, boolean checkPrimaryKey) throws DBAppException {
        if (tableExists(tableName)) {
            Hashtable<String, String> colType = new Hashtable();
            Hashtable<String, String> colmin = new Hashtable();
            Hashtable<String, String> colmax = new Hashtable();
            String primaryKey = "";

            try {
                BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String current = br.readLine();
                while (current != null) {
                    String[] line = current.split(",");
                    if (line[0].equals(tableName)) {
                        do {
                            if (line[3].equals("true")) {
                                primaryKey = line[1];
                            }
                            colType.put(line[1], line[2]);
                            colmin.put(line[1], line[5]);
                            colmax.put(line[1], line[6]);
                            current = br.readLine();
                            if (current != null) {
                                line = current.split(",");
                            }
                        } while (current != null && line[0].equals(tableName));
                        break;
                    }
                    current = br.readLine();
                }
                br.close();

            } catch (FileNotFoundException e) {
                System.out.println("File is not right :(");
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (checkPrimaryKey) {
                if (!(colNameValue.keySet()).contains(primaryKey)) {
                    throw new DBAppException("Primary Key Should Have a Value");
                }
            }
            for (String columnName : colNameValue.keySet()) {
                Object value = colNameValue.get(columnName);
                if (colType.get(columnName) == null) {
                    throw new DBAppException("Column Does not Exist");
                }
                Class type = value.getClass();
                if (!(type.getName().equals(colType.get(columnName)))) {
                    throw new DBAppException("Type Miss-match of Column: " + columnName);
                }
                Object min, max;
                try {
                    Constructor constr = type.getConstructor(String.class);
                    min = constr.newInstance(colmin.get(columnName));
                    max = constr.newInstance(colmax.get(columnName));
                    if (value instanceof java.lang.Integer) {
                        int zvalue = (int) (value);
                        int zmin = (int) (min);
                        if (zvalue < zmin) {
                            throw new DBAppException("Value Inserted is less than minimum allowed" + columnName);

                        }
                        int zmax = (int) (max);
                        if (zvalue > zmax) {
                            throw new DBAppException("Value inserted is larger than maximum allowed" + columnName);
                        }


                    } else if (value instanceof java.lang.Double) {
                        double zvalue = (double) (value);
                        double zmin = (double) (min);
                        if (zvalue < zmin) {
                            throw new DBAppException("Value Inserted is less than minimum allowed for" + columnName);
                        }
                        double zmax = (double) (max);
                        if (zvalue > zmax) {
                            throw new DBAppException("Value inserted is larger than maximum allowed for" + columnName);
                        }
                    } else if (value instanceof java.lang.String) {
                        String svalue = (String) (value);
                        String smin = (String) (min);
                        if ((svalue.compareTo(smin)) < 0) {
                            throw new DBAppException("Value Inserted is less than minimum allowed for" + columnName);
                        }
                        if (smin.length() > svalue.length()) {
                            throw new DBAppException("Value length Inserted is less than minimum allowed for" + columnName);
                        }
                        String smax = (String) (max);
                        if ((svalue.compareTo(smax)) > 0) {
                            throw new DBAppException("Value inserted is larger than maximum allowed for" + columnName);
                        }
                        if (smax.length() < svalue.length()) {
                            throw new DBAppException("Value length Inserted is larger than minimum allowed for" + columnName);
                        }
                    } else if (value instanceof java.util.Date) {
                        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
                        Date dvalue = sdformat.parse(String.valueOf(value));
                        Date dmin = sdformat.parse(String.valueOf(min));
                        Date dmax = sdformat.parse(String.valueOf(max));
                        if (dvalue.compareTo(dmax) > 0) {
                            throw new DBAppException("Date inserted Occurs after maximum allowable Date");
                        } else if (dvalue.compareTo(dmin) < 0) {
                            throw new DBAppException("Date inserted Occurs before minimum allowable Date");
                        }
                    }
                } catch (Exception e) {

                }
                return primaryKey;
            }
        } else {
            throw new DBAppException("Table Does Not Exist");
        }
        return null;
    }

}
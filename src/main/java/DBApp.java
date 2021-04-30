import java.io.*;
import java.lang.reflect.Constructor;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class DBApp implements DBAppInterface {
    static int N = 0;


    public void printAllTuplesOfTable(String name) {
        //Check the value of pages
        Table t = deserializeTableInfo(name);
        Vector<String> pNames = t.getPageNames();
        System.out.println(pNames);
        for (String s : pNames) {
            System.out.println("Page Name: " + s);
            System.out.println(deserializePage(s));
        }
    }

    @Override
    public void init() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is;
        try {
            is = new FileInputStream(fileName);
            prop.load(is);
            N = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            if (!br.ready()) {
                br.close();
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
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        File file = new File("src/main/resources/data");
        if (!file.exists()) {
            file.mkdir();
        }
        //Creating the directory


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
                    createTableInfo(tableName);
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
            Hashtable<String, String> colType = new Hashtable<>();
            Hashtable<String, String> colMin = new Hashtable<>();
            Hashtable<String, String> colMax = new Hashtable<>();
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
                            colMin.put(line[1], line[5]);
                            colMax.put(line[1], line[6]);
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
            if (!(colNameValue).containsKey(primaryKey)) {
                throw new DBAppException("Primary Key Should Have a Value");
            }
            for (String columnName : colNameValue.keySet()) {
                if (colType.get(columnName) == null) {
                    throw new DBAppException("Column Does not Exist");
                }
                Object value = colNameValue.get(columnName);
                Class type = value.getClass();
                if (!(type.getName().equals(colType.get(columnName)))) {
                    throw new DBAppException("Type Miss-match for Column: " + columnName + " , a " + type.getName() + " type should be inserted");
                }
                if (type.getName().charAt(11) == 'a') {
                    try {
                        Date dvalue =(Date)value;
                        Date dmin = new SimpleDateFormat("yyyy-MM-dd").parse(colMin.get(columnName));
                        Date dmax = new SimpleDateFormat("yyyy-MM-dd").parse(colMax.get(columnName));
                        if (dvalue.compareTo(dmax) > 0) {
                            throw new DBAppException("Date inserted Occurs after maximum allowable Date for column: " + columnName);
                        } else if (dvalue.compareTo(dmin) < 0) {
                            throw new DBAppException("Date inserted Occurs before minimum allowable Date for column: " + columnName);
                        }
                    } catch (ParseException e) {
                        throw new DBAppException(e.getMessage());
                    }

                } else {

                    try {
                        Object min, max;
                        Constructor constructor = type.getConstructor(String.class);
                        min = constructor.newInstance(colMin.get(columnName));
                        max = constructor.newInstance(colMax.get(columnName));
                        if (value instanceof java.lang.Integer) {
                            int zvalue = (int) (value);
                            int zmin = (int) (min);
                            if (zvalue < zmin) {
                                throw new DBAppException("Value Inserted is less than minimum allowed column: " + columnName);
                            }
                            int zmax = (int) (max);
                            if (zvalue > zmax) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }
                        } else if (value instanceof java.lang.Double) {
                            double zvalue = (double) (value);
                            double zmin = (double) (min);
                            if (zvalue < zmin) {
                                throw new DBAppException("Value Inserted is less than minimum value allowed for column: " + columnName);
                            }
                            double zmax = (double) (max);
                            if (zvalue > zmax) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }
                        } else if (value instanceof java.lang.String) {
                            String svalue = (String) (value);
                            String smin = (String) (min);
                            if ((svalue.compareTo(smin)) < 0) {
                                throw new DBAppException("Value Inserted is less than minimum value allowed for column: " + columnName);
                            }
                            String smax = (String) (max);
                            if ((svalue.compareTo(smax)) > 0) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }

                        }
                    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }
            Hashtable<String, Comparable> allColValues = new Hashtable<>();
            for (String s : colType.keySet()) {
                allColValues.put(s, (Comparable) colNameValue.get(s));
            }
            //CREATE tuple to be inserted
            Tuple newEntry = new Tuple(allColValues.get(primaryKey), allColValues);
            //FETCH Table info
            Table table = deserializeTableInfo(tableName);
            //if no pages found for the Table
            if (table.getPageNames().size() == 0) {
                //Create the First Page
                try {
                    Vector<Tuple> newPageBody = new Vector<>();
                    newPageBody.addElement(newEntry);
                    String newPageName = createPage(table.getName());
                    table.getPageNames().addElement(newPageName);
                    table.getMinPageValue().addElement(newEntry.getClusteringKey());
                    serializePage(newPageName, newPageBody);
                    serializeTableInfo(tableName, table);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                //if Page(s) was found
                //get index where the range
                int pageIndex = getPageIndex(newEntry.getClusteringKey(), table.getMinPageValue());
                //page name
                String pageName = (String) table.getPageNames().elementAt(pageIndex);
                //Fetch the vector of the page (deserialize)
                Vector<Tuple> page = deserializePage(pageName);
                int keyIndex = getKeyIndex(newEntry.getClusteringKey(), page);
                if (keyIndex != -1) {
                    //check if primary key exist in main page
                    throw new DBAppException("Clustering Key Already Exists");
//                } else {
//                    //check if primary key exist in overflow pages
//                    if (table.getOverflow().get(pageName) != null) {
//                        for (String s : table.getOverflow().get(pageName)) {
//                            Vector<Tuple> ofpage = deserializePage(s);
//                            if (getKeyIndex(newEntry.getClusteringKey(), ofpage) != -1)
//                                throw new DBAppException("Clustering Key Already Exists");
//                        }
//                    }
                }
                if (page.size() < N) {
                    // if there is space in the main page:
                    page.insertElementAt(newEntry, indexToInsertAt(newEntry.getClusteringKey(), page));
                    //Collections.sort(page);
                    serializePage(pageName, page);
                    table.getMinPageValue().setElementAt(page.firstElement().getClusteringKey(), pageIndex);
                } else {
                    //Page is full
                    try {
                        page.insertElementAt(newEntry, indexToInsertAt(newEntry.getClusteringKey(), page));
                        Vector<Tuple> firstHalf = new Vector<>(page.subList(0, (page.size()) / 2));
                        Vector<Tuple> secondHalf = new Vector<>(page.subList((page.size()) / 2, page.size()));


                        String newHalfPageName = createPage(tableName);
                        table.getPageNames().insertElementAt(newHalfPageName, pageIndex + 1);
                        table.getMinPageValue().insertElementAt(0, pageIndex + 1);

                        table.getMinPageValue().setElementAt(firstHalf.elementAt(0).getClusteringKey(), pageIndex);
                        table.getMinPageValue().setElementAt(secondHalf.elementAt(0).getClusteringKey(), pageIndex + 1);
                        serializePage(pageName, firstHalf);
                        serializePage(newHalfPageName, secondHalf);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                serializeTableInfo(tableName, table);
            }
        } else {
            throw new DBAppException("Table Does Not Exist");
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> colNameValue) throws DBAppException {
        if (tableExists(tableName)) {
            Hashtable<String, String> colType = new Hashtable();
            Hashtable<String, String> colMin = new Hashtable();
            Hashtable<String, String> colMax = new Hashtable();
            String clusteringKeyType = "";

            try {
                BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String current = br.readLine();
                while (current != null) {
                    String[] line = current.split(",");
                    if (line[0].equals(tableName)) {
                        do {
                            if (line[3].equals("true")) {
                                clusteringKeyType = line[2];
                            }
                            colType.put(line[1], line[2]);
                            colMin.put(line[1], line[5]);
                            colMax.put(line[1], line[6]);
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
            for (String columnName : colNameValue.keySet()) {
                Object value = colNameValue.get(columnName);
                if (colType.get(columnName) == null) {
                    throw new DBAppException("Column Does not Exist");
                }
                Class type = value.getClass();
                if (!(type.getName().equals(colType.get(columnName)))) {
                    throw new DBAppException("Type Miss-match of Column: " + columnName);
                }
                if (type.getName().charAt(11) == 'a') {
                    try {
                        Date dvalue =(Date)value;
                        Date dmin = new SimpleDateFormat("yyyy-MM-dd").parse(colMin.get(columnName));
                        Date dmax = new SimpleDateFormat("yyyy-MM-dd").parse(colMax.get(columnName));
                        if (dvalue.compareTo(dmax) > 0) {
                            throw new DBAppException("Date inserted Occurs after maximum allowable Date for column: " + columnName);
                        } else if (dvalue.compareTo(dmin) < 0) {
                            throw new DBAppException("Date inserted Occurs before minimum allowable Date for column: " + columnName);
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                } else {


                    try {
                        Constructor constructor = type.getConstructor(String.class);
                        Object min = constructor.newInstance(colMin.get(columnName));
                        Object max = constructor.newInstance(colMax.get(columnName));
                        if (value instanceof java.lang.Integer) {
                            int zvalue = (int) (value);
                            int zmin = (int) (min);
                            if (zvalue < zmin) {
                                throw new DBAppException("Value Inserted is less than minimum allowed column: " + columnName);

                            }
                            int zmax = (int) (max);
                            if (zvalue > zmax) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }


                        } else if (value instanceof java.lang.Double) {
                            double zvalue = (double) (value);
                            double zmin = (double) (min);
                            if (zvalue < zmin) {
                                throw new DBAppException("Value Inserted is less than minimum value allowed for column: " + columnName);
                            }
                            double zmax = (double) (max);
                            if (zvalue > zmax) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }
                        } else if (value instanceof java.lang.String) {
                            String svalue = (String) (value);
                            String smin = (String) (min);
                            if ((svalue.compareTo(smin)) < 0) {
                                throw new DBAppException("Value Inserted is less than minimum value allowed for column: " + columnName);
                            }

                            String smax = (String) (max);
                            if ((svalue.compareTo(smax)) > 0) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }

                        }

                    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }

            Table table = deserializeTableInfo(tableName);
            if (table.getPageNames().size() == 0) {
                throw new DBAppException("This Table has no records to update");

            } else {
                switch (clusteringKeyType) {
                    case "java.lang.Integer": {
                        Integer primary = Integer.parseInt(clusteringKeyValue);
                        int pageIndex = getPageIndex(primary, table.getMinPageValue());
                        String pageName = table.getPageNames().elementAt(pageIndex);
                        Vector<Tuple> page = deserializePage(pageName);
                        int keyIndex = getKeyIndex(primary, page);
                        if (keyIndex == -1) {
                            throw new DBAppException("Record doesn't exist");
                        }
                        Hashtable<String, Comparable> allColValues = page.elementAt(keyIndex).getEntries();
                        for (String s : colNameValue.keySet()) {
                            allColValues.put(s, (Comparable) colNameValue.get(s));
                        }
                        page.elementAt(keyIndex).setEntries(allColValues);
                        serializePage(pageName, page);
                        break;
                    }
                    case "java.lang.Double": {
                        Double primary = Double.parseDouble(clusteringKeyValue);
                        int pageIndex = getPageIndex(primary, table.getMinPageValue());
                        String pageName = table.getPageNames().elementAt(pageIndex);
                        Vector<Tuple> page = deserializePage(pageName);
                        int keyIndex = getKeyIndex((Comparable) primary, page);
                        if (keyIndex == -1) {
                            throw new DBAppException("Record doesn't exist");
                        }
                        Hashtable<String, Comparable> allColValues = page.elementAt(keyIndex).getEntries();
                        for (String s : colNameValue.keySet()) {
                            allColValues.put(s, (Comparable) colNameValue.get(s));
                        }


                        page.elementAt(keyIndex).setEntries(allColValues);
                        serializePage(pageName, page);
                        break;
                    }
                    case "java.lang.String": {
                        int pageIndex = getPageIndex(clusteringKeyValue, table.getMinPageValue());
                        String pageName = table.getPageNames().elementAt(pageIndex);
                        Vector<Tuple> page = deserializePage(pageName);
                        int keyIndex = getKeyIndex(clusteringKeyValue, page);
                        if (keyIndex == -1) {
                            throw new DBAppException("Record doesn't exist");
                        }
                        Hashtable<String, Comparable> allColValues = page.elementAt(keyIndex).getEntries();
                        for (String s : colNameValue.keySet()) {
                            allColValues.put(s, (Comparable) colNameValue.get(s));
                        }
                        page.elementAt(keyIndex).setEntries(allColValues);
                        serializePage(pageName, page);
                        break;
                    }
                    case "java.util.Date":
                        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            Date primary = sdformat.parse(clusteringKeyValue);
                            int pageIndex = getPageIndex(primary, table.getMinPageValue());
                            String pageName = table.getPageNames().elementAt(pageIndex);
                            Vector<Tuple> page = deserializePage(pageName);
                            int keyIndex = getKeyIndex(primary, page);
                            if (keyIndex == -1) {
                                throw new DBAppException("Record doesn't exist");
                            }
                            Hashtable<String, Comparable> allColValues = page.elementAt(keyIndex).getEntries();
                            for (String s : colNameValue.keySet()) {
                                allColValues.put(s, (Comparable) colNameValue.get(s));
                            }
                            page.elementAt(keyIndex).setEntries(allColValues);
                            serializePage(pageName, page);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        break;
                }
            }
        } else {
            throw new DBAppException("Table Does not Exist");
        }

    }


    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Comparable clusteringValue = validateInput(tableName, columnNameValue);
        Table table = deserializeTableInfo(tableName);
        Vector<String> pageNames = table.getPageNames();
        //Looping over all pages
        if (!pageNames.isEmpty()) {
            //if clustering value is not in the input
            if (clusteringValue == null) {
                ArrayList<String> pagesToDelete = new ArrayList<>();
                boolean deleted = false;
                for (String pageName : pageNames) {
                    //OverFlow to be done
                    Vector<Tuple> page = deserializePage(pageName);
                    tuple:
                    //Looping over all tuples in a page
                    for (int i = 0; i < page.size(); i++) {
                        Tuple tuple = page.elementAt(i);
                        Hashtable<String, Comparable> entries = tuple.getEntries();
                        //Looping over every column in a tuple
                        for (String columnName : columnNameValue.keySet()) {
                            if (((Comparable) columnNameValue.get(columnName)).compareTo(entries.get(columnName)) != 0)
                                continue tuple;
                        }
                        //delete row:
                        page.removeElementAt(i);
                        deleted = true;
                    }
                    //update the page:
                    if (page.isEmpty()) {
                        //delete the page:
                        pagesToDelete.add(pageName);
                    } else {
                        //page is not empty
                        int i = pageNames.indexOf(pageName);
                        table.getMinPageValue().setElementAt(page.firstElement().getClusteringKey(), i);
                        serializePage(pageName, page);
                    }
                }
                for (String s : pagesToDelete) {
                    File f = new File("src/main/resources/data/" + s + ".class");
                    f.delete();
                    int i = pageNames.indexOf(s);
                    pageNames.removeElementAt(i);
                    table.getMinPageValue().removeElementAt(i);
                }
                if (!deleted) {
                    throw new DBAppException("No record to delete");
                }
            } else {
                // If the Clustering Key is Known
                int pageIndex = getPageIndex(clusteringValue, table.getMinPageValue());
                String pageName = pageNames.elementAt(pageIndex);
                Vector<Tuple> mainPageBody = deserializePage(pageName);
                int keyIndex = getKeyIndex(clusteringValue, mainPageBody);
                if (keyIndex != -1) {
                    mainPageBody.removeElementAt(keyIndex);
                    if (mainPageBody.isEmpty()) {
                        pageNames.removeElementAt(pageIndex);
                        table.getMinPageValue().removeElementAt(pageIndex);
                        File f = new File("src/main/resources/data/" + pageName + ".class");
                        f.delete();
                    } else {
                        table.getMinPageValue().setElementAt(mainPageBody.firstElement().getClusteringKey(), pageIndex);
                        serializePage(pageName, mainPageBody);
                    }
                } else {
                    throw new DBAppException("Clustering key does not exist");
                }
            }
            serializeTableInfo(tableName, table);
        } else {
            throw new DBAppException("No records to delete from");
        }
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    public void createTableInfo(String TableName) {
        try {
            ObjectOutputStream o = new
                    ObjectOutputStream(
                    new FileOutputStream("src/main/resources/data/" + TableName + ".class"));
            o.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String createPage(String TableName) throws IOException {
        LocalDateTime now = LocalDateTime.now();
        String pageName = TableName + now.getDayOfYear() + now.getHour() + now.getMinute() + now.getSecond() + now.getNano();
        try {
            //bellow line used to avoid issue when java freezes the time
            Thread.sleep(3);
            ObjectOutputStream o = new
                    ObjectOutputStream(
                    new FileOutputStream("src/main/resources/data/" + pageName + ".class"));
            o.close();
        } catch (IOException | InterruptedException e) {
            System.out.println(e);
        }

        return pageName;
    }

    private void serializeTableInfo(String name, Table t) {

        try {
            FileOutputStream fileout = new FileOutputStream("src/main/resources/data/" + name + ".class");
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
            FileInputStream filein = new FileInputStream("src/main/resources/data/" + name + ".class");
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
            FileOutputStream fileout = new FileOutputStream("src/main/resources/data/" + name + ".class");
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
            FileInputStream filein = new FileInputStream("src/main/resources/data/" + name + ".class");
            ObjectInputStream in = new ObjectInputStream(filein);
            v = (Vector<Tuple>) in.readObject();
            in.close();
            filein.close();
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }

        return v;
    }

    public int getKeyIndex(Comparable key, Vector<Tuple> keysInPage) {
        int lo = 0;
        int hi = keysInPage.size() - 1;
        int i;
        while (lo <= hi) {
            i = (lo + hi) / 2;
            if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) == 0) {
                return i;
            } else if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) < 0) {
                hi = i - 1;
            } else {
                lo = i + 1;
            }
        }
        return -1;
    }


    public int getPageIndex(Comparable key, Vector<Comparable> minimumValueInPage) {
        int lo = 0;
        int hi = minimumValueInPage.size() - 1;
        int i = -1;
        while (lo <= hi) {
            i = (lo + hi) / 2;
            if (key.compareTo(minimumValueInPage.elementAt(i)) < 0) {
                hi = i - 1;
            } else {
                if (i != minimumValueInPage.size() - 1) {
                    if (key.compareTo(minimumValueInPage.elementAt(i + 1)) < 0) {
                        return i;
                    } else {
                        lo = i + 1;
                    }
                } else {
                    return i;
                }
            }
        }
        return i;
    }

    public int indexToInsertAt(Comparable key, Vector<Tuple> keysInPage) {
        int lo = 0;
        int hi = keysInPage.size() - 1;
        while (lo <= hi) {
            int i = (lo + hi) / 2;
            if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) < 0) {
                hi = i - 1;
            } else {
                lo = i + 1;
            }
        }
        return lo;
    }

    public Comparable validateInput(String tableName, Hashtable<String, Object> colNameValue) throws
            DBAppException {
        if (tableExists(tableName)) {
            Hashtable<String, String> colType = new Hashtable();
            Hashtable<String, String> colMin = new Hashtable();
            Hashtable<String, String> colMax = new Hashtable();
            String clusteringKeyColumn = "";

            try {
                BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String current = br.readLine();
                while (current != null) {
                    String[] line = current.split(",");
                    if (line[0].equals(tableName)) {
                        do {
                            if (line[3].equals("true")) {
                                clusteringKeyColumn = line[1];
                            }
                            colType.put(line[1], line[2]);
                            colMin.put(line[1], line[5]);
                            colMax.put(line[1], line[6]);
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
            for (String columnName : colNameValue.keySet()) {
                Object value = colNameValue.get(columnName);
                if (colType.get(columnName) == null) {
                    throw new DBAppException("Column Does not Exist");
                }
                Class type = value.getClass();
                if (!(type.getName().equals(colType.get(columnName)))) {
                    throw new DBAppException("Type Miss-match of Column: " + columnName);
                }
                if (type.getName().charAt(11) == 'a') {
                    try {
                        Date dvalue =(Date)value;
                        Date dmin = new SimpleDateFormat("yyyy-MM-dd").parse(colMin.get(columnName));
                        Date dmax = new SimpleDateFormat("yyyy-MM-dd").parse(colMax.get(columnName));
                        if (dvalue.compareTo(dmax) > 0) {
                            throw new DBAppException("Date inserted Occurs after maximum allowable Date for column: " + columnName);
                        } else if (dvalue.compareTo(dmin) < 0) {
                            throw new DBAppException("Date inserted Occurs before minimum allowable Date for column: " + columnName);
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                } else {
                    try {
                        Constructor constructor = type.getConstructor(String.class);
                        Object min = constructor.newInstance(colMin.get(columnName));
                        Object max = constructor.newInstance(colMax.get(columnName));
                        if (value instanceof java.lang.Integer) {
                            int zvalue = (int) (value);
                            int zmin = (int) (min);
                            if (zvalue < zmin) {
                                throw new DBAppException("Value Inserted is less than minimum allowed column: " + columnName);

                            }
                            int zmax = (int) (max);
                            if (zvalue > zmax) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }


                        } else if (value instanceof java.lang.Double) {
                            double zvalue = (double) (value);
                            double zmin = (double) (min);
                            if (zvalue < zmin) {
                                throw new DBAppException("Value Inserted is less than minimum value allowed for column: " + columnName);
                            }
                            double zmax = (double) (max);
                            if (zvalue > zmax) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }
                        } else if (value instanceof java.lang.String) {
                            String svalue = (String) (value);
                            String smin = (String) (min);
                            if ((svalue.compareTo(smin)) < 0) {
                                throw new DBAppException("Value Inserted is less than minimum value allowed for column: " + columnName);
                            }

                            String smax = (String) (max);
                            if ((svalue.compareTo(smax)) > 0) {
                                throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                            }
                        }
                    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }
            return (Comparable) colNameValue.get(clusteringKeyColumn);
        } else {
            throw new DBAppException("Table Does Not Exist");
        }
    }

}
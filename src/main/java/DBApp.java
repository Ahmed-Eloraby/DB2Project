import java.io.*;
import java.lang.reflect.Constructor;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class DBApp implements DBAppInterface {
    static int N = 0;
    static int B = 0;

    public static void main(String[] args) throws DBAppException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp();
        dbApp.init();
        Hashtable htblColNameType = new Hashtable();
        Hashtable htblColNameMin = new Hashtable();
        Hashtable htblColNameMax = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "a");
        htblColNameMin.put("gpa", "0.7");
        htblColNameMax.put("id", "999999");
        htblColNameMax.put("name", "zzzzzzzzzzz");
        htblColNameMax.put("gpa", "4.0");
        dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "Student";
        arrSQLTerms[0]._strColumnName = "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "John Noor";
        arrSQLTerms[1]._strTableName = "Student";
        arrSQLTerms[1]._strColumnName = "gpa";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = new Integer(1);
        String[] strarrOperators = new String[1];
        strarrOperators[0] = "OR";
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);

    }

    public void printAllTuplesOfTable(String name) {
        //Check the  tuples in all pages
        Table t = deserializeTableInfo(name);
        Vector<String> pNames = t.getPageNames();
        System.out.println(pNames);
        System.out.println("Min: \n" + t.getMinPageValue());
        int n = 0;
        for (String s : pNames) {
            System.out.println("Page Name: " + s);
            Vector<Tuple> v = deserializePage(s);
            System.out.println(v);
            System.out.println("Number of Tuples in " + s + "Page = " + v.size());
            n += v.size();
        }
        System.out.println("Total number of Tuples = " + n);
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
            B = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
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
        if (tableName.equals("")) {
            throw new DBAppException("Please insert a name for the new table");
        }
        if ("".equals(clusteringKey)) {
            throw new DBAppException("Please insert Clustering key");
        }
        if (!colNameType.containsKey(clusteringKey)) {
            throw new DBAppException("Clustering key (" + clusteringKey + ") does not exist in the Input");
        }
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
                for (String s : colNameType.keySet()) {
                    if (colNameMax.get(s) == null || colNameMin.get(s) == null)
                        throw new DBAppException("You should Specify values for min and max on column " + s);
                    if (colNameMax.get(s).equals("") || colNameMin.get(s).equals(""))
                        throw new DBAppException("You should Specify values for min and max on column " + s);
                    switch (colNameType.get(s)) {
                        case "java.lang.Integer":
                            int imn, imx = 0;
                            try {
                                imn = Integer.parseInt(colNameMin.get(s));
                                imx = Integer.parseInt(colNameMax.get(s));
                            } catch (Exception e) {
                                throw new DBAppException("Non compatible min/max for Column (" + s + ") type specified as Integer");
                            }
                            if (imn > imx) {
                                throw new DBAppException("Minimum can not be larger than Maximum limit for Column " + s);
                            }
                            break;
                        case "java.lang.Double":
                            double dmn, dmx = 0.0;
                            try {
                                dmn = Double.parseDouble(colNameMin.get(s));
                                dmx = Double.parseDouble(colNameMax.get(s));
                            } catch (Exception e) {
                                throw new DBAppException("Non compatible min/max for Column (" + s + ") type specified as Double");
                            }
                            if (dmn > dmx) {
                                throw new DBAppException("Minimum can not be larger than Maximum limit for Column " + s);
                            }
                            break;
                        case "java.util.Date":
                            Date dmin, dmax = new Date();
                            try {
                                dmin = new SimpleDateFormat("yyyy-MM-dd").parse(colNameMin.get(s));
                                dmax = new SimpleDateFormat("yyyy-MM-dd").parse(colNameMax.get(s));
                            } catch (Exception e) {
                                throw new DBAppException("Non compatible min/max for Column (" + s + ") type specified as Date");
                            }
                            if (dmin.compareTo(dmax) > 0) {
                                throw new DBAppException("Minimum can not be larger than Maximum limit for Column " + s);
                            }
                            break;
                        default:
                            if (colNameMin.get(s).compareTo(colNameMax.get(s)) > 0) {
                                throw new DBAppException("Minimum can not be larger than Maximum limit for Column " + s);
                            }
                    }
                }
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
                    Table temp = new Table(tableName, clusteringKey);
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
        HashSet<String> ColNames = new HashSet<>();
        Hashtable<String, String> colType = new Hashtable<>();
        Hashtable<String, String> colMin = new Hashtable<>();
        Hashtable<String, String> colMax = new Hashtable<>();
        String primaryKey = "";
        boolean found = false;
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String current = br.readLine();
            while (current != null) {
                String[] line = current.split(",");
                if (line[0].equals(tableName)) {
                    found = true;
                    do {
                        if (line[3].equals("true")) {
                            primaryKey = line[1];
                        }
                        ColNames.add(line[1]);
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
            if (!found) {
                throw new DBAppException("Table Does Not Exist");
            }
        } catch (FileNotFoundException e) {
            System.out.println("File is not right :(");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //check if all columns belong to the table
        for (String col : columnNames) {
            if (ColNames.contains(col)) {
                ColNames.remove(col);
            } else {
                throw new DBAppException("Column does not exist: " + col);
            }
        }
        //remove unneeded columns
        for (String s : ColNames) {
            colMin.remove(s);
            colMax.remove(s);
            colType.remove(s);
        }
        Table table = deserializeTableInfo(tableName);
        //Check if new grid index already exists
        for (Vector<String> gic : table.getGridIndicesColumns()) {
            if (gic.size() == columnNames.length) {
                boolean same = true;
                for (String cn : columnNames) {
                    if (!gic.contains(cn)) {
                        same = false;
                        break;
                    }
                }
                if (same == true) {
                    throw new DBAppException("The Grid index already exists");
                }
            }
        }
        Vector<Vector<Comparable>> columnRanges = new Vector<>();
        for (String x : columnNames) {
            Object value = x;
            Class type = value.getClass();
            if (type.getName().equals("java.lang.Integer")) {
                Vector<Comparable> ranges = new Vector<>();
                Integer maximum = Integer.parseInt(colMin.get(x));
                Integer minimum = Integer.parseInt(colMax.get(x));
                int step = (maximum - minimum) / 10;
                int y = minimum;
                int i = 1;
                while (i <= 10) {
                    ranges.addElement(y);
                    y += step;
                    i++;
                }
                columnRanges.addElement(ranges);
            } else if (type.getName().equals("java.lang.Double")) {
                Double maximum = Double.parseDouble(colMax.get(x));
                Double minimum = Double.parseDouble(colMin.get(x));
                Vector<Comparable> ranges = new Vector<Comparable>();
                double step = (maximum - minimum) / 10;
                double y = minimum;
                int i = 1;
                while (i <= 10) {
                    ranges.addElement(y);
                    y += step;
                    i++;
                }
                columnRanges.addElement(ranges);
            } else if (type.getName().equals("java.util.Date")) {
                try {
                    Date minimum = new SimpleDateFormat("yyyy-MM-dd").parse(colMin.get(x));
                    Date maximum = new SimpleDateFormat("yyyy-MM-dd").parse(colMax.get(x));
                    Vector<Comparable> ranges = new Vector<Comparable>();
                    long step = (maximum.getTime() - minimum.getTime()) / 10;
                    Date y = minimum;
                    int i = 1;
                    while (i <= 10) {
                        ranges.addElement(y);
                        y = new Date(y.getTime() + step);
                        i++;
                    }
                    columnRanges.addElement(ranges);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else {
                String minimum = colMin.get(x);
                String maximum = colMax.get(x);
                Vector<Comparable> ranges = new Vector<Comparable>();
                double[] steps = new double[maximum.length()];
                for (int i = 0; i < maximum.length(); i++) {
                    if (i < minimum.length()) {
                        steps[i] = (double) ((int) maximum.charAt(i) - (int) minimum.charAt(i)) / 10;
                    } else {
                        steps[i] = (double) ((int) maximum.charAt(i)) / 10;
                    }
                }
                for (int i = 0; i < 10; i++) {
                    StringBuilder temp = new StringBuilder(minimum);
                    for (int j = 0; j < temp.length(); j++) {
                        temp.setCharAt(j, (char) (int) (temp.charAt(j) + steps[j] * i));
                    }
                    for (int j = minimum.length(); j < steps.length; j++) {
                        temp.append((char) (int) (steps[j] * i));
                    }
                    ranges.addElement(temp.toString());
                }
                columnRanges.addElement(ranges);
            }
        }
        GridIndex gridIndex = new GridIndex(tableName, columnNames, columnRanges, primaryKey);

        table.getGridIndicesColumns().addElement(gridIndex.getColumnNames());

        String s = tableName + table.getGridIndices().size();
        populateGridIndex(table, gridIndex);

        serializeGridIndex(s, gridIndex);
        table.getGridIndices().addElement(s);
        table.getGridIndicesColumns().addElement(gridIndex.getColumnNames());


        serializeTableInfo(tableName, table);
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        Hashtable<String, String> colType = new Hashtable<>();
        Hashtable<String, String> colMin = new Hashtable<>();
        Hashtable<String, String> colMax = new Hashtable<>();
        String primaryKey = "";

        try {
            boolean found = false;
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String current = br.readLine();
            while (current != null) {
                String[] line = current.split(",");
                if (line[0].equals(tableName)) {
                    found = true;
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
            if (!found) {
                throw new DBAppException("Table Does Not Exist");
            }

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
                    Date dvalue = (Date) value;
                    Date dmin = new SimpleDateFormat("yyyy-MM-dd").parse(colMin.get(columnName));
                    Date dmax = new SimpleDateFormat("yyyy-MM-dd").parse(colMax.get(columnName));
                    if (dvalue.compareTo(dmax) > 0) {
                        throw new DBAppException("Date inserted Occurs after maximum allowable Date for column: " + columnName);
                    } else if (dvalue.compareTo(dmin) < 0) {
                        throw new DBAppException("Date inserted Occurs before minimum allowable Date for column: " + columnName);
                    }
                } catch (ClassCastException | ParseException e) {
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

                } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException | ClassCastException e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }
        Hashtable<String, Comparable> allColValues = new Hashtable<>();
        for (String s : colNameValue.keySet()) {
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

                Thread.sleep(1);
                LocalDateTime now = LocalDateTime.now();
                String newPageName = tableName + now.getDayOfYear() + now.getHour() + now.getMinute() + now.getSecond() + now.getNano();
                table.getPageNames().addElement(newPageName);
                table.getMinPageValue().addElement(newEntry.getClusteringKey());
                serializePage(newPageName, newPageBody);
                for(String g:table.getGridIndices()){
                    GridIndex gi = deserializeGridIndex(g);
                        gi.insertInGrid(newEntry,newPageName);

                }
                serializeTableInfo(tableName, table);

            } catch (InterruptedException e) {
                throw new DBAppException(e.getMessage());
            }
        } else {
            String pageName = "";
            int pageIndex = -1;
            //Check if a primary index exists
            int h = 0;
            for (Vector<String> cn : table.getGridIndicesColumns()) {
                if (cn.contains(primaryKey)) {
                    break;
                }
                h++;
            }
            if (h < table.getGridIndicesColumns().size()) {
                GridIndex gridIndex = deserializeGridIndex(table.getGridIndices().elementAt(h));
                pageName = gridIndex.getPageNameFromIndex(newEntry.getClusteringKey());
                pageIndex=table.getPageNames().indexOf(pageName);
            } else {
                //if Page(s) was found
                //get index of the page with the possible range
                pageIndex = getPageIndex(newEntry.getClusteringKey(), table.getMinPageValue());
                //page name
                pageName = (String) table.getPageNames().elementAt(pageIndex);
            }
            //Fetch the vector of the page (deserialize)
            Vector<Tuple> page = deserializePage(pageName);
            int keyIndex = getKeyIndex(newEntry.getClusteringKey(), page);
            if (keyIndex != -1) {
                //check if primary key exist in main page
                throw new DBAppException("Clustering Key Already Exist");
            }
            if (page.size() < N) {
                // if there is space in the main page:
                page.insertElementAt(newEntry, indexToInsertAt(newEntry.getClusteringKey(), page));
                serializePage(pageName, page);
                //insert in index
                for(String g:table.getGridIndices()){
                    GridIndex gi = deserializeGridIndex(g);
                    gi.insertInGrid(newEntry,pageName);
                }
                table.getMinPageValue().setElementAt(page.firstElement().getClusteringKey(), pageIndex);
            } else {
                //Page is full
                try {
                    page.insertElementAt(newEntry, indexToInsertAt(newEntry.getClusteringKey(), page));
                    //insert in index
                    for(String g:table.getGridIndices()){
                        GridIndex gi = deserializeGridIndex(g);
                        gi.insertInGrid(newEntry,pageName);
                    }
                    Vector<Tuple> firstHalf = new Vector<>(page.subList(0, (page.size()) / 2));
                    Vector<Tuple> secondHalf = new Vector<>(page.subList((page.size()) / 2, page.size()));
                    Thread.sleep(1);
                    LocalDateTime now = LocalDateTime.now();
                    String newHalfPageName = tableName + now.getDayOfYear() + now.getHour() + now.getMinute() + now.getSecond() + now.getNano();
                    table.getPageNames().insertElementAt(newHalfPageName, pageIndex + 1);
                    table.getMinPageValue().insertElementAt(0, pageIndex + 1);
                    table.getMinPageValue().setElementAt(firstHalf.elementAt(0).getClusteringKey(), pageIndex);
                    table.getMinPageValue().setElementAt(secondHalf.elementAt(0).getClusteringKey(), pageIndex + 1);
                    for(String g:table.getGridIndices()){
                        GridIndex gi = deserializeGridIndex(g);
                        for(Tuple t: secondHalf){
                            gi.updatePageName(t,newHalfPageName);
                        }
                    }
                    serializePage(pageName, firstHalf);
                    serializePage(newHalfPageName, secondHalf);
                } catch (InterruptedException e) {
                    throw new DBAppException(e.getMessage());
                }
            }
            serializeTableInfo(tableName, table);
        }
    }


    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> colNameValue) throws DBAppException {
        Hashtable<String, String> colType = new Hashtable();
        Hashtable<String, String> colMin = new Hashtable();
        Hashtable<String, String> colMax = new Hashtable();
        String clusteringKeyType = "";
        String clusteringKeyName = "";
        try {
            boolean found = false;
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String current = br.readLine();
            while (current != null) {
                String[] line = current.split(",");
                if (line[0].equals(tableName)) {
                    found = true;
                    do {
                        if (line[3].equals("true")) {
                            clusteringKeyName = line[1];
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
            if (!found) {
                throw new DBAppException("Table Does Not Exist");
            }
            if (colNameValue.containsKey(clusteringKeyName))
                throw new DBAppException("Clustering Key can nt be updated");


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
                    Date dvalue = (Date) value;
                    Date dmin = new SimpleDateFormat("yyyy-MM-dd").parse(colMin.get(columnName));
                    Date dmax = new SimpleDateFormat("yyyy-MM-dd").parse(colMax.get(columnName));
                    if (dvalue.compareTo(dmax) > 0) {
                        throw new DBAppException("Date inserted Occurs after maximum allowable Date for column: " + columnName);
                    } else if (dvalue.compareTo(dmin) < 0) {
                        throw new DBAppException("Date inserted Occurs before minimum allowable Date for column: " + columnName);
                    }
                } catch (ClassCastException | ParseException e) {
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

                } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException | ClassCastException e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }

        Table table = deserializeTableInfo(tableName);
        if (table.getPageNames().size() == 0) {
            //throw new DBAppException("This Table has no records to update");
            return;
        } else {
            switch (clusteringKeyType) {
                case "java.lang.Integer": {
                    Integer primary = 0;
                    try {
                        primary = Integer.parseInt(clusteringKeyValue);
                    } catch (Exception e) {
                        throw new DBAppException("ClusteringKeyValue Can not be parsed to Integer");
                    }
                    int pageIndex = getPageIndex(primary, table.getMinPageValue());
                    String pageName = table.getPageNames().elementAt(pageIndex);
                    Vector<Tuple> page = deserializePage(pageName);
                    int keyIndex = getKeyIndex(primary, page);
                    if (keyIndex == -1) {
                        //throw new DBAppException("Record doesn't exist");
                        return;
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
                    Double primary = 0.0;
                    try {
                        primary = Double.parseDouble(clusteringKeyValue);
                    } catch (Exception e) {
                        throw new DBAppException("ClusteringKeyValue Can not be parsed to Double");
                    }
                    int pageIndex = getPageIndex(primary, table.getMinPageValue());
                    String pageName = table.getPageNames().elementAt(pageIndex);
                    Vector<Tuple> page = deserializePage(pageName);
                    int keyIndex = getKeyIndex((Comparable) primary, page);
                    if (keyIndex == -1) {
                        //throw new DBAppException("Record doesn't exist");
                        return;
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
//                        throw new DBAppException("Record doesn't exist");
                        return;
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
                            //throw new DBAppException("Record doesn't exist");
                            return;
                        }
                        Hashtable<String, Comparable> allColValues = page.elementAt(keyIndex).getEntries();
                        for (String s : colNameValue.keySet()) {
                            allColValues.put(s, (Comparable) colNameValue.get(s));
                        }
                        page.elementAt(keyIndex).setEntries(allColValues);
                        serializePage(pageName, page);
                    } catch (ParseException e) {
                        throw new DBAppException("ClusteringKeyValue Can not be parsed to Date");
                    }
                    break;
            }
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
                            try {
                                if (((Comparable) columnNameValue.get(columnName)).compareTo(entries.get(columnName)) != 0)
                                    continue tuple;
                            } catch (NullPointerException e) {
                                continue tuple;
                            }
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
                if (!deleted) {
                    // throw new DBAppException("No record to delete");
                    return;
                }
                for (String s : pagesToDelete) {
                    File f = new File("src/main/resources/data/" + s + ".class");
                    f.delete();
                    int i = pageNames.indexOf(s);
                    pageNames.removeElementAt(i);
                    table.getMinPageValue().removeElementAt(i);
                }
            } else {
                // If the Clustering Key is Known
                int pageIndex = getPageIndex(clusteringValue, table.getMinPageValue());
                String pageName = pageNames.elementAt(pageIndex);
                Vector<Tuple> mainPageBody = deserializePage(pageName);
                int keyIndex = getKeyIndex(clusteringValue, mainPageBody);
                if (keyIndex != -1) {
                    for (String columnName : columnNameValue.keySet()) {
                        try {
                            if (((Comparable) columnNameValue.get(columnName)).compareTo(mainPageBody.elementAt(keyIndex).getEntries().get(columnName)) != 0)
                                // throw new DBAppException("No record to delete");
                                return;
                        } catch (NullPointerException e) {
                            // throw new DBAppException("No record to delete");
                            return;

                        }
                    }
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
                    //throw new DBAppException("Clustering key does not exist");
                    return;
                }
            }
            serializeTableInfo(tableName, table);
        } else {
            //throw new DBAppException("No records to delete from");
            return;

        }
    }

    public Vector<Tuple> selectWithoutIndex(SQLTerm term) {
        Table table = deserializeTableInfo(term._strTableName);
        Vector<String> p = table.getPageNames();
        Vector<Tuple> output = new Vector<>();
        switch (term._strOperator) {
            case "=":
                if (table.getprimaryKey().equals(term._strColumnName)) {
                    int pageindex = getPageIndex((Comparable) term._objValue, table.getMinPageValue());
                    Vector<Tuple> page = deserializePage(table.getPageNames().elementAt(pageindex));
                    int keyindex = getKeyIndex((Comparable) term._objValue, page);
                    if (keyindex != -1) {
                        output.addElement(page.elementAt(keyindex));
                    }
                } else {
                    for (String pageName : p) {
                        Vector<Tuple> page = deserializePage(pageName);
                        for (Tuple tuple : page) {
                            if (tuple.getEntries().get(term._strColumnName).compareTo((Comparable) term._objValue) == 0) {
                                output.addElement(tuple);
                            }
                        }
                    }
                }
                break;
            case "!=":
                for (String pageName : p) {
                    Vector<Tuple> page = deserializePage(pageName);
                    for (Tuple tuple : page) {
                        if (tuple.getEntries().get(term._strColumnName).compareTo((Comparable) term._objValue) != 0) {
                            output.addElement(tuple);
                        }
                    }
                }
                break;
            case ">":
                if (table.getprimaryKey().equals(term._strColumnName)) {
                    int pageindex = getPageIndex((Comparable) term._objValue, table.getMinPageValue());
                    p = new Vector<String>(p.subList(pageindex, p.size()));
                }
                for (String pageName : p) {
                    Vector<Tuple> page = deserializePage(pageName);
                    for (Tuple tuple : page) {
                        if (tuple.getEntries().get(term._strColumnName).compareTo((Comparable) term._objValue) > 0) {
                            output.addElement(tuple);
                        }
                    }
                }
                break;
            case ">=":
                if (table.getprimaryKey().equals(term._strColumnName)) {
                    int pageindex = getPageIndex((Comparable) term._objValue, table.getMinPageValue());
                    p = new Vector<String>(p.subList(pageindex, p.size()));
                }
                for (String pageName : p) {
                    Vector<Tuple> page = deserializePage(pageName);
                    for (Tuple tuple : page) {
                        if (tuple.getEntries().get(term._strColumnName).compareTo((Comparable) term._objValue) >= 0) {
                            output.addElement(tuple);
                        }
                    }
                }
                break;
            case "<":
                if (table.getprimaryKey().equals(term._strColumnName)) {
                    int pageindex = getPageIndex((Comparable) term._objValue, table.getMinPageValue());
                    p = new Vector<String>(p.subList(0, pageindex + 1));
                }
                for (String pageName : p) {
                    Vector<Tuple> page = deserializePage(pageName);
                    for (Tuple tuple : page) {
                        if (tuple.getEntries().get(term._strColumnName).compareTo((Comparable) term._objValue) < 0) {
                            output.addElement(tuple);
                        }
                    }
                }
                break;
            case "<=":
                if (table.getprimaryKey().equals(term._strColumnName)) {
                    int pageindex = getPageIndex((Comparable) term._objValue, table.getMinPageValue());
                    p = new Vector<String>(p.subList(0, pageindex + 1));
                }
                for (String pageName : p) {
                    Vector<Tuple> page = deserializePage(pageName);
                    for (Tuple tuple : page) {
                        if (tuple.getEntries().get(term._strColumnName).compareTo((Comparable) term._objValue) <= 0) {
                            output.addElement(tuple);
                        }
                    }
                }
                break;
        }
        return output;
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        if (arrayOperators.length != sqlTerms.length - 1)
            throw new DBAppException("Wrong statement");
        for (String s : arrayOperators) {
            switch (s.toLowerCase()) {
                case "or":
                case "xor":
                case "and":
                    continue;
                default:
                    throw new DBAppException("Wrong Operator: " + s);
            }
        }
        for (SQLTerm term : sqlTerms) {
            switch (term._strOperator) {
                case "=":
                case ">":
                case ">=":
                case "<":
                case "<=":
                case "!=":
                    break;
                default:
                    throw new DBAppException("Wrong Operator: " + term._strOperator);
            }
            boolean tableFound = false;
            boolean columnFound = false;
            try {
                BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String current = br.readLine();
                while (current != null) {
                    String[] line = current.split(",");
                    if (line[0].equals(term._strTableName)) {
                        tableFound = true;
                        do {
                            if (line[1].equals(term._strColumnName)) {
                                columnFound = true;
                                if (!term._objValue.getClass().getName().equals(line[2]))
                                    throw new DBAppException("Wrong objvalue type for col: " + line[1]);
                                break;
                            }
                            current = br.readLine();
                            if (current != null) {
                                line = current.split(",");
                            }
                        } while (current != null && line[0].equals(term._strTableName));
                        break;
                    }
                    current = br.readLine();
                }
                br.close();
                if (!tableFound) {
                    throw new DBAppException("Table Does Not Exist");
                }
                if (!columnFound) {
                    throw new DBAppException("Column Does Not Exist");

                }
            } catch (FileNotFoundException e) {
                System.out.println("File is not right :(");
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        return null;
    }

    private void createTableInfo(String TableName) {
        try {
            FileOutputStream fileout = new FileOutputStream("src/main/resources/data/" + TableName + ".class");
            ObjectOutputStream o = new
                    ObjectOutputStream(
                    fileout);
            o.close();
            fileout.close();
        } catch (IOException e) {
            e.getMessage();
        }
    }

    private void serializeTableInfo(String name, Table t) {

        try {
            FileOutputStream fileout = new FileOutputStream("src/main/resources/data/" + name + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileout);
            out.writeObject(t);
            out.close();
            fileout.close();
        } catch (IOException i) {
            i.getMessage();
        }
    }

    private Table deserializeTableInfo(String name) {
        Table t = null;
        try {
            FileInputStream filein = new FileInputStream("src/main/resources/data/" + name + ".class");
            ObjectInputStream in = new ObjectInputStream(filein);
            t = (Table) in.readObject();
            in.close();
            filein.close();
        } catch (IOException | ClassNotFoundException i) {
            i.getMessage();
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
            i.getMessage();
        }
    }

    private Vector<Tuple> deserializePage(String name) {
        Vector<Tuple> v = null;
        try {
            FileInputStream filein = new FileInputStream("src/main/resources/data/" + name + ".class");
            ObjectInputStream in = new ObjectInputStream(filein);
            v = (Vector<Tuple>) in.readObject();
            in.close();
            filein.close();
        } catch (IOException | ClassNotFoundException i) {
            i.getMessage();
        }

        return v;
    }

    private void serializeGridIndex(String name, GridIndex g) {
        try {
            FileOutputStream fileout = new FileOutputStream("src/main/resources/data/" + name + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileout);
            out.writeObject(g);
            out.close();
            fileout.close();
        } catch (IOException i) {
            i.getMessage();
        }
    }

    private GridIndex deserializeGridIndex(String name) {
        GridIndex g = null;
        try {
            FileInputStream filein = new FileInputStream("src/main/resources/data/" + name + ".class");
            ObjectInputStream in = new ObjectInputStream(filein);
            g = (GridIndex) in.readObject();
            in.close();
            filein.close();
        } catch (IOException | ClassNotFoundException i) {
            i.getMessage();
        }
        return g;
    }

    private int getKeyIndex(Comparable key, Vector<Tuple> keysInPage) {
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

    private int getPageIndex(Comparable key, Vector<Comparable> minimumValueInPage) {
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

    private int indexToInsertAt(Comparable key, Vector<Tuple> keysInPage) {
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

    private Comparable validateInput(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        Hashtable<String, String> colType = new Hashtable<>();
        Hashtable<String, String> colMin = new Hashtable<>();
        Hashtable<String, String> colMax = new Hashtable<>();
        String clusteringKeyColumn = "";

        try {
            boolean found = false;
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String current = br.readLine();
            while (current != null) {
                String[] line = current.split(",");
                if (line[0].equals(tableName)) {
                    found = true;
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
            if (!found) {
                throw new DBAppException("Table Does Not Exist");
            }
        } catch (FileNotFoundException e) {
            System.out.println("File is not right :(");
        } catch (IOException e) {
            e.getMessage();
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
                    Date dvalue = (Date) value;
                    Date dmin = new SimpleDateFormat("yyyy-MM-dd").parse(colMin.get(columnName));
                    Date dmax = new SimpleDateFormat("yyyy-MM-dd").parse(colMax.get(columnName));
                    if (dvalue.compareTo(dmax) > 0) {
                        throw new DBAppException("Date inserted Occurs after maximum allowable Date for column: " + columnName);
                    } else if (dvalue.compareTo(dmin) < 0) {
                        throw new DBAppException("Date inserted Occurs before minimum allowable Date for column: " + columnName);
                    }
                } catch (ClassCastException | ParseException e) {
                    e.getMessage();
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

                } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException | ClassCastException e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }
        return (Comparable) colNameValue.get(clusteringKeyColumn);
    }

    private void populateGridIndex(Table t, GridIndex i) {
        for (String page : t.getPageNames()) {
            Vector<Tuple> tuples = deserializePage(page);
            for (Tuple tup : tuples) {
                i.insertInGrid(tup, page);
            }
        }
    }

    private Vector<Tuple> andVectors(Vector<Tuple> v1, Vector<Tuple> v2) {
        Vector<Tuple> output = new Vector<>();
        int i = 0, j = 0;
        while (i < v1.size() || j < v2.size()) {
            //same Tuple
            if (v1.elementAt(i).compareTo(v2.elementAt(j)) == 0) {
                output.addElement(v1.elementAt(i));
                i++;
                j++;
            } else {
                if (v1.elementAt(i).compareTo(v2.elementAt(j)) > 0) {
                    j++;
                } else {
                    i++;
                }
            }
        }
        return output;
    }

    private Vector<Tuple> orVectors(Vector<Tuple> v1, Vector<Tuple> v2) {
        Vector<Tuple> output = new Vector<>();
        int i = 0, j = 0;
        while (i < v1.size() || j < v2.size()) {
            //same Tuple
            if (v1.elementAt(i).compareTo(v2.elementAt(j)) == 0) {
                output.addElement(v1.elementAt(i));
                i++;
                j++;
            } else {
                if (v1.elementAt(i).compareTo(v2.elementAt(j)) > 0) {
                    output.addElement(v2.elementAt(j));
                    j++;
                } else {
                    output.addElement(v1.elementAt(i));
                    i++;
                }
            }
        }
        if (i < v1.size()) {
            while (i < v1.size()) {
                output.addElement(v1.elementAt(i));
            }
        } else if (j < v2.size()) {
            while (j < v2.size()) {
                output.addElement(v2.elementAt(j));
            }
        }
        return output;
    }

    private Vector<Tuple> xorVectors(Vector<Tuple> v1, Vector<Tuple> v2) {
        Vector<Tuple> output = v1;
        int i = 0, j = 0;
        while (i < v1.size() || j < v2.size()) {
            //same Tuple
            if (v1.elementAt(i).compareTo(v2.elementAt(j)) == 0) {
                i++;
                j++;
            } else {
                if (v1.elementAt(i).compareTo(v2.elementAt(j)) > 0) {
                    output.addElement(v2.elementAt(j));
                    j++;
                } else {
                    output.addElement(v1.elementAt(i));
                    i++;
                }
            }
        }
        if (i < v1.size()) {
            while (i < v1.size()) {
                output.addElement(v1.elementAt(i));
            }
        } else if (j < v2.size()) {
            while (j < v2.size()) {
                output.addElement(v2.elementAt(j));
            }
        }
        return output;
    }
}
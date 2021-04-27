import java.io.*;
import java.lang.reflect.Constructor;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;

public class DBApp implements DBAppInterface {
    static int N = 0;
    HashMap<String, HashMap<String, String>> tableData = new HashMap<>();

    public static void main(String[] args) throws DBAppException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp();
        dbApp.init();
        Hashtable htblColNameType = new Hashtable();

        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable min = new Hashtable();
        min.put("id", "0");
        min.put("name", "A");
        min.put("gpa", "0.7");

        Hashtable max = new Hashtable();
        max.put("id", "9");
        max.put("name", "Z");
        max.put("gpa", "4");


        dbApp.createTable(strTableName, "id", htblColNameType, min, max);

        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new Integer(2343432));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(0.95));


        dbApp.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(234432));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(0.95));
        dbApp.insertIntoTable(strTableName, htblColNameValue);
        System.out.println("DONE");
////        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 5674567 ));
//        htblColNameValue.put("name", new String("Dalia Noor" ) );
//        htblColNameValue.put("gpa", new Double( 1.25 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 23498 ));
//        htblColNameValue.put("name", new String("John Noor" ) );
//        htblColNameValue.put("gpa", new Double( 1.5 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 78452 ));
//        htblColNameValue.put("name", new String("Zaky Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.88 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );

//        Hashtable<String,Integer> hs = new Hashtable();
//        System.out.println(hs.get("sadasfd"));


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
            Hashtable<String, String> colType = new Hashtable();
            Hashtable<String, String> colMin = new Hashtable();
            Hashtable<String, String> colMax = new Hashtable();
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
                    throw new DBAppException("Type Miss-match for Column: " + columnName + " , a " + type.getName() + " type should be inserted" );
                }
                Object min, max;
                try {
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
                        if (smin.length() > svalue.length()) {
                            throw new DBAppException("Value length Inserted is less than minimum value allowed for column: " + columnName);
                        }
                        String smax = (String) (max);
                        if ((svalue.compareTo(smax)) > 0) {
                            throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                        }
                        if (smax.length() < svalue.length()) {
                            throw new DBAppException("Value length Inserted is larger than maximum value allowed for column: " + columnName);
                        }
                    } else if (value instanceof java.util.Date) {
                        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
                        Date dvalue = sdformat.parse(String.valueOf(value));
                        Date dmin = sdformat.parse(String.valueOf(min));
                        Date dmax = sdformat.parse(String.valueOf(max));
                        if (dvalue.compareTo(dmax) > 0) {
                            throw new DBAppException("Date inserted Occurs after maximum allowable Date for column: " + columnName);
                        } else if (dvalue.compareTo(dmin) < 0) {
                            throw new DBAppException("Date inserted Occurs before minimum allowable Date for column: " + columnName);
                        }
                    }
                }  catch (NoSuchMethodException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
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
            if (table.getNumberOfPages() == 0) {
                //Create the First Page
                try {
                    System.out.println("Creating new page :)");
                    Vector<Tuple> newPageBody = new Vector<>();
                    newPageBody.addElement(newEntry);
                    String newPageName = createPage(table.getName());
                    table.getPageNames().addElement(newPageName);
                    table.setNumberOfPages(1);
                    table.getMinPageValue().addElement(newEntry.getClusteringKey());
                    serializePage(newPageName, newPageBody);
                    serializeTableInfo(tableName, table);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                //if Page(s) was found
                //get index where the range
                int pageIndex = getPageIndex(newEntry.getClusteringKey(), table.getMinPageValue(), table.getNumberOfPages());
                System.out.println("Page Index: " + pageIndex);
                //page name
                String pageName = (String) table.getPageNames().elementAt(pageIndex);
                System.out.println("Page Name: " + pageName);
                //Fetch the vector of the page (deserialize)
                Vector<Tuple> page = deserializePage(pageName);
                int keyIndex = getKeyIndex(newEntry.getClusteringKey(), page);
                System.out.println(keyIndex);
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
                    page.addElement(newEntry);
                    Collections.sort(page);
                    serializePage(pageName, page);
                    table.getMinPageValue().setElementAt(page.firstElement().getClusteringKey(), pageIndex);
                    serializeTableInfo(tableName, table);
                } else {
                    //Page is full
                    //check if last page
                    if (pageIndex < table.getNumberOfPages() - 1) {
                        //if we are not in the last page
                        //insert in overFlow
                        Vector<Tuple> firsthalf = new Vector<>(page.subList(0, N / 2));
                        Vector<Tuple> secondhalf = new Vector<>(page.subList(N / 2, N));

                        try {
                            String newhalfpagename = createPage(tableName);
                            table.getPageNames().insertElementAt(newhalfpagename, pageIndex + 1);
                            table.getMinPageValue().insertElementAt(0, pageIndex + 1);

                            if (secondhalf.firstElement().getClusteringKey().compareTo(newEntry.getClusteringKey()) < 0) {
                                secondhalf.addElement(newEntry);
                            } else {
                                firsthalf.addElement(newEntry);
                            }
                            table.getMinPageValue().setElementAt(firsthalf.firstElement().getClusteringKey(), pageIndex);
                            table.getMinPageValue().setElementAt(secondhalf.firstElement().getClusteringKey(), pageIndex + 1);

                            serializePage(pageName, firsthalf);
                            serializePage(newhalfpagename, secondhalf);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        serializeTableInfo(tableName, table);

//Over Flow Code
//                        if (table.getOverflow().get(pageName) != null) {
//                            int index = 0;
//                            //find index of overflow page with a vacancy
//                            while (index < table.getOverflow().get(pageName).size() && table.getOverflowSizes().get(pageName).elementAt(index) == N) {
//                                index++;
//                            }
//                            if (index < table.getOverflow().get(pageName).size()) {
//                                //insert in this overflow
//                                String overflowPageName = table.getOverflow().get(pageName).elementAt(index);
//                                Vector overflowPageBody = deserializePage(overflowPageName);
//                                overflowPageBody.addElement(newEntry);
//                                Collections.sort(overflowPageBody);
//                                serializePage(overflowPageName, overflowPageBody);
//                                table.getOverflowSizes().get(pageName).setElementAt(table.getOverflowSizes().get(pageName).elementAt(index) + 1, index);
//                            } else {
//                                //if all over flow pages were full (Size = N) -> create new overflow
//                                String ofPageName;
//                                try {
//                                    ofPageName = createPage(tableName);
//                                    Vector<Tuple> ofPageBody = new Vector<>();
//                                    ofPageBody.addElement(newEntry);
//                                    table.getOverflow().get(pageName).addElement(ofPageName);
//                                    table.getOverflowSizes().get(pageName).addElement(1);
//                                    serializePage(ofPageName, ofPageBody);
//
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                }
//                            }
//                        } else {
//                            //CreateFirstOverFlow
//                            Vector<String> overFlowPagesNames = new Vector<>();
//                            Vector<Integer> overFlowSizes = new Vector<>();
//                            String ofPageName;
//                            try {
//                                ofPageName = createPage(tableName);
//                                Vector<Tuple> ofPageBody = new Vector<>();
//                                ofPageBody.addElement(newEntry);
//                                overFlowPagesNames.addElement(ofPageName);
//                                overFlowSizes.add(1);
//                                table.getOverflow().put(ofPageName, overFlowPagesNames);
//                                table.getOverflowSizes().put(pageName, overFlowSizes);
//
//                                serializePage(ofPageName, ofPageBody);
//
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            }
//
//                        }
                    } else {
                        //if we are in the last page -> create a new page
                        Vector<Tuple> newPageBody = new Vector<>();
                        Tuple temp = page.lastElement();
                        try {
                            String newPageName = createPage(table.getName());
                            table.getPageNames().addElement(newPageName);
                            page.addElement(newEntry);
                            page.removeElementAt(N);
                            Collections.sort(page);
                            newPageBody.addElement(temp);
                            serializePage(newPageName, newPageBody);
                            table.setNumberOfPages(1);
                            table.getMinPageValue().addElement(temp.getClusteringKey());
                            table.getMinPageValue().setElementAt(page.firstElement().getClusteringKey(), pageIndex);
                            serializePage(pageName, page);
                            serializeTableInfo(tableName, table);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } else {
            throw new DBAppException("Table Does Not Exist");
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> colNameValue) throws DBAppException {
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
            Hashtable<String, Comparable> allColValues = new Hashtable<>();
            for (String s : colType.keySet()) {
                allColValues.put(s, (Comparable) colNameValue.get(s));
            }
            //CREATE tuple to be inserted
            Tuple newEntry = new Tuple((Comparable) allColValues.get(primaryKey), allColValues);
            //FETCH Table info
            Table t = deserializeTableInfo(tableName);
            if (t.getNumberOfPages() == 0) {

                throw new DBAppException("This Table has no records to update");

            } else {
                int pageIndex = getPageIndex(clusteringKeyValue, t.getMinPageValue(), t.getNumberOfPages());
                String pageName = (String) t.getPageNames().elementAt(pageIndex);
                Vector<Tuple> page = deserializePage(pageName);
                int keyIndex = getKeyIndex(clusteringKeyValue, page);
                if (keyIndex == -1) {
                    throw new DBAppException("Record doesn't exist");
                }
                //Hashtable<String,Object> toUpdate=page.elementAt(keyIndex).getEntries();
                //Hashtable<String,Object> Updated=new Hashtable<String,Object>();


                page.elementAt(keyIndex).setEntries(allColValues);
                serializePage(pageName, page);
                serializeTableInfo(t.getName(), t);
            }
//                if (page.size() > N) {
//                    Tuple temp = page.lastElement();
//                    page.removeElementAt(N);
//                    if (pageIndex < t.getNumberOfPages()-1) {
//                        Vector<Tuple> nextPage = deserializePage((String) t.getPageNames().elementAt(pageIndex));
//                        if(nextPage.size() < N){
//                            nextPage.addElement(temp);
//                            Collections.sort(nextPage);
            //  serializePage((String) t.getPageNames().elementAt(pageIndex),nextPage);
//                        }
//                        else{
//                            //OverFlow Page Linkage
//                        }
//                    } else {
//                        Vector<Tuple> newPageBody = new Vector<>();
//                        newPageBody.addElement(temp);
//                        try {
//                            String newPageName = createPage(t.getName());
//                            t.getPageNames().addElement(newPageName);
//                            serializePage(newPageName, newPageBody);
//                            t.setNumberOfPages(1);
//                            t.getMinPageValue().addElement(temp.getClusteringKey());
//                            serializeTableInfo(tableName, t);
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//                serializePage(pageName, page);
        }
        //   } else {
        //     throw new DBAppException("Table Does Not Exist");
        //}
    }


    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Comparable clusteringValue = validateInput(tableName, columnNameValue, false);
        Table table = deserializeTableInfo(tableName);
        Vector<String> pageNames = table.getPageNames();
        //Looping over all pages
        if (clusteringValue == null) {
            ArrayList<String> pagesToDelete = new ArrayList<>();
            page:
            for (String pageName : pageNames) {
                //OverFlow to be done
                Vector<Tuple> page = deserializePage(pageName);
                tuple:
                //Looping over all tuples in a page
                for (int i = 0; i < page.size(); i++) {
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
                table.setNumberOfPages(-1);
            }
        } else {
            // If the Clustering Key is Known
            int pageIndex = getPageIndex(clusteringValue, table.getMinPageValue(), table.getNumberOfPages());
            String pageName = pageNames.elementAt(pageIndex);
            Vector<Tuple> mainPageBody = deserializePage(pageName);
            int keyIndex = getKeyIndex(pageName, mainPageBody);
            if (keyIndex != -1) {
                mainPageBody.removeElementAt(keyIndex);
                if (mainPageBody.isEmpty()) {
                    pageNames.removeElementAt(pageIndex);
                    table.getMinPageValue().removeElementAt(pageIndex);
                    table.setNumberOfPages(-1);
                    File f = new File("src/main/resources/data/" + pageName + ".class");
                    f.delete();
                } else {
                    table.getMinPageValue().setElementAt(mainPageBody.firstElement().getClusteringKey(), keyIndex);
                    serializePage(pageName, mainPageBody);
                }
            } else {
                //go to overflow pages
                throw new DBAppException("Record does not exist");
            }
        }
        serializeTableInfo(tableName, table);
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
            System.out.println(e);
        }
    }

    public String createPage(String TableName) throws IOException {
        LocalDateTime now = LocalDateTime.now();
        String pageName = TableName + now.getYear() + now.getDayOfYear() + now.getHour() + now.getMinute() + now.getSecond();
        try {
            ObjectOutputStream o = new
                    ObjectOutputStream(
                    new FileOutputStream("src/main/resources/data/" + pageName + ".class"));
            o.close();
        } catch (IOException e) {
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
        System.out.println(name);
        for (Tuple t : pageBody) {
            System.out.println(t);
        }
        System.out.println();
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
        int i = (lo + hi) / 2;

        while (lo < hi) {
            if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) == 0) {
                return i;
            } else if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) < 0) {
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
        int lo = 0, hi = numberOfPages - 1;
        int i = (lo + hi) / 2;
        while (lo < hi) {
            System.out.println("Help!");
            if (i != numberOfPages - 1) {
                if (minimumValueInPage.elementAt(i).compareTo(key) < 0) {
                    hi = i - 1;
                } else {
                    if (i != numberOfPages - 1) {
                        if (minimumValueInPage.elementAt(i + 1).compareTo(key) > 0) {
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


    public Comparable validateInput(String tableName, Hashtable<String, Object> colNameValue, boolean checkPrimaryKey) throws DBAppException {
        if (tableExists(tableName)) {
            Hashtable<String, String> colType = new Hashtable();
            Hashtable<String, String> colMin = new Hashtable();
            Hashtable<String, String> colMax = new Hashtable();
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
                        if (smin.length() > svalue.length()) {
                            throw new DBAppException("Value length Inserted is less than minimum value allowed for column: " + columnName);
                        }
                        String smax = (String) (max);
                        if ((svalue.compareTo(smax)) > 0) {
                            throw new DBAppException("Value inserted is larger than maximum value allowed for column: " + columnName);
                        }
                        if (smax.length() < svalue.length()) {
                            throw new DBAppException("Value length Inserted is larger than maximum value allowed for column: " + columnName);
                        }
                    } else if (value instanceof java.util.Date) {
                        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
                        Date dvalue = sdformat.parse(String.valueOf(value));
                        Date dmin = sdformat.parse(String.valueOf(min));
                        Date dmax = sdformat.parse(String.valueOf(max));
                        if (dvalue.compareTo(dmax) > 0) {
                            throw new DBAppException("Date inserted Occurs after maximum allowable Date for column: " + columnName);
                        } else if (dvalue.compareTo(dmin) < 0) {
                            throw new DBAppException("Date inserted Occurs before minimum allowable Date for column: " + columnName);
                        }
                    }

                    return (Comparable) colNameValue.get(primaryKey);
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new DBAppException("Table Does Not Exist");
        }
        return null;
    }

}
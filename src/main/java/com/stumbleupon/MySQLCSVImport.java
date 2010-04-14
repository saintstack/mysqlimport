package com.stumbleupon;

import org.apache.commons.io.FileUtils;
import org.json.*;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.management.AttributeList;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.events.Attribute;
import java.io.*;
import java.util.*;

/**
 * Read passed table csv file and schema and inserts content into hbase.  Maps
 * the data to hbase using passed mapping.
 *
 * <p>The XML file is result of this 'describe user' statement:
 * <pre>$ mysql -u chaiNgwae --password=PASSWORD -h  10.10.20.112   --port=3564 --database="stumble" --xml -e "describe user;"</pre>
 * Output is per table and needs to be formatted as xml.
 */
public class MySQLCSVImport {
  private static final String COLUMN_NAME_KEY = "Field";
  private final File csv;
  // List of columns in order.  Each element is a map of column attributes
  // including column name keyed by name 'Field'.
  private final List<Map<String, String>> schema;
  private final String tableName;
  private final Map<String, String> columns =
    new HashMap<String, String>();

  public MySQLCSVImport(final File csvFile, final File schema,
      final File mapping)
  throws IOException {
    if (!csvFile.exists()) throw new FileNotFoundException(csvFile.getPath());
    this.csv = csvFile;
    if (!schema.exists()) throw new FileNotFoundException(schema.getPath());
    this.schema = readSchema(schema);
    if (!mapping.exists()) throw new FileNotFoundException(mapping.getPath());
    this.tableName = readMapping(mapping, this.columns);
  }

  public void import() {

  }

  /**
   * Parse xml schema.  Schema looks like this:
   * <pre>
   * &lt;resultset statement="describe user" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        &lt;row>
        &lt;field name="Field">userid&lt;/field>
        &lt;field name="Type">int(10) unsigned&lt;/field>
        &lt;field name="Null">NO&lt;/field>
        &lt;field name="Key">PRI&lt;/field>
        &lt;field name="Default" xsi:nil="true" />
        &lt;field name="Extra">auto_increment&lt;/field>
        &lt;/row>

        &lt;row>
        &lt;field name="Field">nickname&lt;/field>
        &lt;field name="Type">varchar(16)&lt;/field>
        &lt;field name="Null">NO&lt;/field>
        &lt;field name="Key">MUL&lt;/field>
        &lt;field name="Default">&lt;/field>
        &lt;field name="Extra">&lt;/field>
       &lt;/row>
       ...
   * </pre>
   * @param schema
   * @return List of Maps of what was in schema.  Uses value of attribute
   * 'name' as the key and the element value for the map value.
   * @throws IOException
   */
  private List<Map<String, String>> readSchema(final File schema)
    throws IOException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = null;
    try {
      db = dbf.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new IOException("Failed Builder creation", e);
    }
    Document document = null;
    try {
      document = db.parse(schema);
    } catch (SAXException e) {
      throw new IOException("Failed document parse", e);
    }
    document.getDocumentElement().normalize();
    // Get all the rows in the xml.
    NodeList rowNodes = document.getElementsByTagName("row");
    List rows = new ArrayList<Map<String, String>>(rowNodes.getLength());
    for (int i = 0; i < rowNodes.getLength(); i++) {
      Node row = rowNodes.item(i);
      // Now per row, iterate its fields
      NodeList fields = row.getChildNodes();
      boolean has = false;
      Map<String, String> m = new HashMap<String, String>(fields.getLength());
      for (int j = 0; j < fields.getLength(); j++) {
        Node field = fields.item(j);
        if (field.getNodeType() != Node.ELEMENT_NODE) continue;
        if (!field.hasChildNodes()) continue;
        String value = field.getFirstChild().getNodeValue();
        if (value == null || value.length() == 0) continue;
        NamedNodeMap attributes = field.getAttributes();
        Node a = attributes.getNamedItem("name");
        String key = a.getNodeValue();
        // If this is the 'Field' attribute, we found the column name.
        if (key.equals(COLUMN_NAME_KEY)) has = true;
        m.put(key, value);
      }
      if (!has) throw new IOException("No '" + COLUMN_NAME_KEY + "' in " + m);
      rows.add(m);
    }
    return rows;
  }

  /**
   * Read in JSON mapping.
   * For a column in csv to make it into the hbase table, it needs to be
   * mentioned in the json.  The json has two attributes, table name and then
   * a map of the column name in the source to the column name in hbase: e.g.
   * <pre>{table : "user", columns : { userid : "columns:userid"}}</pre>
   * The above will put into the table 'user', the content of the column
   * 'userid' into the hbase column 'columns:userid'.
   * @param mapping
   * @param columns We'll populate columns into this passed Map.
   * @return HBase table name we're to load into to.
   */
  private String readMapping(final File mapping,
    final Map<String, String> columns)
  throws IOException {
    JSONTokener tokenizer = new JSONTokener(new FileReader(mapping));
    JSONObject obj = null;
    try {
      obj = new JSONObject(tokenizer);
    } catch (JSONException e) {
      throw new IOException("Failed tokenization of json mapping " + mapping, e);
    }
    String tableName = null;
    try {
      tableName = (String)obj.get("table");
      JSONObject cs = obj.getJSONObject("columns");
      String [] keys = JSONObject.getNames(cs);
      for (int i = 0; i < keys.length; i++) {
        String key = keys[i];
        if (key == null || key.length() <= 0) throw new IllegalArgumentException();
        String value = (String)cs.get(key);
        if (value == null || value.length() <= 0) {
          throw new IllegalArgumentException(key + " value is empty");
        }
        columns.put(key, value);
      }
    } catch (JSONException e) {
      throw new IOException("Failed to find table/columns", e);
    }
    return tableName;
  }

  private static void usageAndExit(final String message, final int errCode) {
    usage(message);
    System.exit(errCode);
  }

  private static void usage(final String message) {
    if (message != null) System.out.println(message);
    System.out.println("Usage: mysqlcsvimport <csv_file> <xml_table_schema> " +
      "<json_mapping_to_hbase>");
  }

  public static void main(final String[] args) throws IOException {
    if (args.length != 3) usageAndExit("Wrong number of arguments", 1);
    File csv = new File(args[0]);
    if (!csv.exists()) usageAndExit(csv.getPath() + "does not exist", 2);
    File schema = new File(args[1]);
    if (!schema.exists()) usageAndExit(schema.getPath() + "does not exist", 3);
    File mapping = new File(args[2]);
    if (!mapping.exists()) usageAndExit(mapping.getPath() + "does not exist", 4);
    MySQLCSVImport importer = new MySQLCSVImport(csv, schema, mapping);
  }
}

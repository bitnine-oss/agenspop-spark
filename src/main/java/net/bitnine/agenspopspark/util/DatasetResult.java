package net.bitnine.agenspopspark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;

import java.util.*;
import java.util.stream.Collectors;

public class DatasetResult {

    private List<String> columns = new ArrayList<>();
    private List<List<Object>> rows = new ArrayList<>();

    public String singleValue() {
        if (rows == null || rows.isEmpty()) {
            return "";
        }
        List<Object> values = rows.get(0);
        if (values == null || values.isEmpty()) {
            return "";
        }
        return String.valueOf(values.get(0));
    }

    public static Map<String,Object> convert(GenericRowWithSchema df){
        Map<String,Object> structed = new HashMap<>();
        if( df == null ) return structed;

        String[] fieldNames = df.schema().fieldNames();
        for(int j=0; j<df.size(); j++){
            structed.put(fieldNames[j], df.get(j));
        }
        return structed;
    }

    // **NOTE: not used
    public static DatasetResult convert(Dataset<Row> df) {
        DatasetResult result = new DatasetResult();
        if( df == null ) return result;

        String[] fieldNames = df.schema().fieldNames();
        StructField[] fields = df.schema().fields();
        // for DEBUG
        for( StructField field : fields ){
            System.out.println(String.format("  - field: %s[%s]", field.name(), field.dataType().simpleString()));
        }
        result.getColumns().addAll(Arrays.asList(fieldNames));

        Row[] rows = (Row[]) df.collect();
        for (Row row : rows) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < fieldNames.length; i++) {
                Object obj = row.get(i);
                System.out.println("\n\n  ** type: "+obj.getClass().getName());
                if( obj != null && obj.getClass().getName().startsWith("scala.collection.mutable.WrappedArray") ){
                    List<Object> properties = new ArrayList<>();
                    for( Object p : row.getList(i)){
                        if( p.getClass().getName().equals("org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema")){
                            properties.add( DatasetResult.convert((GenericRowWithSchema) p) );
                        }
                        else properties.add(p);
                    }
                    values.add(properties);
                }
                else
                    values.add(obj);
            }
            result.getRows().add(values);
        }

        return result;
    }

    ///////////////////////////////////////////////

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public void setRows(List<List<Object>> rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        return "\nDatasetResult : col[" + columns.stream().collect(Collectors.joining(","))
                + "]\nrows =>\n"
                + rows.stream().map(r->"  - "+r.stream().map(Object::toString).collect(Collectors.joining(" :: ")))
                .collect(Collectors.joining("\n"));
    }

}

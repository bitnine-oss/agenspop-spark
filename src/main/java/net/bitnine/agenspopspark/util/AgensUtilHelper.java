package net.bitnine.agenspopspark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.bitnine.agenspopspark.config.properties.ProductProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class AgensUtilHelper {

    // Function to get the Stream
    public static <T> Stream<T> iterator2stream(Iterator<T> iterator) {
        // Convert the iterator to Spliterator
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
        // Get a Sequential Stream from spliterator
        return StreamSupport.stream(spliterator, false);
    }

    public static List<List<Object>> convertProperties(List<Row> data){
        List<List<Object>> list = new ArrayList<>();
        for(Row row : data){
            List<Object> item = new ArrayList<>();
            for(int i=0; i<row.size(); i+=1){
                item.add((Object)row.get(i).toString());
            }
            list.add(item);
        }
        return list;
    }

    public static Stream<Map<String,Object>> dataset2stream(Dataset<Row> rows){
        String[] fieldNames = rows.schema().fieldNames();
        Stream<Row> stream = iterator2stream(rows.toLocalIterator());

        Stream<Map<String,Object>> resultMap = stream.map(r->{
            Map<String,Object> row = new HashMap<>();
            int i = 0;
            for(String key: fieldNames){
                if( key.equals("properties")){
                    row.put(key, convertProperties(r.getList(i)));
                }
                else row.put(key, r.get(i));
                i += 1;
            }
            return row;
        });
        return resultMap;
    }

    //////////////////////////////////////////////////////////

    // **NOTE: Java Exception Handle in Stream Operations
    // https://kihoonkim.github.io/2017/09/09/java/noexception-in-stream-operations/

    public interface ExceptionSupplier<T> {
        T get() throws Exception;
    }

    public static <T> T wrapException(ExceptionSupplier<T> z) {
        try {
            return z.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final HttpHeaders productHeaders(ProductProperties productProperties){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", productProperties.getName());
        headers.add("agens.product.version", productProperties.getVersion());
        return headers;
    }
/*
    // **NOTE: "return Flux<String>" not working
    public static <T> ResponseEntity<Flux<String>> responseStream(
            ObjectMapper mapper, HttpHeaders headers, Stream<T> stream, long size
    ) {
        // **NOTE: cannot use StringJoiner beacause it's Streaming and Map function
        // https://stackoverflow.com/a/50988970/6811653
        Stream<String> vstream = stream.map(r ->
            wrapException(() -> mapper.writeValueAsString(r) + ",")
        );
        return new ResponseEntity(
                Flux.fromStream(Stream.concat(Stream.concat(Stream.of("["), vstream), Stream.of("]")))
                , headers, HttpStatus.OK);
    }
 */

    public static <T> ResponseEntity<List<String>> responseStream(
            ObjectMapper mapper, Stream<T> result, HttpHeaders headers
    ) {
        Stream<String> stream = result.map(r ->
                wrapException(() -> mapper.writeValueAsString(r))
        );
        return new ResponseEntity(stream.collect(Collectors.joining(",","[","]"))
                , headers, HttpStatus.OK);
    }

    public static String convertMap2String(Map<String, ?> map) {
        StringBuilder mapAsString = new StringBuilder("{");
        for(String key : map.keySet()) {
            mapAsString.append(key + "=" + map.get(key).toString() + ", ");
        }
        mapAsString.delete(mapAsString.length()-2, mapAsString.length()).append("}");
        return mapAsString.toString();
    }

}

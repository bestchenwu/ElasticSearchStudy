package unit1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import common.ElasticSearchClient;
import org.elasticsearch.common.Strings;

/**
 * 插入单条row
 *
 * @author chenwu on 2020.9.15
 */
public class InsertSingleRowRequestTest {

    public static void main(String[] args) throws IOException {
        ElasticSearchClient elasticSearchClient = ElasticSearchClient.getInstance();
        Map<String,Object> data = new HashMap<>();
        data.put("age",31);
        data.put("address","gaoxin3 street");
        data.put("name","mary");
        boolean result = elasticSearchClient.putSingleRowIntoIndex("user","1",data);
        System.out.println(result);
        Map<String, Object> user = elasticSearchClient.getSingleDoc("user", "1", Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
        System.out.println(user);
        elasticSearchClient.close();
    }
}

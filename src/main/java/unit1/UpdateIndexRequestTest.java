package unit1;

import common.ElasticSearchClient;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试更新索引
 *
 * @author chenwu on 2020.9.15
 */
public class UpdateIndexRequestTest {

    public static void main(String[] args) throws IOException {
        ElasticSearchClient instance = ElasticSearchClient.getInstance();
        Map<String,Object> data = new HashMap<>();
        data.put("age",22);
        data.put("address","gaoxin3 street");
        data.put("name","jack");
        boolean result = instance.updateSingleRowIntoIndex("user", "1", data);
        System.out.println(result);
        Map<String, Object> user = instance.getSingleDoc("user", "1", Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
        System.out.println(user);
        instance.close();
    }
}

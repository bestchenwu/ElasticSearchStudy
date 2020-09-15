package unit1;

import common.ElasticSearchClient;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.util.Map;

/**
 * 删除索引的测试
 */
public class DeleteIndexRequestTest {

    public static void main(String[] args) throws IOException {
        ElasticSearchClient instance = ElasticSearchClient.getInstance();
//        boolean deleteResult = instance.deleteIndex("users", "1");
//        System.out.println("deleteResult="+deleteResult);
//        Map<String, Object> users = instance.getSingleDoc("users", "1", Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
//        System.out.println("users size:"+users.size());
        boolean deleteResult = instance.deleteIndex("users", null);
        System.out.println("deleteResult="+deleteResult);
        boolean existsResult = instance.existsIndexById("users");
        System.out.println("existsResult="+existsResult);
        instance.close();
    }
}

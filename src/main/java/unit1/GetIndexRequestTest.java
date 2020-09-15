package unit1;

import common.ElasticSearchClient;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.util.Map;

/**
 * 根据索引查询单条数据
 *
 * @author chenwu on 2020.9.15
 */
public class GetIndexRequestTest {

    public static void main(String[] args) throws IOException {
        ElasticSearchClient elasticSearchClient = ElasticSearchClient.getInstance();
        Map<String, Object> users = elasticSearchClient.getSingleDoc("users", "1", Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
        System.out.println(users);
        String[] includes = new String[]{"name","message"};
        users = elasticSearchClient.getSingleDoc("users", "1", includes, Strings.EMPTY_ARRAY);
        System.out.println(users);
        elasticSearchClient.close();
    }
}

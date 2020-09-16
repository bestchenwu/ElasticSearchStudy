package unit1;

import common.ElasticSearchClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * 搜索query
 *
 * @author chenwu on 2020.9.16
 */
public class SearchQueryTest {

    public static void main(String[] args) throws IOException {
        ElasticSearchClient elasticSearchClient = ElasticSearchClient.getInstance();
        List<Map<String, Object>> map = elasticSearchClient.processQuery("user");
        System.out.println(map);
        elasticSearchClient.close();
    }
}

package unit1;

import common.ElasticSearchClient;

import java.io.IOException;

/**
 * 创建索引的测试
 *
 * @author chenwu on 2020.9.15
 */
public class CreateIndexRequestTest {

    public static void main(String[] args) throws IOException {
        ElasticSearchClient client = ElasticSearchClient.getInstance();
        boolean result = client.createIndex("user");
        System.out.println(result);
        client.close();
    }
}

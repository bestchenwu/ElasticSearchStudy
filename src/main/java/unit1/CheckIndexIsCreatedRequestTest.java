package unit1;

import common.ElasticSearchClient;

import java.io.IOException;

/**
 * 检查索引是否存在的测试
 *
 * @author chenwu on 2020.9.11
 */
public class CheckIndexIsCreatedRequestTest {

    public static void main(String[] args) throws IOException {
        ElasticSearchClient elasticSearchClient = ElasticSearchClient.getInstance();
        System.out.println(elasticSearchClient.existsIndexById("test"));
    }
}

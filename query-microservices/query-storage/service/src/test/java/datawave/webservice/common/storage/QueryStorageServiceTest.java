package datawave.webservice.common.storage;

import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.storage.config.QueryStorageConfig;
import datawave.webservice.common.storage.config.QueryStorageProperties;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = QueryStorageServiceTest.QueryStorageServiceTestConfiguration.class)
@ActiveProfiles({"QueryStorageServiceTest", "sync-disabled"})
public class QueryStorageServiceTest {
    @LocalServerPort
    private int webServicePort;

    @Autowired
    private RestTemplateBuilder restTemplateBuilder;

    private JWTRestTemplate jwtRestTemplate;

    @Autowired
    private QueryStorageConfig.TaskNotificationSourceBinding taskNotificationSourceBinding;
    
    @Autowired
    private QueryStorageProperties queryStorageProperties;

    @Autowired
    private QueryStorageService storageService;

    @Autowired
    private QueryStorageStateService storageStateService;
    
    private SubjectIssuerDNPair DN;
    private String userDN = "userDn";
    private String query = "some query";
    private String authorizations = "AUTH1,AUTH2";

    @Before
    public void setup() {
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
    }
    
    @Test
    public void testStoreQuery() throws ParseException {
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = storageService.storeQuery(new QueryType("default"), query);
        assertNotNull(queryId);
    }
    

    @Configuration
    @Profile("QueryStorageServiceTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryStorageServiceTestConfiguration {
        @Bean
        QueryStorageCache getQueryStorageCache() {
            return null;
        }
    }

}

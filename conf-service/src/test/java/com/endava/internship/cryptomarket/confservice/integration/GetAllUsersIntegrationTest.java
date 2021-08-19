package com.endava.internship.cryptomarket.confservice.integration;

import com.endava.internship.cryptomarket.confservice.business.model.UserDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import lombok.SneakyThrows;
import oracle.jdbc.pool.OracleDataSource;
import org.dbunit.DataSourceBasedDBTestCase;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.List;

import static com.endava.internship.cryptomarket.confservice.data.model.Roles.ADMIN;
import static com.endava.internship.cryptomarket.confservice.data.model.Roles.CLIENT;
import static com.endava.internship.cryptomarket.confservice.data.model.Roles.OPERAT;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.ACTIVE;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.INACTV;
import static com.endava.internship.cryptomarket.confservice.data.model.Status.SUSPND;
import static com.github.springtestdbunit.annotation.DatabaseOperation.CLEAN_INSERT;
import static com.github.springtestdbunit.annotation.DatabaseOperation.DELETE_ALL;
import static io.restassured.RestAssured.defaultParser;
import static io.restassured.RestAssured.given;
import static io.restassured.parsing.Parser.JSON;
import static org.assertj.core.api.Assertions.assertThat;

class GetAllUsersIntegrationTest extends DataSourceBasedDBTestCase {

    private final static String FILENAME = "/testData.xml";
    private String URL = "jdbc:oracle:thin:@//localhost:1521/pdb";
    private String USERNAME = "crypto_market";
    private String PASSWORD = "crypto_market";

    private final String url = "http://localhost:8080/conf-service/users";

    private final List<UserDto> userList = List.of(
            new UserDto("admin", "admin@gmail.com", ADMIN, ACTIVE, null, null, null),
            new UserDto("operat1", "operat1@gmail.com", OPERAT, ACTIVE, null, null, null),
            new UserDto("operat2", "operat2@gmail.com", OPERAT, ACTIVE, null, null, null),
            new UserDto("operat3", "operat3@gmail.com", OPERAT, SUSPND, null, null, null),
            new UserDto("operat4", "operat4@gmail.com", OPERAT, INACTV, null, null, null),
            new UserDto("client1", "client@gmail.com", CLIENT, ACTIVE, null, null, null)
    );

    @BeforeEach
    void setup() throws Exception {
        super.setUp();
    }

    @AfterEach
    void teardown() throws Exception {
        super.tearDown();
    }

    @DatabaseSetup(value = "/testData.xml", type = CLEAN_INSERT)
    @DatabaseTearDown(value = "/testData.xml", type = DELETE_ALL)
    @Test
    void whenGetAllUsers_thenRespondAccordingToAPI() throws JsonProcessingException {
        defaultParser = JSON;

        UserDto[] usersDTO = given().header("Requester-Username", "admin")
                .when().get(url)
                .then().assertThat().statusCode(200)
                .extract().as(UserDto[].class);

        assertThat(usersDTO).containsAll(userList);
    }

    protected FlatXmlDataSet getDataSet() throws DataSetException {
        return new FlatXmlDataSetBuilder().build(this.getClass()
                .getResourceAsStream(FILENAME));
    }

    @SneakyThrows
    @Override
    protected DataSource getDataSource() {
        final OracleDataSource oracleDataSource = new OracleDataSource();
        oracleDataSource.setDriverType("oracle.jdbc.driver.OracleDriver");
        oracleDataSource.setURL(URL);
        oracleDataSource.setUser(USERNAME);
        oracleDataSource.setPassword(PASSWORD);
        return oracleDataSource;
    }
}

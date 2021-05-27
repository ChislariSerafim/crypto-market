package com.endava.internship.cryptomarket.confservice.integration;

import com.endava.internship.cryptomarket.confservice.business.model.ApiError;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import io.restassured.http.Header;
import io.restassured.http.Headers;
import lombok.SneakyThrows;
import oracle.jdbc.pool.OracleDataSource;
import org.dbunit.DataSourceBasedDBTestCase;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.sql.DataSource;

import static com.github.springtestdbunit.annotation.DatabaseOperation.CLEAN_INSERT;
import static com.github.springtestdbunit.annotation.DatabaseOperation.DELETE_ALL;
import static io.restassured.RestAssured.defaultParser;
import static io.restassured.RestAssured.given;
import static io.restassured.parsing.Parser.JSON;
import static org.assertj.core.api.Assertions.assertThat;

class ApiErrorIntegrationTest extends DataSourceBasedDBTestCase {

    private final static String FILENAME = "/testData.xml";
    private String URL = "jdbc:oracle:thin:@//localhost:1521/pdb";
    private String USERNAME = "crypto_market";
    private String PASSWORD = "crypto_market";

    public ApiErrorIntegrationTest() throws Exception {
        super.setUp();
    }

    @DatabaseSetup(value = "/testData.xml", type = CLEAN_INSERT)
    @DatabaseTearDown(value = "/testData.xml", type = DELETE_ALL)
    @ParameterizedTest
    @EnumSource(value = ApiErrorTest.class)
    void whenRequestInvalid_thenRespondAccordingToAPI(ApiErrorTest test) {
        final Headers postHeaders = new Headers(new Header("Content-Type", test.getContentType()),
                new Header("Requester-Username", test.getRequesterUsername()));
        defaultParser = JSON;

        ApiError apiError = given().headers(postHeaders).body(test.getMessageParam())
                .when().request(test.getMethod(), test.getUrlPath())
                .then().assertThat().statusCode(test.getExceptionResponses().getHttpStatus())
                .extract().as(ApiError.class);

        assertThat(apiError).isEqualTo(test.getExceptionResponses().buildApiError(test.getExceptionParam()));

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

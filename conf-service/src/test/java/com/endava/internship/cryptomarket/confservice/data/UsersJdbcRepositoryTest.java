package com.endava.internship.cryptomarket.confservice.data;

import com.endava.internship.cryptomarket.confservice.data.model.Roles;
import com.endava.internship.cryptomarket.confservice.data.model.Status;
import com.endava.internship.cryptomarket.confservice.data.model.User;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import oracle.jdbc.pool.OracleDataSource;
import org.dbunit.DataSourceBasedDBTestCase;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.github.springtestdbunit.annotation.DatabaseOperation.CLEAN_INSERT;
import static com.github.springtestdbunit.annotation.DatabaseOperation.DELETE_ALL;
import static java.time.LocalDateTime.now;
import static org.assertj.core.api.Assertions.assertThat;

@DatabaseSetup(value = "/testData.xml", type = CLEAN_INSERT)
@DatabaseTearDown(value = "/testData.xml", type = DELETE_ALL)
@NoArgsConstructor
public class UsersJdbcRepositoryTest extends DataSourceBasedDBTestCase {

    private final static String FILENAME = "/testData.xml";
    private final String URL = "jdbc:oracle:thin:@//localhost:1521/pdb";
    private final String USERNAME = "crypto_market";
    private final String PASSWORD = "crypto_market";
    private final int TEST_ID = 1;
    private final String TEST_USERNAME = "operat1";

    IDatabaseTester databaseTester;

    private UserJdbcRepository userJdbcRepository;

    DataSource dataSource;

    @Override
    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();
        createDatasource();

        databaseTester = new JdbcDatabaseTester(
                "oracle.jdbc.driver.OracleDriver",
                URL,
                USERNAME,
                PASSWORD
        );

        IDataSet dataSet = getDataSet();
        databaseTester.setDataSet(dataSet);
        databaseTester.onSetup();
    }


    @Test
    public void testGetUsers() throws DataSetException {
        List<User> expectedUsers = getAllExpectedUsers(0);
        List<User> actualUsers = userJdbcRepository.getAll();

        assertThat(expectedUsers).containsAll(actualUsers);
    }

    @Test
    public void testGetUsersFromEmptyTable() throws Exception {
        clearTable();

        List<User> actualUsers = userJdbcRepository.getAll();

        assertThat(actualUsers).isEmpty();
    }
    @Test
    public void testGetUser() throws DataSetException {
        IDataSet expectedDataSet = getDataSet();
        ITable expectedTable = expectedDataSet.getTable("T_USER");

        User expectedUser = User.builder()
                .username(expectedTable.getValue(TEST_ID - 1, "username").toString())
                .email(expectedTable.getValue(TEST_ID - 1, "email").toString())
                .role(Roles.valueOf(expectedTable.getValue(TEST_ID - 1, "role").toString()))
                .status(Status.valueOf(expectedTable.getValue(TEST_ID - 1, "status").toString()))
                .createdOn(Timestamp.valueOf(expectedTable.getValue(TEST_ID - 1, "created_on").toString()).toLocalDateTime())
                .build();

        User actualUser = userJdbcRepository.get(TEST_USERNAME).get();

        assertThat(expectedUser).isEqualTo(actualUser);
    }

    @Test
    public void testGetUserNonexistent() {
        Optional<User> actualUser = userJdbcRepository.get("Inexistent user");

        assertThat(actualUser).isNotPresent();
    }

    @Test
    public void testGetUserFromEmptyTable() throws Exception {
        clearTable();

        Optional<User> actualUser = userJdbcRepository.get(TEST_USERNAME);

        assertThat(actualUser).isNotPresent();
    }

    @Test
    public void testAddUser() throws DataSetException {
        List<User> expectedTable = getAllExpectedUsers(0);

        User newUser = User.builder()
                .username("username")
                .email("email@email.com")
                .role(Roles.OPERAT)
                .status(Status.ACTIVE)
                .createdOn(now())
                .build();
        expectedTable.add(newUser);

        userJdbcRepository.save(newUser);
        List<User> actualUsers = userJdbcRepository.getAll();

        assertThat(expectedTable).containsAll(actualUsers);
    }

    @Test
    public void testDeleteUser() throws DataSetException {
        List<User> expectedUsers = getAllExpectedUsers(1);

        assertThat(userJdbcRepository.delete(TEST_USERNAME)).isTrue();

        List<User> actualUsers = userJdbcRepository.getAll();
        assertThat(expectedUsers).containsAll(actualUsers);
    }

    @Test
    public void testDeleteUserNonexistent() throws DataSetException {
        List<User> expectedUsers = getAllExpectedUsers(0);

        userJdbcRepository.delete("Nonexistent user");
        List<User> actualUsers = userJdbcRepository.getAll();

        assertThat(expectedUsers).containsAll(actualUsers);
    }

    @Test
    public void testUpdateUser() throws DataSetException {
        List<User> expectedUsers = getAllExpectedUsers(0);

        User updatedUser = User.builder()
                .username(TEST_USERNAME)
                .email("newEmail@email.com")
                .role(Roles.OPERAT)
                .status(Status.ACTIVE)
                .createdOn(now())
                .updatedOn(now())
                .updatedBy("Updater")
                .build();
        expectedUsers.set(TEST_ID - 1, updatedUser);

        userJdbcRepository.save(updatedUser);
        List<User> actualUsers = userJdbcRepository.getAll();

        assertThat(expectedUsers).containsAll(actualUsers);
    }

    @Test
    public void testExists(){
        User existentUser = User.builder()
                .username(TEST_USERNAME)
                .email("newEmail@email.com")
                .role(Roles.OPERAT)
                .status(Status.ACTIVE)
                .createdOn(now())
                .updatedOn(now())
                .updatedBy("Updater")
                .build();
        assertThat(userJdbcRepository.exists(existentUser)).isTrue();
    }

    @Test
    public void testExistsIfNonexistent(){
        User nonexistentUser = User.builder()
                .username("Nonexistent")
                .email("newEmail@email.com")
                .role(Roles.OPERAT)
                .status(Status.ACTIVE)
                .createdOn(now())
                .updatedOn(now())
                .updatedBy("Updater")
                .build();
        assertThat(userJdbcRepository.exists(nonexistentUser)).isFalse();
    }

    private void createDatasource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        config.setJdbcUrl(URL);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);
        userJdbcRepository = new UserJdbcRepository(dataSource);
    }

    private void clearTable() throws Exception {
        databaseTester.setSetUpOperation(DatabaseOperation.DELETE_ALL);
        databaseTester.onSetup();
    }

    private List<User> getAllExpectedUsers(int y) throws DataSetException {
        IDataSet expectedDataSet = getDataSet();
        ITable expectedTable = expectedDataSet.getTable("T_USER");
        List<User> expectedUsers = new ArrayList<>();
        for (int i = y; i < 7; i++) {
            User readUser = User.builder()
                    .username(expectedTable.getValue(i, "username").toString())
                    .email(expectedTable.getValue(i, "email").toString())
                    .role(Roles.valueOf(expectedTable.getValue(i, "role").toString()))
                    .status(Status.valueOf(expectedTable.getValue(i, "status").toString()))
                    .build();
            expectedUsers.add(readUser);
        }
        return expectedUsers;
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
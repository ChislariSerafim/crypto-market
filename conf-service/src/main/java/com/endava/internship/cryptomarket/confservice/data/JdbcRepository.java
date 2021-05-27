package com.endava.internship.cryptomarket.confservice.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

import javax.sql.DataSource;

import com.endava.internship.cryptomarket.confservice.data.dbutils.ResultSetMapper;
import com.endava.internship.cryptomarket.confservice.data.dbutils.SQLStatementFunction;
import com.endava.internship.cryptomarket.confservice.data.dbutils.StatementEntityConsumer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class JdbcRepository {

    private final DataSource hikariDataSource;

    protected <T> T executeSelectQuery(String query, ResultSetMapper<T> mapper) {
        return executeStatement(query, statement -> {
            try (ResultSet resultSet = statement.executeQuery()) {
                return mapper.map(resultSet);
            }
        });
    }

    protected <T> void executeQueryWithArguments(String query, Map<Integer, Object> arguments) {
        executeStatement(query, statement -> {
            mapSqlArguments(arguments, statement);
            return statement.executeUpdate();
        });
    }

    protected <T> void executeQueryWithArguments(String query,
                                                 @NonNull T entity,
                                                 @NonNull StatementEntityConsumer<T> mapper,
                                                 Map<Integer, Object> arguments) {
        executeStatement(query, statement -> {
            mapper.accept(statement, entity);
            mapSqlArguments(arguments, statement);
            return statement.executeUpdate();
        });
    }

    protected <T> T executeSelectQuery(String query, ResultSetMapper<T> mapper, Map<Integer, Object> arguments) {
        return executeStatement(query, statement -> {
            mapSqlArguments(arguments, statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                return mapper.map(resultSet);
            }
        });
    }

    private <T> T executeStatement(String query, SQLStatementFunction<T> function) {
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
            return function.apply(statement);
        } catch (SQLException ex) {
            throw new RuntimeException("Connection to database failed: " + ex.getMessage(), ex);
        }
    }

    private void mapSqlArguments(Map<Integer, Object> arguments, PreparedStatement statement) throws SQLException {
        for (var entry : arguments.entrySet()) {
            if (entry.getValue() instanceof Integer) {
                statement.setInt(entry.getKey(), (Integer) entry.getValue());
            } else if (entry.getValue() instanceof String) {
                statement.setString(entry.getKey(), (String) entry.getValue());
            } else if (entry.getValue() instanceof LocalDateTime) {
                statement.setTimestamp(entry.getKey(), Timestamp.valueOf((LocalDateTime) entry.getValue()));
            } else {
                throw new UnsupportedOperationException("No mapper for argument " + entry.getValue().getClass());
            }
        }
    }
}

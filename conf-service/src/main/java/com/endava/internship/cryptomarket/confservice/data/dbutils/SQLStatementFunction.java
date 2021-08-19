package com.endava.internship.cryptomarket.confservice.data.dbutils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface SQLStatementFunction<T> {

    T apply(PreparedStatement statement) throws SQLException;
}

package com.beepd.batch.springbatch.step.reader;

import com.beepd.batch.springbatch.model.Employee;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;

public class DBReader extends JdbcCursorItemReader<Employee> {

    @Autowired
    private DataSource dataSource;

    @Override
    public Employee read() throws Exception, UnexpectedInputException, ParseException {
        setSql("select id, name from employee");
        setDataSource(dataSource);
        setRowMapper((resultSet, i) -> {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            return new Employee(id, name);
        });
        return super.read();
    }
}

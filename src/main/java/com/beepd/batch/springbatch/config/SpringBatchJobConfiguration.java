package com.beepd.batch.springbatch.config;

import com.beepd.batch.springbatch.model.Employee;
import com.beepd.batch.springbatch.step.reader.DBReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.time.LocalDateTime;

@Configuration
public class SpringBatchJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DBReader dbReader;

    @Autowired
    DataSource dataSource;

    @Bean
    public Step step(){
         return stepBuilderFactory.get("step1")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("step1");
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    @Bean
    public Step step2(){
         return stepBuilderFactory.get("step2")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("step2");
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    @Bean
    public Job job(){
        return jobBuilderFactory.get("job1")
                .start(step()).next(step2())
                .build();
    }

    /*@Bean
    public JdbcCursorItemReader<Employee> employeeJdbcCursorItemReader() {
        JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<>();
        reader.setSql("select id, name from employee");
        reader.setDataSource(dataSource);
        reader.setRowMapper((resultSet, i) -> {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            return new Employee(id, name);
        });
        return reader;
    }*/

    @Bean
    public Step dbReaderJobStep(){
        return stepBuilderFactory.get("DBReaderJob_"+LocalDateTime.now().toString())
                .<Employee, Employee>chunk(10)
                .reader(dbReader)
                .writer(list -> {
                    list.stream().forEach(o -> System.out.println(o));
                })
                .build();
    }

    @Bean
    public Job dbReaderJob(){
        return jobBuilderFactory.get("DBReaderJob_"+LocalDateTime.now().toString())
                .start(dbReaderJobStep())
                .build();
    }
}

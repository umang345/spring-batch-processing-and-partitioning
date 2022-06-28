package com.umang345.springbatch.config;

import com.umang345.springbatch.entity.Customer;
import com.umang345.springbatch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.Job;

/***
 * Configuration class for Batch Processing
 */
@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig
{
    /***
     * Injecting JobBuilderFactory Dependency to create Job
     */
    private JobBuilderFactory jobBuilderFactory;

    /***
     * Injecting StepBuilderFactory Dependency to create Step
     */
    private StepBuilderFactory stepBuilderFactory;

    /***
     * Injecting Customer JPARepository
     */
    private CustomerRepository customerRepository;

    /***
     * Creates a Bean of Type FlatFileItemReader to read the content from the csv file
     * @return Instance of built FlatFileItemReader class
     */
    @Bean
    public FlatFileItemReader<Customer> reader() {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    /***
     * Return LineMapper Instance of Type Customer to map data from CSV file
     * @return
     */
    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    /***
     * Bean for ItemProcessor
     * @return The implementation of ItemProcessor as a Bean
     */
    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }

    /***
     * Bean for ItemWriter
     * @return Implementation of ItemWriter<T> as RepositoryItemWriter<Customer>
     */
    @Bean
    public RepositoryItemWriter<Customer> writer() {
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    /***
     * Bean that return built Step class Object using StepBuilderFactory
     * @return the built Step object
     */
    @Bean
    public Step step1() {
        return stepBuilderFactory.get("csv-step").<Customer, Customer>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .build();
    }

    /***
     * Bean to get the cuilt instance of Job class using JobBuilderFactory
     * @return The constructed Job class instance
     */
    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("importCustomers")
                .flow(step1()).end().build();

    }

    /***
     * Bean for Asynchronous Task Executor
     * @return TaskExecutor Object
     */
    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }

}

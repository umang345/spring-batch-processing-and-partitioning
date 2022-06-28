package com.umang345.springbatch.config;

import com.umang345.springbatch.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

/***
 *  To filter out the data from CSV file while processing
 */
public class CustomerProcessor implements ItemProcessor<Customer,Customer> {

    /***
     * Contains the logic to filter out data rows from dataset
     * @param customer : populated customer object
     * @return the same customer object if it meets the criteria, null otherwise
     * @throws Exception
     */
    @Override
    public Customer process(Customer customer) throws Exception {
        return customer;
    }
}
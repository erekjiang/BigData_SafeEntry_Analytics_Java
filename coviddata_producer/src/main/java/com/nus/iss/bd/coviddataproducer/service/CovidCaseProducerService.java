package com.nus.iss.bd.coviddataproducer.service;

import com.nus.iss.bd.coviddataproducer.dto.CaseRecordDto;
import com.nus.iss.bd.coviddataproducer.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cglib.core.Local;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class CovidCaseProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CovidCaseProducerService.class);

    @Autowired
    private KafkaTemplate<String, CaseRecordDto> covidCaseKafkaTemplate;

    @Autowired
    CovidCaseApiService covidCaseApiService;

    private LocalDateTime lastCalledDttm;

    @Value(value = "${covidCase.topic.name}")
    private String covidCaseTopicName;

    @Scheduled(cron= "*/10 * *  * * ? ")
    public void produceMessage(){
        if(lastCalledDttm == null){
            lastCalledDttm = LocalDateTime.now().minusSeconds(10);
        }else{
            lastCalledDttm = lastCalledDttm.plusSeconds(10);
        }

        LOGGER.info("Pulling data from covidCaseEndpoint for datetime:{}",lastCalledDttm);
        List<CaseRecordDto> cases = covidCaseApiService.getCases(lastCalledDttm);
        cases.parallelStream().forEach(record ->{
            LOGGER.info("Producing message for record:{}",record);
            covidCaseKafkaTemplate.send(covidCaseTopicName,record);
        });
    }

}

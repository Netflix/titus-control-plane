package com.netflix.titus.supplementary.taskspublisher.es;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class ElasticSearchUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUtilsTest.class);

    @Test
    public void verifyCurrentEsIndexName() {
        SimpleDateFormat indexDateFormat = new SimpleDateFormat("yyyyMM");
        String monthlySuffix = indexDateFormat.format(new Date());
        String esIndexNameCurrent = ElasticSearchUtils.buildEsIndexNameCurrent("workloads_", indexDateFormat);
        assertThat(esIndexNameCurrent).isNotNull();
        assertThat(esIndexNameCurrent).isNotEmpty();
        assertThat(esIndexNameCurrent).isEqualTo(String.format("workloads_%s", monthlySuffix));
    }
}

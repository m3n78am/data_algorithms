package com.youzan.dataalgorithms.secondarysort.mapred;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class DateTemperatureGroupingComparator
        extends WritableComparator {

    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    @Override
    /**
     * @param wc1 a WritableComparable object, which represnts a DateTemperaturePair
     * @param wc2 a WritableComparable object, which represnts a DateTemperaturePair
     * @return 0, 1, or -1 (depending on the comparsion of two DateTemperaturePair objects).
     */
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        DateTemperaturePair pair = (DateTemperaturePair) wc1;
        DateTemperaturePair pair2 = (DateTemperaturePair) wc2;
        return pair.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
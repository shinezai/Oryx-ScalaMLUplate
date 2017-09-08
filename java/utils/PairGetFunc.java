package com.shinezai.oryx.utils;

import com.cloudera.oryx.common.collection.Pair;
import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by wuxuan on 2017/8/1 0001.
 */

public final class PairGetFunc{

    public static Collector<Pair<Path, Double>, ?, Map<Path,Double>> toMap(){
        return Collectors.toMap(Pair::getFirst, Pair::getSecond);
    }

}

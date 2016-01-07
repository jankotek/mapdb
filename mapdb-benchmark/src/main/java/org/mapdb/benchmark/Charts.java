//package org.mapdb.benchmark;
//
//import org.jfree.chart.ChartFactory;
//import org.jfree.chart.ChartUtilities;
//import org.jfree.chart.JFreeChart;
//import org.jfree.data.category.DefaultCategoryDataset;
//
//import java.io.File;
//import java.io.IOException;
//
///**
// * Creates chart from results
// */
//public class Charts {
//
//    public static void main(String[] args) throws IOException {
//        {
//            //generate graph for memory usage
//            DefaultCategoryDataset dataset = new DefaultCategoryDataset();
//
//            for (String name : InMemorySpaceUsage.fabs.keySet()) {
//                String val = BU.resultGet(InMemorySpaceUsage.class.getSimpleName() + "." + name + ".out");
//                dataset.addValue(Long.parseLong(val), name, "Memory");
//
//            }
//
//            // based on the dataset we create the chart
//            JFreeChart chart = ChartFactory.createBarChart(
//                    "",//"Entries inserted into " + InMemorySpaceUsage.memUsage + "GB of memory",
//                    "",
//                    "Final count",
//                    dataset
//            );
//            ChartUtilities.saveChartAsPNG(new File("res/" + InMemorySpaceUsage.class.getSimpleName() + ".png"), chart, 700, 500);
//        }
//
//        {
//            //generate graph for memory usage 2
//            DefaultCategoryDataset dataset = new DefaultCategoryDataset();
//
//            for (String name : InMemorySpaceUsage.fabs.keySet()) {
//                if("BTreeMap_offheap".equals(name))
//                    continue;
//                String val = BU.resultGet(InMemorySpaceUsage.class.getSimpleName() + "." + name + ".out");
//                dataset.addValue(Long.parseLong(val), name, "Memory");
//            }
//
//            // based on the dataset we create the chart
//            JFreeChart chart = ChartFactory.createBarChart(
//                    "",//"Entries inserted into " + InMemorySpaceUsage.memUsage + "GB of memory",
//                    "",
//                    "Final count",
//                    dataset
//            );
//            ChartUtilities.saveChartAsPNG(new File("res/" + InMemorySpaceUsage.class.getSimpleName() + "2.png"), chart, 700, 500);
//        }
//
//
//        {
//            //generate graph for memory usage
//            DefaultCategoryDataset dataset = new DefaultCategoryDataset();
//
//            for (String name : InMemoryCreate.fabs.keySet()) {
//                String val = BU.resultGet(InMemoryCreate.class.getSimpleName() + "." + name + ".out");
//                dataset.addValue(Long.parseLong(val), name, "Duration");
//
//            }
//
//            // based on the dataset we create the chart
//            JFreeChart chart = ChartFactory.createBarChart(
//                    "",//""+ InMemoryCreate.max/1000000+"M records inserted in N milliseconds",
//                    "",
//                    "Duration",
//                    dataset
//            );
//            ChartUtilities.saveChartAsPNG(new File("res/" + InMemoryCreate.class.getSimpleName() + ".png"), chart, 700, 500);
//        }
//
//        {
//            //generate graph for memory usage
//            DefaultCategoryDataset dataset = new DefaultCategoryDataset();
//
//            for (String name : InMemoryUpdate.fabs.keySet()) {
//                String val = BU.resultGet(InMemoryUpdate.class.getSimpleName() + "." + name + ".out");
//                dataset.addValue(Long.parseLong(val), name, "Duration");
//
//            }
//
//            // based on the dataset we create the chart
//            JFreeChart chart = ChartFactory.createBarChart(
//                    "",//"Time to randomly update "+ InMemoryUpdate.max/1000000+"M records",
//                    "",
//                    "Duration",
//                    dataset
//            );
//            ChartUtilities.saveChartAsPNG(new File("res/" + InMemoryUpdate.class.getSimpleName() + ".png"), chart, 700, 500);
//        }
//
//
//        {
//            //generate graph for memory usage
//            DefaultCategoryDataset dataset = new DefaultCategoryDataset();
//
//            for (String name : InMemoryGet.fabs.keySet()) {
//                String val = BU.resultGet(InMemoryGet.class.getSimpleName() + "." + name + ".out");
//                dataset.addValue(Long.parseLong(val), name, "Duration");
//
//            }
//
//            // based on the dataset we create the chart
//            JFreeChart chart = ChartFactory.createBarChart(
//                    "",//"Time to randomly get "+ InMemoryGet.max/1000000+"M records",
//                    "",
//                    "Duration",
//                    dataset
//            );
//            ChartUtilities.saveChartAsPNG(new File("res/" + InMemoryGet.class.getSimpleName() + ".png"), chart, 700, 500);
//        }
//
//        {
////            for(String cat:cats){
////                XYSeriesCollection dataset = new XYSeriesCollection();
////
////                for (String task : tasks) {
////                    XYSeries xyseries1 = new XYSeries(task);
////                    for (Integer thread : threads) {
////                        String val = (String) p.get(cat + "_" + task + "_"+thread);
////                        xyseries1.add(thread,new Long(val));
////                    }
////                    dataset.addSeries(xyseries1);
////
////                }
////
////
////
////                // based on the dataset we create the chart
////                JFreeChart chart = ChartFactory.createXYLineChart(
////                        cat + " - concurrent performance",
////                        "number of threads",
////                        "ops/second",
////                        dataset
////                );
////
////
////
////
////                ChartUtilities.saveChartAsPNG(new File(target + "-"+cat+"-scalability.png"), chart, 700, 500);
////
////            }
//
//        }
//    }
//}

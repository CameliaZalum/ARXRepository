import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.criteria.EntropyLDiversity;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.metric.Metric;
import org.deidentifier.arx.risk.RiskEstimateBuilder;
import org.deidentifier.arx.risk.RiskModelSampleWildcard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;


public class Main {
    protected static AttributeType.Hierarchy.DefaultHierarchy age = AttributeType.Hierarchy.create();
    protected static AttributeType.Hierarchy.DefaultHierarchy fnlwgt = AttributeType.Hierarchy.create();
    protected static AttributeType.Hierarchy.DefaultHierarchy educationalNum = AttributeType.Hierarchy.create();
    protected static AttributeType.Hierarchy.DefaultHierarchy capitalGain = AttributeType.Hierarchy.create();
    protected static AttributeType.Hierarchy.DefaultHierarchy capitalLoss = AttributeType.Hierarchy.create();
    protected static AttributeType.Hierarchy.DefaultHierarchy hoursPerWeek = AttributeType.Hierarchy.create();
    protected static HierarchyBuilderRedactionBased<Object> region;
    public static void main(String[] args) {
        try {
            DataSource source = DataSource.createCSVSource("./data/adult.csv", StandardCharsets.UTF_8, ',', true);
            source.addColumn("age", DataType.INTEGER, true);
            source.addColumn("workclass", DataType.STRING, true);
            source.addColumn("fnlwgt", DataType.INTEGER, true);
            source.addColumn("education", DataType.STRING, true);
            source.addColumn("educational-num", DataType.INTEGER, true);
            source.addColumn("marital-status", DataType.STRING, true);
            source.addColumn("occupation", DataType.STRING, true);
            source.addColumn("relationship", DataType.STRING, true);
            source.addColumn("race", DataType.STRING, true);
            source.addColumn("gender", DataType.STRING, true);
            source.addColumn("capital-gain", DataType.INTEGER, true);
            source.addColumn("capital-loss", DataType.INTEGER, true);
            source.addColumn("hours-per-week", DataType.INTEGER, true);
            source.addColumn("native-country", DataType.STRING, true);
            source.addColumn("income", DataType.STRING, true);

            Data data = Data.create(source);
            // create Hierarchies for requested columns
            createDefaultHierarchies();

            data.getDefinition().setAttributeType("age", Main.age);
            data.getDefinition().setAttributeType("workclass", AttributeType.INSENSITIVE_ATTRIBUTE);
            data.getDefinition().setAttributeType("fnlwgt", Main.fnlwgt);
            data.getDefinition().setAttributeType("education", AttributeType.SENSITIVE_ATTRIBUTE);
            data.getDefinition().setAttributeType("educational-num", Main.educationalNum);
            data.getDefinition().setAttributeType("marital-status", AttributeType.INSENSITIVE_ATTRIBUTE);
            data.getDefinition().setAttributeType("occupation", AttributeType.SENSITIVE_ATTRIBUTE);
            data.getDefinition().setAttributeType("relationship", AttributeType.SENSITIVE_ATTRIBUTE);
            data.getDefinition().setAttributeType("race", AttributeType.INSENSITIVE_ATTRIBUTE);
            data.getDefinition().setAttributeType("gender", AttributeType.INSENSITIVE_ATTRIBUTE);
            data.getDefinition().setAttributeType("capital-gain", Main.capitalGain);
            data.getDefinition().setAttributeType("capital-loss", Main.capitalLoss);
            data.getDefinition().setAttributeType("hours-per-week", Main.hoursPerWeek);
            data.getDefinition().setAttributeType("native-country", Main.region);
            data.getDefinition().setAttributeType("income", AttributeType.INSENSITIVE_ATTRIBUTE);

            ARXConfiguration config = ARXConfiguration.create();
            config.addPrivacyModel(new KAnonymity(5));
            config.addPrivacyModel(new EntropyLDiversity("education", 3));
            config.addPrivacyModel(new EntropyLDiversity("occupation", 3));
            config.addPrivacyModel(new EntropyLDiversity("relationship", 3));
            config.setSuppressionLimit(0.5d);
            config.setQualityModel(Metric.createLossMetric());

            ARXAnonymizer anonymizer = new ARXAnonymizer();
            ARXResult result = anonymizer.anonymize(data, config);
            DataHandle output = result.getOutput();
            result.getOutput().save("./result/results.csv", ',');

            // Perform risk analysis
            System.out.println("\n - Output data");
            print(output);
            System.out.println("\n - Risk analysis:");
            analyzeData(output);

        }catch (IOException exception){
            System.out.println(exception.getMessage());
        }
    }

    /**
     * Method taken from https://github.com/arx-deidentifier/arx/blob/master/src/example/org/deidentifier/arx/examples/Example.java
     * @param handle
     */
    protected static void print(DataHandle handle) {
        final Iterator<String[]> itHandle = handle.iterator();
        print(itHandle);
    }

    /**
     * Method taken from https://github.com/arx-deidentifier/arx/blob/master/src/example/org/deidentifier/arx/examples/Example.java
     * @param iterator
     */
    protected static void print(Iterator<String[]> iterator) {
        while (iterator.hasNext()) {
            System.out.print("   ");
            System.out.println(Arrays.toString(iterator.next()));
        }
    }
    private static void analyzeData(DataHandle handle) {

        double THRESHOLD = 0.1d;

        RiskEstimateBuilder builder = handle.getRiskEstimator();
        RiskModelSampleWildcard risks = builder.getSampleBasedRiskSummaryWildcard(THRESHOLD);

        System.out.println(" * Wildcard risk model");
        System.out.println("   - Records at risk: " + getPercent(risks.getRecordsAtRisk()));
        System.out.println("   - Highest risk: " + getPercent(risks.getHighestRisk()));
        System.out.println("   - Average risk: " + getPercent(risks.getAverageRisk()));
    }

    /**
     * Converts a double value of type 0.XXy to a percentage string value XX.y%
     * @param value double number of form 0.XXy where X is a digit (the two Xs may be different) and y is an array of digits
     * @return formatted string
     */
    private static String getPercent(double value) {
        return (value * 100) + "%";
    }

    /**
     * Formats the Hierarchies for the anonymization process
     */
    private static void createDefaultHierarchies() {
        for (int i = 10; i <= 120; ++i){
            String stringValueOfAge = String.valueOf(i);
            String firstDigitAndStarStringValue;

            if (i > 99){
                firstDigitAndStarStringValue = "1**";
            } else {
                firstDigitAndStarStringValue = i / 10 + "*";
            }
            age.add(stringValueOfAge, firstDigitAndStarStringValue);
        }
        for (int i = 0; i <= 9999999; ++i) {
            String stringValueOfFnlgwt = String.valueOf(i);
            String flngwtAnonymizationStringValue;
            flngwtAnonymizationStringValue =  "*".repeat(stringValueOfFnlgwt.length());
            fnlwgt.add(stringValueOfFnlgwt, flngwtAnonymizationStringValue);
        }
        for (int i = 0; i <= 20; ++i)
        {
            String educationalNumStringValue = String.valueOf(i);
            String rangeAnonymizationStringValue;
            if(i <= 7){
                rangeAnonymizationStringValue = "<=7";
            } else {
                rangeAnonymizationStringValue = ">7";
            }
            educationalNum.add(educationalNumStringValue, rangeAnonymizationStringValue);
        }
        for (int i =0; i <= 99999; ++i){
            String capitalStringValue = String.valueOf(i);
            String rangeStringValue;
            if (i <= 2000){
                rangeStringValue = "low";

            }else if (i > 2000 && i <= 5000){
                rangeStringValue = "moderate";

            } else {
                rangeStringValue = "high";
            }
            capitalGain.add(capitalStringValue, rangeStringValue);
            capitalLoss.add(capitalStringValue, rangeStringValue);
        }

        for (int i = 0; i <= 99; ++i){
            String hoursStringValue = String.valueOf(i);
            String rangeStringValue;
            if (i <= 40){
                rangeStringValue = "<=40";

            }else {
                rangeStringValue = ">40";

            }
            hoursPerWeek.add(hoursStringValue, rangeStringValue);

        }

        region = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ',
                '*');
        region.setAlphabetSize(10, 5);

    }
}

/*
 * CSCI-654 | Assignment Four | DivIndex
 * Tushar Iyer | txi9546
 */

import edu.rit.pjmr.Combiner;
import edu.rit.pjmr.Customizer;
import edu.rit.pjmr.Mapper;
import edu.rit.pjmr.PjmrJob;
import edu.rit.pjmr.Reducer;
import edu.rit.pjmr.TextFileSource;
import edu.rit.pjmr.TextId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

/**
 * Class DivIndex provides a class that will act as the main portion of a
 * program designed to use PJMR in order to calculate the U.S. Census Diversity
 * Index for all counties in a given state or for all counties in all states as
 * well as the Diversity Index for the states themselves
 *
 * @author tushariyer
 */
public class DivIndex extends PjmrJob<TextId, String, StateCountyKey, DivIndexVbl> {

    /**
     * Method main is the PJMR job main program
     *
     * @param args command line arguments
     * @throws Exception
     */
    @Override
    public void main(String[] args) throws Exception {
        if (verify(args)) {
            // Store relevant variables
            String[] nodes = args[0].split(","); // Store nodes
            String filename = args[1]; // Store filename
            int numThreads = Math.max(threads(), 1);  // Get number of mapper threads

            for (String node : nodes) {
                mapperTask(node)
                        .source(new TextFileSource(filename))
                        .mapper(numThreads, MyMapper.class, args);
            }
            reducerTask()
                    .customizer(MyCustomizer.class)
                    .reducer(MyReducer.class);

            startJob();
        } else {
            execSpecs();
        }
    }

    /**
     * HashSet getStates provides a method to return a set of all states as
     * Strings.
     *
     * @return set of all states
     */
    private static HashSet<String> getStates() {
        return new HashSet<>(Arrays.asList(new String[]{"Alabama", "Alaska",
            "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
            "Delaware", "District of Columbia", "Florida", "Georgia", "Hawaii",
            "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
            "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
            "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
            "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
            "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
            "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
            "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
            "West Virginia", "Wisconsin", "Wyoming"}));
    }

    /**
     * Method execSpecs prints out the instructions on how to run this program
     */
    private static void execSpecs() {
        System.err.println("Run this program by typing: java pj2 jar=<jar>"
                + " threads=<NT> DivIndex <nodes> <file> <year> [ \"<state>\" ...]"
                + "\n - <jar> is the name of the JAR file."
                + "\n - <NT> is the number of mapper threads (Defaults to 1 if omitted)."
                + "\n - <nodes> is a csv list of cluster node names."
                + "\n - <file> is the name of the census data file."
                + "\n - <year> is the year to be analyzed. Must be a positive int between 1 and 10."
                + "\n - <state> is/are the state(s) to be analyzed (Each as its own string). Do not repeat state names.");
        terminate(1); // End the program
    }

    /**
     * Method verify is called in the main method to verify that all the inputs
     * are correct
     */
    private static boolean verify(String[] args) {
        // Check for minimum required parameters
        if (args.length < 3) {
            System.err.println("Minimum required parameters missing.");
            execSpecs();
        }
        // Check <nodes> argument
        String[] nodes = args[0].split(",");
        if (nodes.length == 0) {
            System.err.println("A minimum of one node must be specified");
            execSpecs();
        }

        // Check <file> argument
        try {
            String filename = args[1]; // Store filename
        } catch (Exception e) {
            System.err.println("<file> not parsing. Please check that there aren't any spaces in the filepath");
            execSpecs();
        }

        // Check <year> argument
        try {
            int year = Integer.parseInt(args[2]);

            if (year < 1 || year > 10) {
                System.err.println("<year> must be a positive integer in the bounds: 1 < <year> < 10 ");
                execSpecs();
            }
        } catch (NumberFormatException e) {
            System.err.println("<year> must be a positive integer in the bounds: 1 < <year> < 10 ");
            execSpecs();
        }

        // Check any <state> arguments
        ArrayList<String> states = new ArrayList<>();
        int numStates = args.length - 3; // Store number of state arguments
        // If we have state arguments present
        if (numStates > 0) {

            // Limited by fifty states
            if (numStates > 51) {
                System.err.println("Cannot have more than fifty-one state arguments.");
                execSpecs();
            }

            // Load them all into a string array
            for (int i = 3; i <= args.length - 1; i++) {
                states.add(args[i]);
            }

            // Check if state argument is not a string
            try {
                int stateInt = Integer.parseInt(states.get(0));
                System.err.println("States cannot be numbers,\nthey must be proper "
                        + "state names in quotes, each delimited by a space");
                execSpecs();

            } catch (NumberFormatException e) {
                // Check if any state arguments have been repeated
                for (int i = 0; i < numStates; i++) {
                    for (int j = i + 1; j < numStates; j++) {
                        try {
                            if (states.get(i).equals(states.get(j))) {
                                System.err.println("Repeating state found! Do not repeat <state> args");
                                execSpecs();
                            }
                        } catch (NullPointerException npe) {
                            System.err.println("NullPointerException when checking for duplicate states!");
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Class MyMapper provides a class for the map-phase of the map-reduce
     * procedure. This class will take in a record from the dataset and check
     * that it meets the prerequisites needed to create a key,value pair that is
     * then added to the combiner
     */
    private static class MyMapper extends Mapper<TextId, String, StateCountyKey, DivIndexVbl> {

        // Instance variables
        private static Integer year;
        private static final int[] positions = new int[]{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};
        private HashSet stateList;

        /**
         * Method start is used to bring in the parameters passed in as command
         * line arguments
         *
         * @param args arguments containing the year and states (if any)
         * @param cmbnr Combiner
         */
        @Override
        public void start(String[] args, Combiner<StateCountyKey, DivIndexVbl> combiner) {
            stateList = new HashSet<>(); // Set to hold states
            year = Integer.parseInt(args[2]); // Store the year passed from the command line

            for (int i = 3; i < args.length; i++) {
                stateList.add(args[i]); // Store all states passed from the command line
            }
            if (stateList.isEmpty()) {
                stateList = getStates(); // Get the entire set if no states passed in
            }
        }

        /**
         * Method map is the main part of the map-phase. Here we extract the
         * relevant parts of the record and check to see that the prerequisites
         * all fit. Then we create a StateCountyKey key for the state and for
         * the county, and a DivIndexVbl value with the population statistics
         *
         * @param i TextId of the record
         * @param c String containing the record from the dataset
         * @param cmbnr Combiner
         */
        @Override
        public void map(TextId i, String c, Combiner<StateCountyKey, DivIndexVbl> cmbnr) {
            String[] record = c.split(","); // Split the record
            String stateName = record[3]; // Store the state name
            String countyName = record[4]; // Store the county name
            Integer yearMatch = Integer.parseInt(record[5]); // Store the year

            /**
             * Check if the state name is part of the state arguments given. If
             * no arguments given, run for all states. Check that the years
             * match and that the age group is 0
             */
            if (stateList.contains(stateName) && Objects.equals(year, yearMatch) && Integer.parseInt(record[6]) == 0) {

                /**
                 * Create a StateCountyKey key for both state and county. For
                 * the state, make the county name "NoCounty" and the county
                 * code 999 This county name will be used in the reducer to
                 * determine which of the print statements to use
                 */
                StateCountyKey stateKey = new StateCountyKey(stateName, "NoCounty");
                StateCountyKey countyKey = new StateCountyKey(stateName, countyName);

                // Arrays for the population information
                int[] tempStats = new int[positions.length];
                int[] stats = new int[(positions.length / 2) + 1];

                for (int k = 0; k < 12; k++) {
                    tempStats[k] = (Integer.parseInt(record[positions[k]]));
                }

                // Load demographic populations
                stats[0] = (tempStats[0] + tempStats[1]);
                stats[1] = (tempStats[2] + tempStats[3]);
                stats[2] = (tempStats[4] + tempStats[5]);
                stats[3] = (tempStats[6] + tempStats[7]);
                stats[4] = (tempStats[8] + tempStats[9]);
                stats[5] = (tempStats[10] + tempStats[11]);

                // Store the total population
                stats[6] = (stats[0] + stats[1] + stats[2] + stats[3] + stats[4] + stats[5]);

                // Create a new DivIndexVbl value with the location and population info
                DivIndexVbl vbl = new DivIndexVbl(stats);

                // Add both key,value pairs to the combiner
                cmbnr.add(stateKey, vbl);
                cmbnr.add(countyKey, vbl);
            }
        }
    }

    /**
     * Class MyCustomizer provides a class that is called before the reducer in
     * order to compare two key,value pairs and determine the order in which
     * they should be printed.
     */
    private static class MyCustomizer extends Customizer<StateCountyKey, DivIndexVbl> {

        /**
         * Method comesBefore takes in two key,value pairs and checks first if
         * the state codes are in correct order (states should print in
         * alphabetical order). It then checks (assuming the states are the
         * same) that the counties are printed in descending order of diversity
         * index
         *
         * @param keyOne
         * @param valueOne
         * @param keyTwo
         * @param valueTwo
         * @return
         */
        @Override
        public boolean comesBefore(StateCountyKey keyOne, DivIndexVbl valueOne, StateCountyKey keyTwo, DivIndexVbl valueTwo) {
            if (keyOne.getState().compareTo(keyTwo.getState()) < 0) {
                return true;
            } else if (keyOne.getState().compareTo(keyTwo.getState()) > 0) {
                return false;
            } else if (keyOne.getCounty().equals("NoCounty")) {
                return true;
            } else if (keyTwo.getCounty().equals("NoCounty")) {
                return false;
            } else {
                return valueOne.calcDiv() > valueTwo.calcDiv();
            }
        }
    }

    /**
     * Class MyReducer provides a class with a reduce method that handles the
     * second half of the map-reduce procedure and prints out the states and
     * counties with their respective diversity index values
     */
    private static class MyReducer extends Reducer<StateCountyKey, DivIndexVbl> {

        /**
         * Method reduce takes in a key,value pair and prints out one of two
         * statements depending on whether the key corresponds to a state or a
         * county, and gets the respective diversity index from the DivIndexVbl
         * object
         *
         * @param key StateCountyKey with the state and county (if county) names
         * @param value DivIndexVbl which will then calculate the respective
         * diversity index
         */
        @Override
        public void reduce(StateCountyKey key, DivIndexVbl vbl) {
            String[] stateCounty = {key.getState(), key.getCounty()};
            double diversityIndex = vbl.calcDiv();

            if (stateCounty[1].equalsIgnoreCase("NoCounty")) {
                String stateName = stateCounty[0];
                System.out.printf("%s\t\t%.5g%n", stateName, diversityIndex);
            } else {
                String countyName = stateCounty[1];
                System.out.printf("\t%s\t%.5g%n", countyName, diversityIndex);
            }
        }
    }
}

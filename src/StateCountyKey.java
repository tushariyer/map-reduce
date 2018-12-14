/*
 * CSCI-654 | Assignment Four | StateCountyKey
 * Tushar Iyer | txi9546
 */

import edu.rit.io.InStream;
import edu.rit.io.OutStream;
import edu.rit.io.Streamable;
import java.io.IOException;

/**
 * Class StateCountyKey is a class that will be used as the Key in the Key,Value
 * pair passed to the map-reduce combiner in the Mapper class for the DivIndex
 * program.
 *
 * @author tushariyer
 */
public class StateCountyKey implements Streamable {

    // Instance variables
    private String stateName; // State name
    private String countyName; // County name. If the object is being made for the state itself, this value will be "NoCounty"

    /**
     * Class empty constructor sets the inner strings to empty
     */
    public StateCountyKey() {
        this.stateName = "";
        this.countyName = "";
    }

    /**
     * Class constructor takes in values to set the object's inner strings
     *
     * @param stateN The state name
     * @param countyN The county name
     */
    public StateCountyKey(String stateN, String countyN) {
        // Set values
        this.stateName = stateN;
        this.countyName = countyN;
    }

    /**
     * Method getState returns the state name
     *
     * @return state name
     */
    public String getState() {
        return this.stateName;
    }

    /**
     * Method getCounty returns the county name
     *
     * @return county name or "NoCounty" if states' own object
     */
    public String getCounty() {
        return this.countyName;
    }

    /**
     * Method equals checks to see if this StateCountyKey object and another
     * object cast as a StateCountyKey object have the same inner values.
     *
     * @param o an object
     * @return boolean true or false depending on whether the objects are equal
     */
    @Override
    public boolean equals(Object o) {
        return (this.stateName.equals(((StateCountyKey) o).getState()))
                && (this.countyName.equals(((StateCountyKey) o).getCounty()));
    }

    /**
     * Method hashCode is overridden and defines a custom hash code for a given
     * StateCountyKey object. For this code, the state and county name are
     * concatenated into a single string and then an integer is calculated by
     * the product of all individual characters
     *
     * @return integer hash code
     */
    @Override
    public int hashCode() {
        int retVal = 0;
        String total = getState() + getCounty();
        for (int i = 0; i < total.length(); i++) {
            retVal *= total.charAt(i);
        }
        return retVal;
    }

    /**
     * Method toString returns the string representation of the StateCountyKey's
     * inner variables
     *
     * @return String that looks like: "State, County."
     */
    @Override
    public String toString() {
        return (this.stateName + ", " + this.countyName + ".");
    }

    /**
     * Uses an OutStream to write out the objects contents
     *
     * @param stream an output stream
     * @throws IOException Exception if error
     */
    @Override
    public void writeOut(OutStream stream) throws IOException {
        stream.writeString(stateName);
        stream.writeString(countyName);
    }

    /**
     * Uses an InStream to read in the objects new contents
     *
     * @param stream an input stream
     * @throws IOException Exception if error
     */
    @Override
    public void readIn(InStream stream) throws IOException {
        this.stateName = stream.readString();
        this.countyName = stream.readString();
    }
}

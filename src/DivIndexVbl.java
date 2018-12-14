/*
 * CSCI-654 | Assignment Four | DivIndexVbl
 * Tushar Iyer | txi9546
 */

import edu.rit.io.InStream;
import edu.rit.io.OutStream;
import edu.rit.pj2.Tuple;
import edu.rit.pj2.Vbl;
import java.io.IOException;

/**
 * Class DivIndexVbl is a variable reduction class. It will just be used to
 * replace the DivIndex data objects' inner variables.
 *
 * @author tushariyer
 */
public class DivIndexVbl extends Tuple implements Vbl {

    // Instance variables
    private int[] stats;

    /**
     * Default constructor for Vbl object
     */
    public DivIndexVbl() {
        stats = new int[7];
    }

    /**
     * Parameter-constructor for Vbl object
     *
     * @param newVals the county population values
     */
    public DivIndexVbl(int[] newVals) { 
        this.stats = newVals;
    }

    /**
     * Uses an OutStream to write out the objects contents
     *
     * @param stream an output stream
     * @throws IOException Exception if error
     */
    @Override
    public void writeOut(OutStream stream) throws IOException {
        stream.writeIntArray(this.stats);
    }

    /**
     * Uses an InStream to read in the objects new contents
     *
     * @param stream an input stream
     * @throws IOException Exception if error
     */
    @Override
    public void readIn(InStream stream) throws IOException {
        setStats(stream.readIntArray());
    }

    /**
     * Method set takes in a vbl and casts it as a DivIndexVbl object. It then
     * sets the passed DivIndexVbl object to be this DivIndexVbl object
     *
     * @param vbl a Vbl object
     */
    @Override
    public void set(Vbl vbl) {
        DivIndexVbl temp = (DivIndexVbl) vbl;
        setStats(temp.getStats());
    }

    /**
     * Method reduce takes in a vbl and casts it as a DivIndexVbl object. It
     * then sets the passed DivIndexVbl object's population statistics to be the
     * population statistics for this DivIndexVbl object
     *
     * @param vbl Vbl
     */
    @Override
    public void reduce(Vbl vbl) {
        DivIndexVbl temp = (DivIndexVbl) vbl;
        int[] tempStats = temp.getStats();
        for (int i = 0; i < tempStats.length; i++) {
            this.stats[i] += tempStats[i];
        }
    }

    /**
     * Method getStats returns the population statistics of this county, and is
     * called by the set and reduce methods in this class
     *
     * @return The population statistics
     */
    public int[] getStats() {
        return this.stats;
    }

    /**
     * Method setStats sets the passed integer array to the inner stats
     * variable.
     *
     * @param newStats new stats integer array
     */
    private void setStats(int[] newStats) {
        this.stats = newStats;
    }

    /**
     * Method calcDiv calculates the countyDiversity index for the given record.
     *
     * @return countyDiversity index
     */
    public double calcDiv() {
        double x = 0;
        double temp = this.stats[6];
        double sqr = temp * temp;
        for (int j = 0; j < 6; j++) {
            x += (this.stats[j] * (temp - this.stats[j]));
        }
        return (1 / sqr) * x;
    }
}

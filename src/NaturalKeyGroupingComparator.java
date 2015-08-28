import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(Text.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	// Makes sure everything goes to the same reducer. 
        return 0;
    }
}
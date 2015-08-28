import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(Text.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text k1 = (Text)w1;
        Text k2 = (Text)w2;
        
        // Get the percentages of elderly people to compare
        Double percent1 = Double.parseDouble(k1.toString().split(":")[1]);
        Double percent2 = Double.parseDouble(k2.toString().split(":")[1]);
        
        // Multiply by -1 because it was sorting the opposite way. 
        int result = percent1.compareTo(percent2)*-1;
        
        // Return the result
        return result;
    }
}
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.broadinstitute.sting.gatk.walkers.bqsr.BQSRGatherer;

public class BQSRGathererMain {
    /* * args: outputFile inputFiles */
    public static void main(String[] args) {
        File output = new File(args[0]);
        List<File> inputs = new ArrayList<File>();

        for(int i=1;i<args.length;i++) {
            inputs.add(new File(args[i]));
        }

        QSRGatherer bQSRGatherer = new BQSRGatherer();
        bQSRGatherer.gather(inputs, output);
    }
}
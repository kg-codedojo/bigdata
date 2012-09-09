package codedojo.bigdata.hadoop.meters;

import java.io.*;

/**
 * Created By: KonstantinG
 * Date,time: 9/9/12, 8:15 PM
 */
public class InputDataProcessor {
    private String _outputDirPath;
    public InputDataProcessor(String outputDirPath) {
        _outputDirPath=outputDirPath;
        File outputDir = new File(outputDirPath);
        if(!outputDir.exists()){
            outputDir.mkdirs();
        }
    }

    public void preprocess(String path,String fileName, String meterNodeID) throws IOException {
        FileInputStream in = new FileInputStream(path);
        FileOutputStream out = new FileOutputStream(_outputDirPath+File.separator+fileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(new DataInputStream(in)));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new DataOutputStream(out)));
        String strLine;
        while((strLine = br.readLine()) != null){
            if(!"".equals(strLine.trim())){
                bw.write(meterNodeID+";"+strLine+"\n");
                bw.flush();
            }
        }
        in.close();
        out.close();
    }
}
